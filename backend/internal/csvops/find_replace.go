package csvops

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/utils"
)

// ReplaceRule defines a mapping from multiple target variants to one replacement.
type ReplaceRule struct {
	Targets         []string `json:"targets"`                    // e.g. ["USA", "U.S.", "United States of America"]
	Replacement     string   `json:"replacement"`                // e.g. "USA"
	CaseInsensitive *bool    `json:"case_insensitive,omitempty"` // nil => use global option default
	WholeCell       *bool    `json:"whole_cell,omitempty"`       // nil => default false (substring replace)
}

// FindReplaceOptions configures the operation behavior.
type FindReplaceOptions struct {
	TrimSpaces      bool     `json:"trim_spaces"`       // trim cell before matching (and when doing whole-cell compare)
	CaseInsensitive bool     `json:"case_insensitive"`  // default for rules where rule.CaseInsensitive==nil
	Columns         []string `json:"columns,omitempty"` // columns to apply; empty => all columns
}

// Request / response
type FindReplaceRequest struct {
	Operation string             `json:"operation"`
	Options   FindReplaceOptions `json:"options"`
	Dataset   types.TableData    `json:"dataset"`
	Rules     []ReplaceRule      `json:"rules"`
}

type FindReplaceRuleResult struct {
	Index        int      `json:"index"` // rule index
	Targets      []string `json:"targets"`
	Replacement  string   `json:"replacement"`
	Replacements int      `json:"replacements"` // how many replacements applied (occurrences or cells changed)
}

type FindReplaceResponse struct {
	Operation string                  `json:"operation"`
	Summary   types.ResultSummary     `json:"summary"`
	Result    types.TableData         `json:"result"`
	PerRule   []FindReplaceRuleResult `json:"per_rule"`
	Error     *string                 `json:"error"`
}

// buildRegexForRule builds a regexp for the rule.
// If wholeCell==true it anchors ^(?:a|b|c)$
// If wholeCell==false it builds (?:a|b|c) (to match substrings)
// caseInsensitive toggles the (?i) flag via prefix.
func buildRegexForRule(targets []string, wholeCell bool, caseInsensitive bool) (*regexp.Regexp, error) {
	if len(targets) == 0 {
		return nil, errors.New("empty targets")
	}
	parts := make([]string, 0, len(targets))
	for _, t := range targets {
		parts = append(parts, regexp.QuoteMeta(t))
	}
	pat := "(?:" + strings.Join(parts, "|") + ")"
	if wholeCell {
		pat = "^" + pat + "$"
	}
	if caseInsensitive {
		pat = "(?i)" + pat
	}
	re, err := regexp.Compile(pat)
	if err != nil {
		return nil, fmt.Errorf("compile regex: %w", err)
	}
	return re, nil
}

// resolveColumnsToIndices is reused from data_clean.go (it's in same package).
// If Columns empty => all columns (based on header length or first row length).
// It leverages utils.ParseIndexString for numeric indices.
func resolveColumnsToIndicesForReplace(tbl types.TableData, cols []string) ([]int, error) {
	// reuse logic from data_clean.resolveColumnsToIndices but simpler here
	// if empty, return all indices
	if len(cols) == 0 {
		indices := make([]int, 0)
		if len(tbl.Header) > 0 {
			for i := 0; i < len(tbl.Header); i++ {
				indices = append(indices, i)
			}
			return indices, nil
		}
		if len(tbl.Rows) > 0 {
			for i := 0; i < len(tbl.Rows[0]); i++ {
				indices = append(indices, i)
			}
			return indices, nil
		}
		return []int{}, nil
	}

	// build header map (trim & lower)
	headerMap := map[string]int{}
	for i, h := range tbl.Header {
		key := strings.TrimSpace(h)
		key = strings.ToLower(key)
		headerMap[key] = i
	}

	indices := make([]int, 0, len(cols))
	for _, c := range cols {
		cTrim := strings.TrimSpace(c)
		if idx, ok := utils.ParseIndexString(cTrim); ok {
			indices = append(indices, idx)
			continue
		}
		key := strings.ToLower(cTrim)
		if pos, ok := headerMap[key]; ok {
			indices = append(indices, pos)
			continue
		}
		return nil, fmt.Errorf("column '%s' not found in header", c)
	}
	return indices, nil
}

// FindAndReplace performs the smart find/replace on a single dataset (no multi-list support).
func FindAndReplace(req FindReplaceRequest) (FindReplaceResponse, error) {
	var res FindReplaceResponse
	res.Operation = req.Operation
	start := time.Now()

	// validation
	if req.Dataset.Rows == nil {
		msg := "dataset required"
		res.Error = &msg
		return res, errors.New(msg)
	}
	if len(req.Rules) == 0 {
		msg := "no rules provided"
		res.Error = &msg
		return res, errors.New(msg)
	}

	// resolve columns
	indices, err := resolveColumnsToIndicesForReplace(req.Dataset, req.Options.Columns)
	if err != nil {
		msg := err.Error()
		res.Error = &msg
		return res, err
	}

	// compile regexes for each rule
	type compiledRule struct {
		rule       ReplaceRule
		re         *regexp.Regexp
		caseInRule bool
		wholeCell  bool
	}

	compiled := make([]compiledRule, 0, len(req.Rules))
	for _, r := range req.Rules {
		ci := req.Options.CaseInsensitive
		if r.CaseInsensitive != nil {
			ci = *r.CaseInsensitive
		}
		wc := false
		if r.WholeCell != nil {
			wc = *r.WholeCell
		}
		re, err := buildRegexForRule(r.Targets, wc, ci)
		if err != nil {
			msg := fmt.Sprintf("rule compile error: %v", err)
			res.Error = &msg
			return res, err
		}
		compiled = append(compiled, compiledRule{
			rule:       r,
			re:         re,
			caseInRule: ci,
			wholeCell:  wc,
		})
	}

	// Prepare output table copy
	outRows := make([][]string, 0, len(req.Dataset.Rows))
	for _, r := range req.Dataset.Rows {
		outRows = append(outRows, append([]string(nil), r...))
	}

	// per-rule counters
	perRuleCounts := make([]int, len(compiled))

	// Apply rules: iterate rows, columns, rules (rules applied in order)
	for ri, row := range outRows {
		for _, colIdx := range indices {
			// ensure column exists; if not, pad row
			if colIdx >= len(row) {
				needed := colIdx - len(row) + 1
				for i := 0; i < needed; i++ {
					row = append(row, "")
				}
				outRows[ri] = row
			}
			cell := row[colIdx]
			origCell := cell

			// pre-trim if requested (affects matching)
			if req.Options.TrimSpaces {
				cell = strings.TrimSpace(cell)
			}

			// apply rules sequentially
			modifiedCell := cell
			for i, cr := range compiled {
				// if wholeCell: match entire cell; if matched => replace whole cell
				if cr.wholeCell {
					if cr.re.MatchString(modifiedCell) {
						// replace whole cell with replacement
						modifiedCell = cr.rule.Replacement
						perRuleCounts[i] += 1 // count cell change once
						// note: don't break; subsequent rules may also operate on the new value
					}
					continue
				}

				// substring replacement: use ReplaceAllStringFunc to count occurrences
				if cr.re.MatchString(modifiedCell) {
					count := 0
					newVal := cr.re.ReplaceAllStringFunc(modifiedCell, func(_ string) string {
						count++
						return cr.rule.Replacement
					})
					if count > 0 {
						perRuleCounts[i] += count
						modifiedCell = newVal
					}
				}
			}

			// If trimmed earlier but original input had different whitespace and we don't want to lose it
			// we preserve trimmed value (user asked trimming only for matching). We'll set cell to modifiedCell.
			// If the caller didn't want trimming, they'd set TrimSpaces=false.
			if modifiedCell != origCell {
				outRows[ri][colIdx] = modifiedCell
			}
		}
	}

	// build per-rule results
	perRuleRes := make([]FindReplaceRuleResult, len(compiled))
	totalReplacements := 0
	for i, cr := range compiled {
		perRuleRes[i] = FindReplaceRuleResult{
			Index:        i,
			Targets:      cr.rule.Targets,
			Replacement:  cr.rule.Replacement,
			Replacements: perRuleCounts[i],
		}
		totalReplacements += perRuleCounts[i]
	}

	// assemble response
	res.Result = types.TableData{
		HasHeader: req.Dataset.HasHeader,
		Header:    append([]string(nil), req.Dataset.Header...),
		Rows:      outRows,
	}
	res.PerRule = perRuleRes
	res.Summary = types.ResultSummary{
		Processed:  len(req.Dataset.Rows),
		Matched:    totalReplacements, // number of replacements occurrences
		Missing:    0,
		DurationMS: time.Since(start).Milliseconds(),
	}
	res.Error = nil
	return res, nil
}
