package csvops

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/utils"
)

// CaseMode controls case standardization
type CaseMode string

const (
	CaseNone  CaseMode = "none"
	CaseUpper CaseMode = "upper"
	CaseLower CaseMode = "lower"
	CaseTitle CaseMode = "title"
)

// Options and request/response types
type DataCleanOptions struct {
	TrimSpaces      bool     `json:"trim_spaces"`       // trim leading/trailing whitespace
	CollapseInnerWS bool     `json:"collapse_inner_ws"` // collapse multiple internal whitespace to single space
	CaseMode        CaseMode `json:"case_mode"`         // none|upper|lower|title
	Columns         []string `json:"columns,omitempty"` // columns to apply; empty == all columns
	CaseInsensitive bool     `json:"case_insensitive"`  // used when resolving header names (not for converting)
}

type PerCleanResult struct {
	Name      string          `json:"name"`
	Processed int             `json:"processed"` // rows processed
	Modified  int             `json:"modified"`  // number of cells changed
	Result    types.TableData `json:"result"`
	Error     *string         `json:"error"`
}

type DataCleanRequest struct {
	Operation string              `json:"operation"`
	Options   DataCleanOptions    `json:"options"`
	Datasets  types.MultiDatasets `json:"datasets"`
}

type DataCleanResponse struct {
	Operation string           `json:"operation"`
	Summary   map[string]int   `json:"summary"`
	PerList   []PerCleanResult `json:"per_list"`
	Error     *string          `json:"error"`
}

// helper: collapse internal whitespace (convert runs of whitespace to single space)
func collapseInnerWhitespace(s string) string {
	var b strings.Builder
	lastWasSpace := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			if !lastWasSpace {
				b.WriteRune(' ')
				lastWasSpace = true
			}
		} else {
			b.WriteRune(r)
			lastWasSpace = false
		}
	}
	return b.String()
}

// helper: title case a string (simple wordwise Title Case)
func toTitleCase(s string) string {
	// split on spaces, keep it simple
	words := strings.Fields(s)
	for i, w := range words {
		if w == "" {
			continue
		}
		runes := []rune(w)
		first := unicode.ToUpper(runes[0])
		if len(runes) == 1 {
			words[i] = string(first)
		} else {
			words[i] = string(first) + strings.ToLower(string(runes[1:]))
		}
	}
	return strings.Join(words, " ")
}

// resolveColumnsToIndices returns the indices for the requested column identifiers.
// If opts.Columns is empty, return all indices for the table.
func resolveColumnsToIndices(tbl types.TableData, cols []string, caseInsensitive bool) ([]int, error) {
	// if empty, return all indices
	if len(cols) == 0 {
		indices := make([]int, 0, len(tbl.Header))
		for i := range tbl.Header {
			indices = append(indices, i)
		}
		// if headerless, and no header present, apply to all positions inferred by whatever rows length is (best-effort)
		if len(indices) == 0 && len(tbl.Rows) > 0 {
			// pick length of first row
			for i := 0; i < len(tbl.Rows[0]); i++ {
				indices = append(indices, i)
			}
		}
		return indices, nil
	}

	// build header map (lowercased trimmed)
	headerMap := make(map[string]int)
	for i, h := range tbl.Header {
		key := strings.TrimSpace(h)
		if caseInsensitive {
			key = strings.ToLower(key)
		}
		headerMap[key] = i
	}

	indices := make([]int, 0, len(cols))
	for _, c := range cols {
		cTrim := strings.TrimSpace(c)
		// if numeric index passed
		if idx, ok := utils.ParseIndexString(cTrim); ok {
			indices = append(indices, idx)
			continue
		}
		key := cTrim
		if caseInsensitive {
			key = strings.ToLower(key)
		}
		if pos, ok := headerMap[key]; ok {
			indices = append(indices, pos)
			continue
		}
		return nil, fmt.Errorf("column '%s' not found in header", c)
	}
	return indices, nil
}

// applyTransforms applies trimming/case transforms to a single cell according to options.
// returns (newVal, changed)
func applyTransforms(cell string, opts DataCleanOptions) (string, bool) {
	orig := cell
	if opts.TrimSpaces {
		cell = strings.TrimSpace(cell)
	}
	if opts.CollapseInnerWS {
		cell = collapseInnerWhitespace(cell)
	}
	switch opts.CaseMode {
	case CaseUpper:
		cell = strings.ToUpper(cell)
	case CaseLower:
		cell = strings.ToLower(cell)
	case CaseTitle:
		cell = toTitleCase(cell)
	}
	return cell, cell != orig
}

// processSingleTable runs cleaning ops on a single table and returns modified table + counts.
func processSingleTable(tbl types.TableData, opts DataCleanOptions) (types.TableData, int, int, error) {
	indices, err := resolveColumnsToIndices(tbl, opts.Columns, opts.CaseInsensitive)
	if err != nil {
		return types.TableData{}, 0, 0, err
	}
	processedRows := len(tbl.Rows)
	modifiedCells := 0

	// deep copy rows to avoid mutating input
	outRows := make([][]string, 0, len(tbl.Rows))
	for _, r := range tbl.Rows {
		rowCopy := append([]string(nil), r...)
		for _, colIdx := range indices {
			// ensure column exists for this row (if shorter, consider as empty cell; extend?)
			if colIdx >= len(rowCopy) {
				// if row shorter than header, pad with empty strings up to colIdx
				needed := colIdx - len(rowCopy) + 1
				for i := 0; i < needed; i++ {
					rowCopy = append(rowCopy, "")
				}
			}
			newVal, changed := applyTransforms(rowCopy[colIdx], opts)
			if changed {
				modifiedCells++
				rowCopy[colIdx] = newVal
			}
		}
		outRows = append(outRows, rowCopy)
	}

	out := types.TableData{
		HasHeader: tbl.HasHeader,
		Header:    append([]string(nil), tbl.Header...),
		Rows:      outRows,
	}
	return out, processedRows, modifiedCells, nil
}

// DataClean executes cleaning operations across master and/or lists.
// It returns per-list results and a summary.
func DataClean(req DataCleanRequest) (DataCleanResponse, error) {
	var res DataCleanResponse
	res.Operation = req.Operation
	start := time.Now()

	// Validate options
	if req.Options.CaseMode == "" {
		req.Options.CaseMode = CaseNone
	}

	// gather tables (if lists provided use them, otherwise master)
	tables := []types.NamedTable{}
	if len(req.Datasets.Lists) > 0 {
		tables = append(tables, req.Datasets.Lists...)
	} else {
		// wrap master
		tables = append(tables, types.NamedTable{
			Name:    "dataset",
			ListKey: "",
			Table:   req.Datasets.Master,
		})
	}

	perList := make([]PerCleanResult, 0, len(tables))
	totalProcessed := 0
	totalModified := 0

	for _, nt := range tables {
		pl := PerCleanResult{Name: nt.Name}
		outTbl, processed, modified, err := processSingleTable(nt.Table, req.Options)
		if err != nil {
			msg := err.Error()
			pl.Error = &msg
			perList = append(perList, pl)
			continue
		}
		pl.Processed = processed
		pl.Modified = modified
		pl.Result = outTbl
		perList = append(perList, pl)
		totalProcessed += processed
		totalModified += modified
	}

	res.PerList = perList
	res.Summary = map[string]int{
		"tables_count":    len(perList),
		"processed_total": totalProcessed,
		"modified_total":  totalModified,
		"duration_ms":     int(time.Since(start).Milliseconds()),
	}
	res.Error = nil
	return res, nil
}
