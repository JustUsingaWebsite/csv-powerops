package csvops

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/utils"
)

// --- Filter condition types ---

type ConditionOperator string

const (
	OpEquals      ConditionOperator = "equals"
	OpNotEquals   ConditionOperator = "not_equals"
	OpContains    ConditionOperator = "contains"
	OpNotContains ConditionOperator = "not_contains"
	OpStartsWith  ConditionOperator = "starts_with"
	OpEndsWith    ConditionOperator = "ends_with"
	OpIn          ConditionOperator = "in"
	OpNotIn       ConditionOperator = "not_in"
	OpGt          ConditionOperator = "gt"
	OpGte         ConditionOperator = "gte"
	OpLt          ConditionOperator = "lt"
	OpLte         ConditionOperator = "lte"
	OpDateAfter   ConditionOperator = "date_after"
	OpDateBefore  ConditionOperator = "date_before"
	OpIsTrue      ConditionOperator = "is_true"
	OpIsFalse     ConditionOperator = "is_false"
	OpIsNull      ConditionOperator = "is_null"
	OpIsNotNull   ConditionOperator = "is_not_null"
	OpMatches     ConditionOperator = "matches" // regex
)

// Condition describes a single atomic condition
type Condition struct {
	Column          string            `json:"column"`
	Operator        ConditionOperator `json:"operator"`
	Value           interface{}       `json:"value,omitempty"`            // string | number | []string
	CaseInsensitive *bool             `json:"case_insensitive,omitempty"` // override
	TrimSpaces      *bool             `json:"trim_spaces,omitempty"`      // override
}

// ConditionGroup composes conditions with logical operators
type ConditionGroup struct {
	Op        string           `json:"op"` // "and" | "or"
	Conds     []Condition      `json:"conds,omitempty"`
	SubGroups []ConditionGroup `json:"subgroups,omitempty"`
}

// Request/Response types
type AdvancedExtractOptions struct {
	TrimSpaces      bool   `json:"trim_spaces"`
	CaseInsensitive bool   `json:"case_insensitive"`
	DateFormat      string `json:"date_format,omitempty"` // optional precise format (go layout)
}

type PaginationOptions struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}

type AdvancedExtractRequest struct {
	Operation  string                 `json:"operation"`
	Options    AdvancedExtractOptions `json:"options"`
	Dataset    types.TableData        `json:"dataset"`
	Filter     ConditionGroup         `json:"filter"`
	Pagination PaginationOptions      `json:"pagination,omitempty"`
}

type AdvancedExtractResponse struct {
	Operation string              `json:"operation"`
	Summary   types.ResultSummary `json:"summary"`
	Result    types.TableData     `json:"result"`
	Error     *string             `json:"error"`
}

// --- AdvancedExtract implementation ---

// tryParseNumber attempts to parse s as float64; returns ok=false if not parseable.
func tryParseNumber(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// tryParseDate attempts to parse s with multiple layouts (or explicit layout if provided).
func tryParseDate(s string, explicitLayout string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	layouts := []string{}
	if explicitLayout != "" {
		layouts = append(layouts, explicitLayout)
	}
	// common layouts (order matters: prefer ISO/RFC)
	layouts = append(layouts,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
		"02/01/2006 15:04",
		"01/02/2006 15:04",
		"02/01/2006",
		"01/02/2006",
		"1/2/2006 15:04",
		"2006-01-02 03:04PM",
		"02 Jan 2006 15:04",
		"02 Jan 2006",
	)
	for _, L := range layouts {
		if t, err := time.Parse(L, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// stringInList: supports string value or []interface{}
func stringInList(elem string, listVal interface{}, caseInsensitive bool, trim bool) bool {
	if listVal == nil {
		return false
	}
	// if listVal is []interface{}
	switch v := listVal.(type) {
	case []interface{}:
		for _, it := range v {
			its := fmt.Sprintf("%v", it)
			if trim {
				its = strings.TrimSpace(its)
			}
			if caseInsensitive {
				if strings.EqualFold(elem, its) {
					return true
				}
			} else {
				if elem == its {
					return true
				}
			}
		}
	case []string:
		for _, its := range v {
			if trim {
				its = strings.TrimSpace(its)
			}
			if caseInsensitive {
				if strings.EqualFold(elem, its) {
					return true
				}
			} else {
				if elem == its {
					return true
				}
			}
		}
	default:
		// maybe a comma-separated string
		s := fmt.Sprintf("%v", listVal)
		parts := strings.Split(s, ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if caseInsensitive {
				if strings.EqualFold(elem, p) {
					return true
				}
			} else {
				if elem == p {
					return true
				}
			}
		}
	}
	return false
}

func coerceToBool(s string) (bool, bool) {
	s = strings.TrimSpace(strings.ToLower(s))
	trueSet := map[string]struct{}{"true": {}, "1": {}, "yes": {}, "y": {}, "t": {}}
	falseSet := map[string]struct{}{"false": {}, "0": {}, "no": {}, "n": {}, "f": {}}
	if _, ok := trueSet[s]; ok {
		return true, true
	}
	if _, ok := falseSet[s]; ok {
		return false, true
	}
	return false, false
}

// gatherColumnsFromGroup recursively collects all column names referenced by the filter group
func gatherColumnsFromGroup(g ConditionGroup, cols map[string]struct{}) {
	for _, c := range g.Conds {
		key := strings.ToLower(strings.TrimSpace(c.Column))
		if key != "" {
			cols[key] = struct{}{}
		}
	}
	for _, sg := range g.SubGroups {
		gatherColumnsFromGroup(sg, cols)
	}
}

// getBool applies per-cond or global options for bools
func boolOption(global bool, per *bool) bool {
	if per == nil {
		return global
	}
	return *per
}

// evalCondition evaluates a single condition against a row
func evalCondition(cond Condition, row []string, headerMap map[string]int, globalOptions AdvancedExtractOptions) (bool, error) {
	col := strings.ToLower(strings.TrimSpace(cond.Column))
	idx, ok := headerMap[col]
	if !ok {
		return false, fmt.Errorf("column '%s' not found in dataset", cond.Column)
	}
	// determine normalization flags
	trim := boolOption(globalOptions.TrimSpaces, cond.TrimSpaces)
	caseInsensitive := boolOption(globalOptions.CaseInsensitive, cond.CaseInsensitive)

	// fetch cell value (if index outside row bounds treat as empty)
	cell := ""
	if idx < len(row) {
		cell = row[idx]
	}
	if trim {
		cell = utils.WhitespaceTrimmer(cell)
	}

	switch cond.Operator {
	case OpEquals:
		// numeric equality if cond.Value is numeric
		switch v := cond.Value.(type) {
		case float64:
			if n, ok := tryParseNumber(cell); ok {
				return n == v, nil
			}
			return false, nil
		default:
			vstr := fmt.Sprintf("%v", cond.Value)
			if trim {
				vstr = strings.TrimSpace(vstr)
			}
			if caseInsensitive {
				return strings.EqualFold(cell, vstr), nil
			}
			return cell == vstr, nil
		}
	case OpNotEquals:
		ok, _ := evalCondition(Condition{Column: cond.Column, Operator: OpEquals, Value: cond.Value, CaseInsensitive: cond.CaseInsensitive, TrimSpaces: cond.TrimSpaces}, row, headerMap, globalOptions)
		return !ok, nil
	case OpContains:
		vstr := fmt.Sprintf("%v", cond.Value)
		if trim {
			vstr = strings.TrimSpace(vstr)
		}
		if caseInsensitive {
			return strings.Contains(strings.ToLower(cell), strings.ToLower(vstr)), nil
		}
		return strings.Contains(cell, vstr), nil
	case OpNotContains:
		ok, _ := evalCondition(Condition{Column: cond.Column, Operator: OpContains, Value: cond.Value, CaseInsensitive: cond.CaseInsensitive, TrimSpaces: cond.TrimSpaces}, row, headerMap, globalOptions)
		return !ok, nil
	case OpStartsWith:
		vstr := fmt.Sprintf("%v", cond.Value)
		if trim {
			vstr = strings.TrimSpace(vstr)
		}
		if caseInsensitive {
			return strings.HasPrefix(strings.ToLower(cell), strings.ToLower(vstr)), nil
		}
		return strings.HasPrefix(cell, vstr), nil
	case OpEndsWith:
		vstr := fmt.Sprintf("%v", cond.Value)
		if trim {
			vstr = strings.TrimSpace(vstr)
		}
		if caseInsensitive {
			return strings.HasSuffix(strings.ToLower(cell), strings.ToLower(vstr)), nil
		}
		return strings.HasSuffix(cell, vstr), nil
	case OpIn:
		// value can be []interface{} or string
		target := cond.Value
		elem := cell
		if trim {
			elem = strings.TrimSpace(elem)
		}
		return stringInList(elem, target, caseInsensitive, trim), nil
	case OpNotIn:
		ok := false
		ok = stringInList(cell, cond.Value, caseInsensitive, trim)
		return !ok, nil
	case OpGt, OpGte, OpLt, OpLte:
		// try numeric compare first
		if vnum, ok := cond.Value.(float64); ok {
			if cnum, ok2 := tryParseNumber(cell); ok2 {
				switch cond.Operator {
				case OpGt:
					return cnum > vnum, nil
				case OpGte:
					return cnum >= vnum, nil
				case OpLt:
					return cnum < vnum, nil
				case OpLte:
					return cnum <= vnum, nil
				}
			}
			// numeric value but cell not numeric -> false
			return false, nil
		}
		// try parsing both as numbers from string if cond.Value is string
		vstr := fmt.Sprintf("%v", cond.Value)
		if trim {
			vstr = strings.TrimSpace(vstr)
		}
		if vnum, err := strconv.ParseFloat(vstr, 64); err == nil {
			if cnum, ok := tryParseNumber(cell); ok {
				switch cond.Operator {
				case OpGt:
					return cnum > vnum, nil
				case OpGte:
					return cnum >= vnum, nil
				case OpLt:
					return cnum < vnum, nil
				case OpLte:
					return cnum <= vnum, nil
				}
			}
			// attempt date comparison as fallback
			if t1, ok1 := tryParseDate(cell, globalOptions.DateFormat); ok1 {
				if t2, ok2 := tryParseDate(vstr, globalOptions.DateFormat); ok2 {
					switch cond.Operator {
					case OpGt:
						return t1.After(t2), nil
					case OpGte:
						return t1.After(t2) || t1.Equal(t2), nil
					case OpLt:
						return t1.Before(t2), nil
					case OpLte:
						return t1.Before(t2) || t1.Equal(t2), nil
					}
				}
			}
			return false, nil
		}
		// final fallback: try date parsing direct
		if t1, ok1 := tryParseDate(cell, globalOptions.DateFormat); ok1 {
			if vstr := fmt.Sprintf("%v", cond.Value); vstr != "" {
				if t2, ok2 := tryParseDate(vstr, globalOptions.DateFormat); ok2 {
					switch cond.Operator {
					case OpGt:
						return t1.After(t2), nil
					case OpGte:
						return t1.After(t2) || t1.Equal(t2), nil
					case OpLt:
						return t1.Before(t2), nil
					case OpLte:
						return t1.Before(t2) || t1.Equal(t2), nil
					}
				}
			}
		}
		return false, nil
	case OpDateAfter:
		vstr := fmt.Sprintf("%v", cond.Value)
		if trim {
			vstr = strings.TrimSpace(vstr)
		}
		t1, ok1 := tryParseDate(cell, globalOptions.DateFormat)
		t2, ok2 := tryParseDate(vstr, globalOptions.DateFormat)
		if !ok1 || !ok2 {
			return false, nil
		}
		return t1.After(t2), nil
	case OpDateBefore:
		vstr := fmt.Sprintf("%v", cond.Value)
		if trim {
			vstr = strings.TrimSpace(vstr)
		}
		t1, ok1 := tryParseDate(cell, globalOptions.DateFormat)
		t2, ok2 := tryParseDate(vstr, globalOptions.DateFormat)
		if !ok1 || !ok2 {
			return false, nil
		}
		return t1.Before(t2), nil
	case OpIsTrue:
		if b, ok := coerceToBool(cell); ok {
			return b, nil
		}
		return false, nil
	case OpIsFalse:
		if b, ok := coerceToBool(cell); ok {
			return !b, nil
		}
		return false, nil
	case OpIsNull:
		if strings.TrimSpace(cell) == "" {
			return true, nil
		}
		return false, nil
	case OpIsNotNull:
		return strings.TrimSpace(cell) != "", nil
	case OpMatches:
		pat := fmt.Sprintf("%v", cond.Value)
		if trim {
			pat = strings.TrimSpace(pat)
		}
		re, err := regexp.Compile(pat)
		if err != nil {
			return false, fmt.Errorf("invalid regex in matches: %v", err)
		}
		return re.MatchString(cell), nil
	default:
		return false, fmt.Errorf("unsupported operator: %v", cond.Operator)
	}
}

// evalGroup recursively evaluates a condition group
func evalGroup(g ConditionGroup, row []string, headerMap map[string]int, globalOptions AdvancedExtractOptions) (bool, error) {
	op := strings.ToLower(strings.TrimSpace(g.Op))
	if op != "and" && op != "or" {
		return false, fmt.Errorf("invalid group op: %s", g.Op)
	}
	if op == "and" {
		for _, cond := range g.Conds {
			ok, err := evalCondition(cond, row, headerMap, globalOptions)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
		}
		for _, sg := range g.SubGroups {
			ok, err := evalGroup(sg, row, headerMap, globalOptions)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
		}
		return true, nil
	}
	// op == "or"
	for _, cond := range g.Conds {
		ok, err := evalCondition(cond, row, headerMap, globalOptions)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	for _, sg := range g.SubGroups {
		ok, err := evalGroup(sg, row, headerMap, globalOptions)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// AdvancedExtract executes the provided filter on the dataset and returns matching rows.
func AdvancedExtract(req AdvancedExtractRequest) (AdvancedExtractResponse, error) {
	var res AdvancedExtractResponse
	res.Operation = req.Operation
	start := time.Now()

	// Validate dataset and filter
	if req.Dataset.Rows == nil {
		msg := "dataset required"
		res.Error = &msg
		return res, errors.New(msg)
	}
	// Build header -> index map (lowercased, trimmed keys)
	headerMap := map[string]int{}
	for i, h := range req.Dataset.Header {
		headerMap[strings.ToLower(strings.TrimSpace(h))] = i
	}

	// Gather referenced columns and validate they exist
	cols := map[string]struct{}{}
	gatherColumnsFromGroup(req.Filter, cols)
	for c := range cols {
		if _, ok := headerMap[c]; !ok {
			// build available headers list for helpful error
			avail := strings.Join(req.Dataset.Header, ", ")
			msg := fmt.Sprintf("filter column '%s' not found in dataset. available headers: [%s]", c, avail)
			res.Error = &msg
			return res, errors.New(msg)
		}
	}

	processed := 0
	matchedRows := make([][]string, 0, 64)

	// Pre-normalization decisions are applied per condition inside evalCondition
	for _, row := range req.Dataset.Rows {
		processed++
		ok, err := evalGroup(req.Filter, row, headerMap, req.Options)
		if err != nil {
			msg := err.Error()
			res.Error = &msg
			return res, err
		}
		if ok {
			// copy row to avoid aliasing
			matchedRows = append(matchedRows, append([]string(nil), row...))
		}
	}

	// pagination
	offset := req.Pagination.Offset
	limit := req.Pagination.Limit
	if limit <= 0 {
		limit = len(matchedRows)
	}
	// adjust bounds
	if offset < 0 {
		offset = 0
	}
	if offset > len(matchedRows) {
		offset = len(matchedRows)
	}
	end := offset + limit
	if end > len(matchedRows) {
		end = len(matchedRows)
	}
	pagedRows := matchedRows[offset:end]

	res.Result = types.TableData{
		HasHeader: req.Dataset.HasHeader,
		Header:    append([]string(nil), req.Dataset.Header...),
		Rows:      pagedRows,
	}
	res.Summary = types.ResultSummary{
		Processed:  processed,
		Matched:    len(matchedRows),
		Missing:    processed - len(matchedRows),
		DurationMS: time.Since(start).Milliseconds(),
	}
	res.Error = nil
	return res, nil
}
