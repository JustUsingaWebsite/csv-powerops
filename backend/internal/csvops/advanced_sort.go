package csvops

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/utils"
)

// --- Sort modes / options ---

type SortMode string
type SortOrder string

const (
	SortAlpha   SortMode = "alphabetical"
	SortNumeric SortMode = "numeric"
	SortDate    SortMode = "date"

	OrderAsc  SortOrder = "asc"
	OrderDesc SortOrder = "desc"
)

// request/response types
type AdvancedSortOptions struct {
	Mode            SortMode  `json:"mode"`             // alphabetical | numeric | date
	Order           SortOrder `json:"order"`            // asc | desc
	Key             string    `json:"key"`              // column name or numeric index string
	TrimSpaces      bool      `json:"trim_spaces"`      // apply trimming before comparisons
	CaseInsensitive bool      `json:"case_insensitive"` // for alphabetical mode
	DateFormat      string    `json:"date_format"`      // optional explicit Go layout
}

type AdvancedSortRequest struct {
	Operation string              `json:"operation"`
	Options   AdvancedSortOptions `json:"options"`
	Datasets  types.MultiDatasets `json:"datasets"`
}

type PerSortResult struct {
	Name      string          `json:"name"`
	Processed int             `json:"processed"`
	Sorted    int             `json:"sorted"`
	Result    types.TableData `json:"result"`
	Error     *string         `json:"error"`
}

type AdvancedSortResponse struct {
	Operation string          `json:"operation"`
	Summary   map[string]int  `json:"summary"`
	PerList   []PerSortResult `json:"per_list"`
	Error     *string         `json:"error"`
}

// parseDate attempts common layouts or explicit layout if provided
func parseDateGuess(s string, explicitLayout string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	if explicitLayout != "" {
		if t, err := time.Parse(explicitLayout, s); err == nil {
			return t, true
		}
	}
	layouts := []string{
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
	}
	for _, L := range layouts {
		if t, err := time.Parse(L, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// sortSingleTable sorts a single TableData according to options
func sortSingleTable(tbl types.TableData, opts AdvancedSortOptions) (types.TableData, int, error) {
	// resolve key index
	idx, err := utils.ResolveKeyIndex(tbl, opts.Key)
	if err != nil {
		return types.TableData{}, 0, fmt.Errorf("key resolution: %w", err)
	}

	// prepare rows copy
	rows := make([][]string, 0, len(tbl.Rows))
	for _, r := range tbl.Rows {
		rows = append(rows, append([]string(nil), r...))
	}
	processed := len(rows)

	// comparator uses extracted sort value per row
	type rowWrap struct {
		row      []string
		alphaKey string
		numKey   float64
		numOk    bool
		dateKey  time.Time
		dateOk   bool
	}

	wrapped := make([]rowWrap, 0, len(rows))
	for _, r := range rows {
		w := rowWrap{row: r}
		cell := ""
		if idx < len(r) {
			cell = r[idx]
		}
		if opts.TrimSpaces {
			cell = strings.TrimSpace(cell)
		}
		switch opts.Mode {
		case SortAlpha:
			if opts.CaseInsensitive {
				w.alphaKey = strings.ToLower(cell)
			} else {
				w.alphaKey = cell
			}
		case SortNumeric:
			// parse float
			if v, ok := tryParseFloat(cell); ok {
				w.numKey = v
				w.numOk = true
			} else {
				w.numOk = false
			}
		case SortDate:
			if t, ok := parseDateGuess(cell, opts.DateFormat); ok {
				w.dateKey = t
				w.dateOk = true
			} else {
				w.dateOk = false
			}
		}
		wrapped = append(wrapped, w)
	}

	// Define sort function
	asc := opts.Order == OrderAsc

	sort.SliceStable(wrapped, func(i, j int) bool {
		a := wrapped[i]
		b := wrapped[j]
		switch opts.Mode {
		case SortAlpha:
			ai := a.alphaKey
			bi := b.alphaKey
			if ai == bi {
				// stable tie-breaker: preserve original order (SliceStable handles)
				return false
			}
			if asc {
				return ai < bi
			}
			return ai > bi
		case SortNumeric:
			// treat non-parsable values as greater-than for ascending (so they go to end)
			// For descending, reverse behavior
			if a.numOk && b.numOk {
				if a.numKey == b.numKey {
					return false
				}
				if asc {
					return a.numKey < b.numKey
				}
				return a.numKey > b.numKey
			}
			// if only a is ok
			if a.numOk && !b.numOk {
				return asc // when asc, valid numeric comes before invalid -> true; when desc -> false
			}
			if !a.numOk && b.numOk {
				return !asc
			}
			// both invalid: fallback to alphabetical compare on raw cell (trim/case handled earlier?)
			ai := ""
			bi := ""
			if idx < len(a.row) {
				ai = a.row[idx]
			}
			if idx < len(b.row) {
				bi = b.row[idx]
			}
			if opts.CaseInsensitive {
				ai = strings.ToLower(strings.TrimSpace(ai))
				bi = strings.ToLower(strings.TrimSpace(bi))
			}
			if asc {
				return ai < bi
			}
			return ai > bi
		case SortDate:
			// valid dates sort chronologically; invalid dates treated like numeric invalid values
			if a.dateOk && b.dateOk {
				if a.dateKey.Equal(b.dateKey) {
					return false
				}
				if asc {
					return a.dateKey.Before(b.dateKey)
				}
				return a.dateKey.After(b.dateKey)
			}
			if a.dateOk && !b.dateOk {
				return asc
			}
			if !a.dateOk && b.dateOk {
				return !asc
			}
			// both invalid: fallback to alpha
			ai := ""
			bi := ""
			if idx < len(a.row) {
				ai = a.row[idx]
			}
			if idx < len(b.row) {
				bi = b.row[idx]
			}
			if opts.CaseInsensitive {
				ai = strings.ToLower(strings.TrimSpace(ai))
				bi = strings.ToLower(strings.TrimSpace(bi))
			}
			if asc {
				return ai < bi
			}
			return ai > bi
		default:
			// unknown mode -> fallback to alpha asc
			ai := ""
			bi := ""
			if idx < len(a.row) {
				ai = a.row[idx]
			}
			if idx < len(b.row) {
				bi = b.row[idx]
			}
			if asc {
				return ai < bi
			}
			return ai > bi
		}
	})

	// reconstruct rows
	sortedRows := make([][]string, 0, len(wrapped))
	for _, w := range wrapped {
		sortedRows = append(sortedRows, append([]string(nil), w.row...))
	}

	out := types.TableData{
		HasHeader: tbl.HasHeader,
		Header:    append([]string(nil), tbl.Header...),
		Rows:      sortedRows,
	}
	return out, processed, nil
}

func tryParseFloat(s string) (float64, bool) {
	if strings.TrimSpace(s) == "" {
		return 0, false
	}
	// attempt to remove commas
	clean := strings.ReplaceAll(s, ",", "")
	if f, err := strconv.ParseFloat(clean, 64); err == nil {
		return f, true
	}
	// attempt to parse with possible currency or extra chars by scanning prefix
	if f, err := strconv.ParseFloat(strings.Fields(clean)[0], 64); err == nil {
		return f, true
	}
	return 0, false
}

// AdvancedSort sorts each table provided in datasets.Lists (or master if lists empty) with the given options.
func AdvancedSort(req AdvancedSortRequest) (AdvancedSortResponse, error) {
	var res AdvancedSortResponse
	res.Operation = req.Operation
	start := time.Now()

	// Validate options
	if req.Options.Mode == "" {
		msg := "sort mode required"
		res.Error = &msg
		return res, errors.New(msg)
	}
	if req.Options.Key == "" {
		msg := "sort key required"
		res.Error = &msg
		return res, errors.New(msg)
	}
	if req.Options.Order == "" {
		req.Options.Order = OrderAsc
	}

	// determine tables to operate on
	tables := []types.NamedTable{}
	if len(req.Datasets.Lists) > 0 {
		tables = append(tables, req.Datasets.Lists...)
	} else if len(req.Datasets.Master.Rows) > 0 || len(req.Datasets.Master.Header) > 0 {
		// wrap master as a single dataset named "dataset"
		tables = append(tables, types.NamedTable{
			Name:    "dataset",
			ListKey: "", // not needed here
			Table:   req.Datasets.Master,
		})
	} else {
		msg := "no tables provided"
		res.Error = &msg
		return res, errors.New(msg)
	}

	perList := make([]PerSortResult, 0, len(tables))
	totalProcessed := 0
	totalSorted := 0

	for _, nt := range tables {
		pr := PerSortResult{Name: nt.Name}
		// sort the table
		sorted, processed, err := sortSingleTable(nt.Table, req.Options)
		if err != nil {
			msg := err.Error()
			pr.Error = &msg
			perList = append(perList, pr)
			// continue to next table
			continue
		}
		pr.Processed = processed
		pr.Sorted = len(sorted.Rows)
		pr.Result = sorted
		totalProcessed += processed
		totalSorted += pr.Sorted
		perList = append(perList, pr)
	}

	res.PerList = perList
	res.Summary = map[string]int{
		"tables_count":    len(perList),
		"processed_total": totalProcessed,
		"sorted_total":    totalSorted,
		"duration_ms":     int(time.Since(start).Milliseconds()),
	}
	res.Error = nil
	return res, nil
}
