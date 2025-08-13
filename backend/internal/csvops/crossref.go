package csvops

import (
	"errors"
	"strings"
	"time"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/utils"
)

// Only the minimal types needed for multi-list crossref

type MatchMethod string

const (
	MatchExact           MatchMethod = "exact"
	MatchCaseInsensitive MatchMethod = "case_insensitive"
)

// CrossRefMultiRequest carries a master table and an array of named lists to compare.
type CrossRefMultiRequest struct {
	Operation string               `json:"operation"`
	Options   CrossRefMultiOptions `json:"options"`
	Datasets  types.MultiDatasets  `json:"datasets"`
}

type CrossRefMultiOptions struct {
	MatchMethod    MatchMethod `json:"match_method"` // exact | case_insensitive
	MasterKey      string      `json:"master_key"`   // header name or numeric index string
	DefaultListKey string      `json:"list_key"`     // fallback list key if per-list not provided
	TrimSpaces     bool        `json:"trim_spaces"`
}

// PerListResult returns stats and the matched rows for a single list.
type PerListResult struct {
	Name      string          `json:"name"`
	Processed int             `json:"processed"`
	Matched   int             `json:"matched"`
	Missing   int             `json:"missing"`
	Result    types.TableData `json:"result"`
	Error     *string         `json:"error"`
}

// CrossRefMultiResponse contains per-list results and a small summary.
type CrossRefMultiResponse struct {
	Operation string          `json:"operation"`
	Summary   map[string]int  `json:"summary"`
	PerList   []PerListResult `json:"per_list"`
	Error     *string         `json:"error"`
}

// CrossRefMulti compares the master key against each list and returns matched rows per list.
// It does not merge results. Non-fatal list-level errors are reported in per_list[].error.
func CrossRefMulti(req CrossRefMultiRequest) (CrossRefMultiResponse, error) {
	var res CrossRefMultiResponse
	res.Operation = req.Operation
	start := time.Now()

	// validate master key presence
	if strings.TrimSpace(req.Options.MasterKey) == "" {
		msg := "master_key required"
		res.Error = &msg
		return res, errors.New(msg)
	}

	// resolve master key index
	mKeyIdx, err := utils.ResolveKeyIndex(req.Datasets.Master, req.Options.MasterKey)
	if err != nil {
		msg := "master key resolution: " + err.Error()
		res.Error = &msg
		return res, err
	}

	// build normalized master set
	masterSet := make(map[string]struct{}, len(req.Datasets.Master.Rows))
	for _, row := range req.Datasets.Master.Rows {
		if mKeyIdx < 0 || mKeyIdx >= len(row) {
			continue
		}
		n := utils.Normalize(row[mKeyIdx], req.Options.TrimSpaces, req.Options.MatchMethod == MatchCaseInsensitive)
		masterSet[n] = struct{}{}
	}

	totalProcessed := 0
	totalMatched := 0
	perList := make([]PerListResult, 0, len(req.Datasets.Lists))

	// iterate each provided list
	for _, named := range req.Datasets.Lists {
		pl := PerListResult{Name: named.Name}

		// determine list key (per-list override -> default -> master key)
		listKey := strings.TrimSpace(named.ListKey)
		if listKey == "" {
			listKey = req.Options.DefaultListKey
		}
		if listKey == "" {
			listKey = req.Options.MasterKey
		}

		// resolve index for list
		lKeyIdx, err := utils.ResolveKeyIndex(named.Table, listKey)
		if err != nil {
			msg := "list key resolution: " + err.Error()
			pl.Error = &msg
			perList = append(perList, pl)
			continue // skip this list but continue others
		}

		matches := [][]string{}
		processed := 0
		matched := 0
		missing := 0

		for _, row := range named.Table.Rows {
			processed++
			totalProcessed++
			keyVal := ""
			if lKeyIdx < len(row) {
				keyVal = utils.Normalize(row[lKeyIdx], req.Options.TrimSpaces, req.Options.MatchMethod == MatchCaseInsensitive)
			}
			if _, ok := masterSet[keyVal]; ok {
				matched++
				totalMatched++
				// copy row to avoid aliasing
				matches = append(matches, append([]string(nil), row...))
			} else {
				missing++
			}
		}

		pl.Processed = processed
		pl.Matched = matched
		pl.Missing = missing
		pl.Result = types.TableData{
			HasHeader: named.Table.HasHeader,
			Header:    append([]string(nil), named.Table.Header...),
			Rows:      matches,
		}

		perList = append(perList, pl)
	}

	// summary
	res.Summary = map[string]int{
		"master_count":    len(req.Datasets.Master.Rows),
		"lists_count":     len(req.Datasets.Lists),
		"processed_total": totalProcessed,
		"matched_total":   totalMatched,
	}
	res.PerList = perList
	res.Error = nil
	// add duration in ms
	res.Summary["duration_ms"] = int(time.Since(start).Milliseconds())
	return res, nil
}
