package csvops

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/utils"
)

// --- match/action types ---
type MatchMethod string
type ActionType string

const (
	MatchExact           MatchMethod = "exact"
	MatchCaseInsensitive MatchMethod = "case_insensitive"

	ActionTagged      ActionType = "tagged"
	ActionMatchesOnly ActionType = "matches_only"
	ActionMissingOnly ActionType = "missing_only"
)

// --- request/response types for crossref ---

type CrossRefRequest struct {
	Operation string           `json:"operation"`
	Options   CrossRefOptions  `json:"options"`
	Datasets  CrossRefDatasets `json:"datasets"`
}

type CrossRefOptions struct {
	MatchMethod     MatchMethod `json:"match_method"`
	Action          ActionType  `json:"action"`
	MasterKey       string      `json:"master_key"`
	ListKey         string      `json:"list_key"`
	TrimSpaces      bool        `json:"trim_spaces"`
	FoundColumnName string      `json:"found_column_name"`
}

type CrossRefDatasets struct {
	Master types.TableData `json:"master"`
	List   types.TableData `json:"list"`
}

type CrossRefResponse struct {
	Operation string              `json:"operation"`
	Summary   types.ResultSummary `json:"summary"`
	Result    types.TableData     `json:"result"`
	Error     *string             `json:"error"`
}

// --- Core function ---

// CrossRefJSON performs cross-referencing using shared types/utils.
func CrossRefJSON(req CrossRefRequest) (CrossRefResponse, error) {
	var res CrossRefResponse
	res.Operation = req.Operation
	start := time.Now()

	// Validate
	if strings.TrimSpace(req.Options.MasterKey) == "" {
		return resWithErr(res, "master_key is required"), errors.New("master_key required")
	}
	if req.Options.Action == "" {
		return resWithErr(res, "action is required"), errors.New("action required")
	}
	// If action==tagged enforce match method
	if req.Options.Action == ActionTagged && req.Options.MatchMethod == "" {
		return resWithErr(res, "match_method is required when action=tagged"), errors.New("match_method required for tagging")
	}

	// keys
	mKey := req.Options.MasterKey
	lKey := req.Options.ListKey
	if strings.TrimSpace(lKey) == "" {
		lKey = mKey
	}

	// resolve indices using utils.ResolveKeyIndex
	mKeyIdx, err := utils.ResolveKeyIndex(req.Datasets.Master, mKey)
	if err != nil {
		return resWithErr(res, "master key resolution: "+err.Error()), err
	}
	lKeyIdx, err := utils.ResolveKeyIndex(req.Datasets.List, lKey)
	if err != nil {
		return resWithErr(res, "list key resolution: "+err.Error()), err
	}

	// Build master lookup set
	masterSet := make(map[string]struct{}, len(req.Datasets.Master.Rows))
	for _, row := range req.Datasets.Master.Rows {
		if mKeyIdx < 0 || mKeyIdx >= len(row) {
			continue
		}
		val := row[mKeyIdx]
		norm := utils.Normalize(val, req.Options.TrimSpaces, req.Options.MatchMethod == MatchCaseInsensitive)
		masterSet[norm] = struct{}{}
	}

	// iterate list rows and build result according to action
	var processed, matched, missing int
	resultRows := make([][]string, 0, len(req.Datasets.List.Rows))
	resultHeader := append([]string(nil), req.Datasets.List.Header...)

	if req.Options.Action == ActionTagged {
		foundName := req.Options.FoundColumnName
		if strings.TrimSpace(foundName) == "" {
			foundName = "tagged"
		}
		resultHeader = append(resultHeader, foundName)
	}

	for _, row := range req.Datasets.List.Rows {
		processed++
		var present bool
		if lKeyIdx >= 0 && lKeyIdx < len(row) {
			k := row[lKeyIdx]
			n := utils.Normalize(k, req.Options.TrimSpaces, req.Options.MatchMethod == MatchCaseInsensitive)
			_, present = masterSet[n]
		} else {
			present = false
		}
		if present {
			matched++
		} else {
			missing++
		}

		switch req.Options.Action {
		case ActionTagged:
			if present { // NEW behavior: tagged returns only matched rows with tag=true
				newRow := append([]string(nil), row...)
				newRow = append(newRow, "true")
				resultRows = append(resultRows, newRow)
			}
		case ActionMatchesOnly:
			if present {
				resultRows = append(resultRows, append([]string(nil), row...))
			}
		case ActionMissingOnly:
			if !present {
				resultRows = append(resultRows, append([]string(nil), row...))
			}
		default:
			return resWithErr(res, "unsupported action"), errors.New("unsupported action")
		}
	}

	res.Summary = types.ResultSummary{
		Processed:  processed,
		Matched:    matched,
		Missing:    missing,
		DurationMS: time.Since(start).Milliseconds(),
	}
	res.Result = types.TableData{
		HasHeader: req.Datasets.List.HasHeader,
		Header:    resultHeader,
		Rows:      resultRows,
	}
	res.Error = nil
	return res, nil
}

// --- helpers ---
func resWithErr(r CrossRefResponse, msg string) CrossRefResponse {
	r.Error = &msg
	return r
}

// Decode helper if you receive raw JSON bytes
func DecodeCrossRefRequest(data []byte) (CrossRefRequest, error) {
	var req CrossRefRequest
	err := json.Unmarshal(data, &req)
	return req, err
}
