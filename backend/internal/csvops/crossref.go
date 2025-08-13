package csvops

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

// --- Types for request/response ---

type MatchMethod string
type ActionType string

const (
	MatchExact           MatchMethod = "exact"
	MatchCaseInsensitive MatchMethod = "case_insensitive"

	ActionTagged      ActionType = "tagged"
	ActionMatchesOnly ActionType = "matches_only"
	ActionMissingOnly ActionType = "missing_only"
)

// CrossRefRequest represents incoming JSON.
type CrossRefRequest struct {
	Operation string           `json:"operation"`
	Options   CrossRefOptions  `json:"options"`
	Datasets  CrossRefDatasets `json:"datasets"`
}

type CrossRefOptions struct {
	MatchMethod     MatchMethod `json:"match_method"` // required for tagging per your rule
	Action          ActionType  `json:"action"`       // tagged | matches_only | missing_only
	MasterKey       string      `json:"master_key"`   // header name or numeric index string
	ListKey         string      `json:"list_key"`     // optional; if empty, MasterKey will be used for list too
	TrimSpaces      bool        `json:"trim_spaces"`
	FoundColumnName string      `json:"found_column_name"` // only used for tagged
}

type CrossRefDatasets struct {
	Master TableData `json:"master"`
	List   TableData `json:"list"`
}

type TableData struct {
	HasHeader bool       `json:"hasHeader"`
	Header    []string   `json:"header"`
	Rows      [][]string `json:"rows"`
}

type CrossRefResponse struct {
	Operation string        `json:"operation"`
	Summary   ResultSummary `json:"summary"`
	Result    TableData     `json:"result"`
	Error     *string       `json:"error"` // nil on success
}

type ResultSummary struct {
	Processed  int   `json:"processed"`
	Matched    int   `json:"matched"`
	Missing    int   `json:"missing"`
	DurationMS int64 `json:"durationMs"`
}

// --- Core function ---

// CrossRefJSON performs cross-referencing using the agreed JSON contract.
// It returns a CrossRefResponse (struct) and an error (non-nil for internal failure).
func CrossRefJSON(req CrossRefRequest) (CrossRefResponse, error) {
	var res CrossRefResponse
	res.Operation = req.Operation
	start := time.Now()

	// Validate options
	if req.Options.MasterKey == "" {
		return resWithErr(res, "master_key is required"), errors.New("master_key required")
	}
	if req.Options.Action == "" {
		return resWithErr(res, "action is required (tagged|matches_only|missing_only)"), errors.New("action required")
	}
	// Enforce your rule: if action == tagged, require a match method
	if req.Options.Action == ActionTagged && req.Options.MatchMethod == "" {
		return resWithErr(res, "match_method is required when action=tagged"), errors.New("match_method required for tagging")
	}

	// Determine which key names/indices to use for master & list
	mKey := req.Options.MasterKey
	lKey := req.Options.ListKey
	if lKey == "" {
		lKey = mKey
	}

	// Resolve master key index
	mKeyIdx, err := resolveKeyIndex(req.Datasets.Master, mKey)
	if err != nil {
		return resWithErr(res, "master key resolution: "+err.Error()), err
	}
	// Resolve list key index
	lKeyIdx, err := resolveKeyIndex(req.Datasets.List, lKey)
	if err != nil {
		return resWithErr(res, "list key resolution: "+err.Error()), err
	}

	// Build master lookup set
	masterSet := make(map[string]struct{}, len(req.Datasets.Master.Rows))
	for _, row := range req.Datasets.Master.Rows {
		if mKeyIdx < 0 || mKeyIdx >= len(row) {
			continue
		}
		k := normalize(row[mKeyIdx], req.Options.TrimSpaces, req.Options.MatchMethod)
		masterSet[k] = struct{}{}
	}

	// Process list rows
	var processed, matched, missing int
	resultRows := make([][]string, 0, len(req.Datasets.List.Rows))
	resultHeader := append([]string(nil), req.Datasets.List.Header...)

	// For tagged action, append found column to header
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
			k := normalize(row[lKeyIdx], req.Options.TrimSpaces, req.Options.MatchMethod)
			_, present = masterSet[k]
		} else {
			// missing key field in this row -> treat as not present
			present = false
		}
		if present {
			matched++
		} else {
			missing++
		}

		switch req.Options.Action {
		case ActionTagged:
			newRow := append([]string(nil), row...)
			if present {
				newRow = append(newRow, "true")
			} else {
				newRow = append(newRow, "false")
			}
			resultRows = append(resultRows, newRow)
		case ActionMatchesOnly:
			if present {
				resultRows = append(resultRows, append([]string(nil), row...))
			}
		case ActionMissingOnly:
			if !present {
				resultRows = append(resultRows, append([]string(nil), row...))
			}
		default:
			// unknown action (shouldn't happen because of earlier validation)
			return resWithErr(res, "unsupported action"), errors.New("unsupported action")
		}
	}

	// Fill response
	res.Summary = ResultSummary{
		Processed:  processed,
		Matched:    matched,
		Missing:    missing,
		DurationMS: time.Since(start).Milliseconds(),
	}
	res.Result = TableData{
		HasHeader: req.Datasets.List.HasHeader,
		Header:    resultHeader,
		Rows:      resultRows,
	}
	res.Error = nil
	return res, nil
}

// --- Helpers ---

// resolveKeyIndex returns the numeric index for a key that can be a header name or numeric index string.
// If dataset.HasHeader == false, the key MUST be a numeric index string like "0".
func resolveKeyIndex(tbl TableData, key string) (int, error) {
	if tbl.HasHeader {
		// try find by header name (case-insensitive, trimmed)
		for i, h := range tbl.Header {
			if strings.EqualFold(strings.TrimSpace(h), strings.TrimSpace(key)) {
				return i, nil
			}
		}
		// fallback: maybe key is numeric string pointing to index
		if idx, err := strconv.Atoi(key); err == nil {
			if idx < 0 || idx >= len(tbl.Header) {
				return -1, errors.New("numeric key index out of range")
			}
			return idx, nil
		}
		return -1, errors.New("key not found in header")
	}
	// no header -> key must be numeric index
	idx, err := strconv.Atoi(key)
	if err != nil {
		return -1, errors.New("no header: key must be numeric index string")
	}
	return idx, nil
}

// WhitespaceTrimmer removes leading/trailing/multiple spaces and normalizes whitespace.
func WhitespaceTrimmer(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Join(strings.Fields(s), " ") // collapse multiple spaces
	return s
}

// normalize performs trimming and case normalization per options.
// If matchMethod is empty, it behaves like exact match (no case-change).
func normalize(val string, trim bool, matchMethod MatchMethod) string {
	if trim {
		val = WhitespaceTrimmer(val)
	}
	if matchMethod == MatchCaseInsensitive {
		val = strings.ToLower(val)
	}
	return val
}

func resWithErr(r CrossRefResponse, msg string) CrossRefResponse {
	r.Error = &msg
	return r
}

// Helper to decode raw JSON bytes into CrossRefRequest
func DecodeCrossRefRequest(data []byte) (CrossRefRequest, error) {
	var req CrossRefRequest
	err := json.Unmarshal(data, &req)
	return req, err
}
