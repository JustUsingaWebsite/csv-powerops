package csvops

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/utils"
)

type ManyToOneOptions struct {
	MatchMethod MatchMethod `json:"match_method"` // "exact" | "case_insensitive"
	TrimSpaces  bool        `json:"trim_spaces"`
}

type ManyToOneTarget struct {
	OneKey  string `json:"one_key"`  // e.g., "User"
	ManyKey string `json:"many_key"` // e.g., "Device"
	Value   string `json:"value"`    // required: user we are filtering by
}

type ManyToOneRequest struct {
	Operation string           `json:"operation"`
	Options   ManyToOneOptions `json:"options"`
	Target    ManyToOneTarget  `json:"target"`
	Dataset   types.TableData  `json:"dataset"`
}

type ManyToOneResponse struct {
	Operation  string              `json:"operation"`
	Summary    types.ResultSummary `json:"summary"`
	Matched    *types.TableData    `json:"matched"`
	Error      *string             `json:"error"`
	RawRequest json.RawMessage     `json:"-"`
}

// ManyToOne returns all rows where one_key == value
func ManyToOne(req ManyToOneRequest) (ManyToOneResponse, error) {
	var res ManyToOneResponse
	res.Operation = req.Operation
	start := time.Now()

	// Validation
	if strings.TrimSpace(req.Target.OneKey) == "" || strings.TrimSpace(req.Target.ManyKey) == "" || strings.TrimSpace(req.Target.Value) == "" {
		msg := "target.one_key, target.many_key, and target.value are required"
		res.Error = &msg
		return res, errors.New(msg)
	}
	if req.Dataset.Rows == nil {
		msg := "dataset required"
		res.Error = &msg
		return res, errors.New(msg)
	}

	// Resolve indices
	oneIdx, err := utils.ResolveKeyIndex(req.Dataset, req.Target.OneKey)
	if err != nil {
		msg := "one_key resolution: " + err.Error()
		res.Error = &msg
		return res, errors.New(msg)
	}
	manyIdx, err := utils.ResolveKeyIndex(req.Dataset, req.Target.ManyKey)
	if err != nil {
		msg := "many_key resolution: " + err.Error()
		res.Error = &msg
		return res, errors.New(msg)
	}
	_ = manyIdx // kept for validation purposes

	processed := 0
	matched := 0
	valNorm := utils.Normalize(req.Target.Value, req.Options.TrimSpaces, req.Options.MatchMethod == MatchCaseInsensitive)
	outRows := make([][]string, 0)

	for _, row := range req.Dataset.Rows {
		processed++
		keyVal := ""
		if oneIdx < len(row) {
			keyVal = utils.Normalize(row[oneIdx], req.Options.TrimSpaces, req.Options.MatchMethod == MatchCaseInsensitive)
		}
		if keyVal == valNorm {
			matched++
			outRows = append(outRows, append([]string(nil), row...))
		}
	}

	res.Matched = &types.TableData{
		HasHeader: req.Dataset.HasHeader,
		Header:    append([]string(nil), req.Dataset.Header...),
		Rows:      outRows,
	}
	res.Summary = types.ResultSummary{
		Processed:  processed,
		Matched:    matched,
		Missing:    processed - matched,
		DurationMS: time.Since(start).Milliseconds(),
	}
	return res, nil
}
