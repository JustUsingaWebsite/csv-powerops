package csvops

import (
	"errors"
	"strings"
	"time"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/utils"
)

// OneToMany simplified: search master + N lists for rows matching target key=value,
// return per-list matched rows (master included) and a combined result aligned to master header.

type OneToManyRequest struct {
	Operation string              `json:"operation"`
	Options   OneToManyOptions    `json:"options"`
	Target    OneToManyTarget     `json:"target"`
	Datasets  types.MultiDatasets `json:"datasets"`
}

type OneToManyOptions struct {
	MatchMethod MatchMethod `json:"match_method"` // exact | case_insensitive
	TrimSpaces  bool        `json:"trim_spaces"`
}

type OneToManyTarget struct {
	Key   string `json:"key"`   // column name or numeric index string, e.g. "DeviceName"
	Value string `json:"value"` // value to look up, e.g. "device1"
}

type OneToManyPerList struct {
	Name      string          `json:"name"`
	Processed int             `json:"processed"`
	Matched   int             `json:"matched"`
	Missing   int             `json:"missing"` // per your request, we will leave it 0 if you prefer; here we include processed & matched
	Result    types.TableData `json:"result"`
	Error     *string         `json:"error"`
}

type OneToManyResponse struct {
	Operation string             `json:"operation"`
	Summary   map[string]int     `json:"summary"`
	PerList   []OneToManyPerList `json:"per_list"`
	Combined  types.TableData    `json:"combined"` // aligned to master header + source_list column
	Error     *string            `json:"error"`
}

// OneToMany searches master & lists for rows where target.key == target.value.
func OneToMany(req OneToManyRequest) (OneToManyResponse, error) {
	var res OneToManyResponse
	res.Operation = req.Operation
	start := time.Now()

	// validate
	if strings.TrimSpace(req.Target.Key) == "" || strings.TrimSpace(req.Target.Value) == "" {
		msg := "target.key and target.value are required"
		res.Error = &msg
		return res, errors.New(msg)
	}
	if req.Datasets.Master.Rows == nil {
		msg := "master dataset required"
		res.Error = &msg
		return res, errors.New(msg)
	}

	// Normalize target value per options
	targetNorm := utils.Normalize(req.Target.Value, req.Options.TrimSpaces, req.Options.MatchMethod == MatchCaseInsensitive)

	// Resolve master key index
	mKeyIdx, err := utils.ResolveKeyIndex(req.Datasets.Master, req.Target.Key)
	if err != nil {
		headers := strings.Join(req.Datasets.Master.Header, ", ")
		msg := "master key resolution: " + err.Error() + ". available master headers: [" + headers + "]"
		res.Error = &msg
		return res, errors.New(msg)
	}

	// 1) Search master for matches
	masterMatches := [][]string{}
	masterProcessed := 0
	for _, row := range req.Datasets.Master.Rows {
		masterProcessed++
		var keyVal string
		if mKeyIdx < len(row) {
			keyVal = utils.Normalize(row[mKeyIdx], req.Options.TrimSpaces, req.Options.MatchMethod == MatchCaseInsensitive)
		}
		if keyVal == targetNorm {
			// keep entire master row as-is
			masterMatches = append(masterMatches, append([]string(nil), row...))
		}
	}

	// Per-list results: start with master as first entry
	perList := []OneToManyPerList{}

	masterPL := OneToManyPerList{
		Name:      "master",
		Processed: masterProcessed,
		Matched:   len(masterMatches),
		Missing:   0,
		Result: types.TableData{
			HasHeader: req.Datasets.Master.HasHeader,
			Header:    append([]string(nil), req.Datasets.Master.Header...),
			Rows:      masterMatches,
		},
		Error: nil,
	}
	perList = append(perList, masterPL)

	// 2) For each named list, search for matches and build per-list result
	totalProcessed := masterProcessed
	totalMatched := len(masterMatches)
	combinedRows := [][]string{}
	// We'll build combined rows later; first add master rows with source "master"
	// Combined header will be master.Header + "source_list"
	for _, r := range masterMatches {
		combinedRows = append(combinedRows, append([]string(nil), r...)) // will add source later when header is built
	}

	for _, named := range req.Datasets.Lists {
		pl := OneToManyPerList{
			Name:      named.Name,
			Processed: 0,
			Matched:   0,
			Missing:   0,
			Result: types.TableData{
				HasHeader: named.Table.HasHeader,
				Header:    append([]string(nil), named.Table.Header...),
				Rows:      [][]string{},
			},
			Error: nil,
		}

		// determine list key (per-list override -> master key)
		listKey := strings.TrimSpace(named.ListKey)
		if listKey == "" {
			listKey = req.Target.Key
		}

		lKeyIdx, lerr := utils.ResolveKeyIndex(named.Table, listKey)
		if lerr != nil {
			headers := strings.Join(named.Table.Header, ", ")
			msg := "list key resolution: " + lerr.Error() + ". available headers for list '" + named.Name + "': [" + headers + "]"
			pl.Error = &msg
			perList = append(perList, pl)
			continue
		}

		// scan rows
		for _, row := range named.Table.Rows {
			pl.Processed++
			totalProcessed++
			var keyVal string
			if lKeyIdx < len(row) {
				keyVal = utils.Normalize(row[lKeyIdx], req.Options.TrimSpaces, req.Options.MatchMethod == MatchCaseInsensitive)
			}
			if keyVal == targetNorm {
				pl.Matched++
				totalMatched++
				// keep original list row in per-list result
				pl.Result.Rows = append(pl.Result.Rows, append([]string(nil), row...))
				// store list row for combined output (we'll map to master header later)
				combinedRows = append(combinedRows, append([]string(nil), row...))
			}
		}

		perList = append(perList, pl)
	}

	// 3) Build combined TableData aligned to master header + source_list
	combinedHeader := append([]string(nil), req.Datasets.Master.Header...)
	combinedHeader = append(combinedHeader, "source_list")

	combinedMappedRows := make([][]string, 0, len(combinedRows))
	// helper: build a map from list header name (lower trimmed) -> index
	mapMasterHeader := map[string]int{}
	for i, h := range req.Datasets.Master.Header {
		mapMasterHeader[strings.ToLower(strings.TrimSpace(h))] = i
	}

	// First, add master matches mapped directly (source "master")
	for _, r := range masterMatches {
		mapped := make([]string, len(combinedHeader))
		// copy values for master header columns (they align)
		for i := 0; i < len(req.Datasets.Master.Header) && i < len(r); i++ {
			mapped[i] = r[i]
		}
		mapped[len(combinedHeader)-1] = "master"
		combinedMappedRows = append(combinedMappedRows, mapped)
	}

	// Then, add list matches: need to map list headers to master header indices
	for _, named := range req.Datasets.Lists {
		// build header map for this list
		listHeaderMap := map[string]int{}
		for i, h := range named.Table.Header {
			listHeaderMap[strings.ToLower(strings.TrimSpace(h))] = i
		}
		// for each matched row in perList for this named list, find those entries
		// find the perList entry for named.Name
		var rowsForList [][]string
		for _, p := range perList {
			if p.Name == named.Name {
				rowsForList = p.Result.Rows
				break
			}
		}
		for _, r := range rowsForList {
			mapped := make([]string, len(combinedHeader))
			// map each list column to master column if name matches (case-insensitive)
			for lname, lidx := range listHeaderMap {
				if midx, ok := mapMasterHeader[lname]; ok {
					if lidx < len(r) {
						mapped[midx] = r[lidx]
					}
				}
			}
			// set source_list
			mapped[len(combinedHeader)-1] = named.Name
			combinedMappedRows = append(combinedMappedRows, mapped)
		}
	}

	// 4) Fill summary and return
	res.PerList = perList
	res.Combined = types.TableData{
		HasHeader: true,
		Header:    combinedHeader,
		Rows:      combinedMappedRows,
	}
	res.Summary = map[string]int{
		"master_processed": len(req.Datasets.Master.Rows),
		"master_matched":   len(masterMatches),
		"lists_processed":  totalProcessed - len(req.Datasets.Master.Rows),
		"lists_matched":    totalMatched - len(masterMatches),
		"total_processed":  totalProcessed,
		"total_matched":    totalMatched,
		"duration_ms":      int(time.Since(start).Milliseconds()),
	}
	res.Error = nil
	return res, nil
}
