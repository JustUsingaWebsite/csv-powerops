package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/csvops"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
)

func main() {
	// sample dataset (headers similar to your screenshot)
	dataset := types.TableData{
		HasHeader: true,
		Header: []string{
			"Computer Name",
			"LoggedOnUsers",
			"Domain",
			"Operating System",
			"Offline Devices",
			"Sophos",
			"LastSuccessfulScan",
			"DeviceModel",
			"SID",
			"Asset Tag",
			"Does Not Exist In Sophos",
			"Active Computers",
		},
		Rows: [][]string{
			// a Windows 10 Pro machine that matches filters
			{
				"pc-win10-01",
				"jdoe",
				"MYDOMAIN",
				"Windows 10 Pro",
				"FALSE",
				"TRUE",
				"2025-01-02 08:11",
				"ModelX",
				"SID123",
				"AT1001",
				"FALSE",
				"TRUE",
			},
			// a Windows 7 machine that should NOT match
			{
				"isddlegacy",
				"",
				"-",
				"Windows 7 Professional",
				"TRUE",
				"TRUE",
				"11/03/2022 09:44",
				"-",
				"-",
				"-",
				"FALSE",
				"FALSE",
			},
		},
	}

	// build filter: Sophos is_true AND Active Computers is_true AND Operating System contains "Windows 10 Pro"
	filter := csvops.ConditionGroup{
		Op: "and",
		Conds: []csvops.Condition{
			{
				Column:   "Sophos",
				Operator: csvops.OpIsTrue,
			},
			{
				Column:   "Active Computers",
				Operator: csvops.OpIsTrue,
			},
			{
				Column:   "Operating System",
				Operator: csvops.OpContains,
				Value:    "Windows 10 Pro",
			},
		},
	}

	req := csvops.AdvancedExtractRequest{
		Operation: "advanced_extract",
		Options: csvops.AdvancedExtractOptions{
			TrimSpaces:      true,
			CaseInsensitive: true,
		},
		Dataset: dataset,
		Filter:  filter,
		// Pagination omitted; defaults will return all matches
	}

	resp, err := csvops.AdvancedExtract(req)
	if err != nil {
		log.Fatalf("AdvancedExtract failed: %v", err)
	}

	out, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Fatalf("json marshal failed: %v", err)
	}
	fmt.Println(string(out))
}
