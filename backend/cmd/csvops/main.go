package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/csvops"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
)

func printJSONLabel(label string, v interface{}) {
	fmt.Println("--------------------------------------------------")
	fmt.Println(">>>", label)
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Fatalf("json marshal failed: %v", err)
	}
	fmt.Println(string(out))
	fmt.Println()
}

func main() {
	// sample dataset to demonstrate trimming and case standardization
	master := types.TableData{
		HasHeader: true,
		Header:    []string{"Device", "User", "City"},
		Rows: [][]string{
			{" deviceA ", "  Alice  ", "new   york"},
			{"DeviceB", "bob", "Los Angeles"},
			{"deviceC  ", "Charlie  ", "SAN FRANCISCO"},
		},
	}

	// build DataClean request: Trim whitespace only on all columns
	reqTrim := csvops.DataCleanRequest{
		Operation: "data_clean",
		Options: csvops.DataCleanOptions{
			TrimSpaces:      true,
			CollapseInnerWS: true,
			CaseMode:        csvops.CaseUpper,
			Columns:         []string{}, // empty => all columns
			CaseInsensitive: true,
		},
		Datasets: types.MultiDatasets{
			Master: master,
		},
	}

	trimResp, err := csvops.DataClean(reqTrim)
	if err != nil {
		log.Fatalf("DataClean trim failed: %v", err)
	}
	printJSONLabel("TRIM (trim + collapse inner ws)", trimResp)

	// build DataClean request: Title Case only for "User" and "City"
	reqCase := csvops.DataCleanRequest{
		Operation: "data_clean",
		Options: csvops.DataCleanOptions{
			TrimSpaces:      true,
			CollapseInnerWS: true,
			CaseMode:        csvops.CaseTitle,
			Columns:         []string{"User", "City"},
			CaseInsensitive: true,
		},
		Datasets: types.MultiDatasets{
			Master: master,
		},
	}

	caseResp, err := csvops.DataClean(reqCase)
	if err != nil {
		log.Fatalf("DataClean case failed: %v", err)
	}
	printJSONLabel("CASE (Title case applied to User & City)", caseResp)

	fmt.Println("Done.")
}
