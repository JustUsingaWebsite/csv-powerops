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
	// -----------------------
	// 1) Alphabetical example
	// -----------------------
	alphaMaster := types.TableData{
		HasHeader: true,
		Header:    []string{"Name", "Age", "Dept"},
		Rows: [][]string{
			{"Bob", "45", "IT"},
			{"alice", "34", "HR"},
			{"Carol", "29", "Sales"},
			{"zoe", "27", "Ops"},
		},
	}

	alphaReq := csvops.AdvancedSortRequest{
		Operation: "advanced_sort",
		Options: csvops.AdvancedSortOptions{
			Mode:            csvops.SortAlpha,
			Order:           csvops.OrderAsc,
			Key:             "Name",
			TrimSpaces:      true,
			CaseInsensitive: true,
		},
		Datasets: types.MultiDatasets{
			Master: alphaMaster,
		},
	}

	alphaResp, err := csvops.AdvancedSort(alphaReq)
	if err != nil {
		log.Fatalf("alphabetical sort failed: %v", err)
	}
	printJSONLabel("ALPHABETICAL SORT (Name A→Z, case-insensitive)", alphaResp)

	// -----------------------
	// 2) Numeric example
	// -----------------------
	numMaster := types.TableData{
		HasHeader: true,
		Header:    []string{"Device", "DiskGB"},
		Rows: [][]string{
			{"A", "120"},
			{"B", "240"},
			{"C", "60"},
			{"D", "n/a"},
			{"E", "1,024"},
		},
	}

	numReq := csvops.AdvancedSortRequest{
		Operation: "advanced_sort",
		Options: csvops.AdvancedSortOptions{
			Mode:       csvops.SortNumeric,
			Order:      csvops.OrderAsc, // smallest -> largest
			Key:        "DiskGB",
			TrimSpaces: true,
			// CaseInsensitive is harmless here
		},
		Datasets: types.MultiDatasets{
			Master: numMaster,
		},
	}

	numResp, err := csvops.AdvancedSort(numReq)
	if err != nil {
		log.Fatalf("numeric sort failed: %v", err)
	}
	printJSONLabel("NUMERIC SORT (DiskGB asc, non-parseable values go to end)", numResp)

	// -----------------------
	// 3) Date/time example
	// -----------------------
	dateMaster := types.TableData{
		HasHeader: true,
		Header:    []string{"Device", "LastScan"},
		Rows: [][]string{
			{"d1", "2025-01-02 08:11:00"},
			{"d2", "01/02/2025 09:00"},     // MM/DD/YYYY
			{"d3", "02 Jan 2025 10:00"},    // dd Mon YYYY
			{"d4", "n/a"},                  // invalid
			{"d5", "2024-12-31T23:59:59Z"}, // RFC3339
		},
	}

	dateReq := csvops.AdvancedSortRequest{
		Operation: "advanced_sort",
		Options: csvops.AdvancedSortOptions{
			Mode:       csvops.SortDate,
			Order:      csvops.OrderAsc, // chronological
			Key:        "LastScan",
			TrimSpaces: true,
			// DateFormat: "" // leave empty to let parser try many formats
		},
		Datasets: types.MultiDatasets{
			Master: dateMaster,
		},
	}

	dateResp, err := csvops.AdvancedSort(dateReq)
	if err != nil {
		log.Fatalf("date sort failed: %v", err)
	}
	printJSONLabel("DATE SORT (LastScan chronological asc; unparsable values go to end)", dateResp)

	fmt.Println("Done.")
}
