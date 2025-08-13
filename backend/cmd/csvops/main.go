package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/JustUsingaWebsite/csv-powerops/internal/csvops"
)

func main() {
	// Build dummy master dataset
	master := csvops.TableData{
		HasHeader: true,
		Header:    []string{"Manager Name", "Age", "Years Spent", "Gender"},
		Rows: [][]string{
			{"Alice Smith", "34", "5", "F"},
			{"Bob Jones", "45", "10", "M"},
			{"carol white", "29", "2", "F"},
		},
	}

	// Build dummy list dataset with slightly different headers
	list := csvops.TableData{
		HasHeader: true,
		Header:    []string{"Name", "Age", "years did", "gender"},
		Rows: [][]string{
			{"Alice Smith", "34", "5", "F"},
			{"David Green", "40", "7", "M"},
			{"bob jones", "45", "10", "M"}, // lowercase to test case-insensitive matching
		},
	}

	// Build request: match Name (master: "Manager Name", list: "Name"), case-insensitive, tagged action
	req := csvops.CrossRefRequest{
		Operation: "crossref",
		Options: csvops.CrossRefOptions{
			MatchMethod:     csvops.MatchCaseInsensitive,
			Action:          csvops.ActionTagged,
			MasterKey:       "Manager Name",
			ListKey:         "Name",
			TrimSpaces:      true,
			FoundColumnName: "tagged",
		},
		Datasets: csvops.CrossRefDatasets{
			Master: master,
			List:   list,
		},
	}

	// Call the in-memory crossref function
	resp, err := csvops.CrossRefJSON(req)
	if err != nil {
		log.Fatalf("CrossRefJSON failed: %v", err)
	}

	// Pretty-print the JSON response
	out, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Fatalf("json marshal failed: %v", err)
	}

	fmt.Println(string(out))
}
