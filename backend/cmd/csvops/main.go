package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/csvops"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
)

func main() {
	master := types.TableData{
		HasHeader: true,
		Header:    []string{"Manager Name", "Age", "Years Spent", "Gender"},
		Rows: [][]string{
			{"Alice Smith", "34", "5", "F"},
			{"Bob Jones", "45", "10", "M"},
			{"Carol White", "29", "2", "F"},
		},
	}

	list := types.TableData{
		HasHeader: true,
		Header:    []string{"Name", "Age", "years did", "gender"},
		Rows: [][]string{
			{"Alice Smith", "34", "5", "F"},
			{"David Green", "40", "7", "M"},
			{"bob jones", "45", "10", "M"},
		},
	}

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

	resp, err := csvops.CrossRefJSON(req)
	if err != nil {
		log.Fatalf("crossref failed: %v", err)
	}

	out, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Fatalf("json marshal failed: %v", err)
	}
	fmt.Println(string(out))
}
