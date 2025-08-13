package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/csvops"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
)

func main() {
	// Master table (Manager dataset)
	master := types.TableData{
		HasHeader: true,
		Header:    []string{"Manager Name", "Age", "Years Spent", "Gender"},
		Rows: [][]string{
			{"Alice Smith", "34", "5", "F"},
			{"Bob Jones", "45", "10", "M"},
			{"Carol White", "29", "2", "F"},
		},
	}

	// One or more list tables to compare against master
	list1 := types.NamedTable{
		Name:    "list1",
		ListKey: "Name", // header name in this list to match master key
		Table: types.TableData{
			HasHeader: true,
			Header:    []string{"Name", "Age", "years did", "gender"},
			Rows: [][]string{
				{"Alice Smith", "33", "53", "M"},
				{"David Green", "403", "70", "F"},
				{"bob jones", "455", "100", "F"},
			},
		},
	}

	list2 := types.NamedTable{
		Name:    "list2",
		ListKey: "Name",
		Table: types.TableData{
			HasHeader: true,
			Header:    []string{"Name", "Age", "years did", "gender"},
			Rows: [][]string{
				{"Alice Smith", "34", "5", "F"},
				{"David Green", "40", "7", "M"},
				{"bob jones", "45", "10", "M"},
			},
		},
	}

	// Build the CrossRefMulti request (master + N lists)
	req := csvops.CrossRefMultiRequest{
		Operation: "crossref",
		Options: csvops.CrossRefMultiOptions{
			MatchMethod:    csvops.MatchCaseInsensitive, // case-insensitive match
			MasterKey:      "Manager Name",              // header in master to match on
			DefaultListKey: "",                          // not needed because lists provide ListKey
			TrimSpaces:     true,
		},
		Datasets: types.MultiDatasets{
			Master: master,
			Lists:  []types.NamedTable{list1, list2},
		},
	}

	// Call the function
	resp, err := csvops.CrossRefMulti(req)
	if err != nil {
		log.Fatalf("CrossRefMulti failed: %v", err)
	}

	// Pretty-print JSON response
	out, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Fatalf("json marshal failed: %v", err)
	}
	fmt.Println(string(out))
}
