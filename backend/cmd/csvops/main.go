package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/csvops"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
)

func main() {
	// Master dataset
	master := types.TableData{
		HasHeader: true,
		Header:    []string{"DeviceName", "LoggedOnUsers", "LastLoggedOn"},
		Rows: [][]string{
			{"device1", "jake,paul", "2024-11-11"},
			{"device2", "alice", "2024-10-10"},
		},
	}

	// List A
	listA := types.NamedTable{
		Name:    "listA",
		ListKey: "DeviceName",
		Table: types.TableData{
			HasHeader: true,
			Header:    []string{"Device", "User", "Note"},
			Rows: [][]string{
				{"device1", "sue", "hr"},
				{"device1", "paul", "dup-case"},
				{"device3", "tom", "other"},
			},
		},
	}

	// List B
	listB := types.NamedTable{
		Name:    "listB",
		ListKey: "DeviceName",
		Table: types.TableData{
			HasHeader: true,
			Header:    []string{"DeviceName", "User"},
			Rows: [][]string{
				{"device1", "dave"},
				{"device2", "bob"},
			},
		},
	}

	// Build request for OneToMany (search for device1)
	req := csvops.OneToManyRequest{
		Operation: "one_to_many",
		Options: csvops.OneToManyOptions{
			MatchMethod: csvops.MatchCaseInsensitive,
			TrimSpaces:  true,
		},
		Target: csvops.OneToManyTarget{
			Key:   "DeviceName",
			Value: "device1",
		},
		Datasets: types.MultiDatasets{
			Master: master,
			Lists:  []types.NamedTable{listA, listB},
		},
	}

	// Call OneToMany
	resp, err := csvops.OneToMany(req)
	if err != nil {
		log.Fatalf("OneToMany failed: %v", err)
	}

	// Pretty-print the JSON response
	out, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Fatalf("json marshal failed: %v", err)
	}
	fmt.Println(string(out))
}
