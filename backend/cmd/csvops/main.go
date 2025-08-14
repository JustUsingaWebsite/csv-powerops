package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/csvops"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
)

func main() {
	// Sample dataset
	data := types.TableData{
		HasHeader: true,
		Header:    []string{"Device", "User", "Location"},
		Rows: [][]string{
			{"DeviceA", "Alice", "HQ"},
			{"DeviceB", "Bob", "Remote"},
			{"DeviceC", "Alice", "Branch"},
			{"DeviceD", "Charlie", "HQ"},
			{"DeviceE", "Alice", "Remote"},
		},
	}

	// Request: Get all devices for Alice
	req := csvops.ManyToOneRequest{
		Operation: "many_to_one",
		Options: csvops.ManyToOneOptions{
			MatchMethod: csvops.MatchCaseInsensitive,
			TrimSpaces:  true,
		},
		Target: csvops.ManyToOneTarget{
			OneKey:  "User",
			ManyKey: "Device",
			Value:   "Alice",
		},
		Dataset: data,
	}

	resp, err := csvops.ManyToOne(req)
	if err != nil {
		log.Fatalf("many_to_one failed: %v", err)
	}

	out, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Fatalf("json marshal failed: %v", err)
	}
	fmt.Println(string(out))
}
