package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/csvops"
	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
)

func main() {
	// sample dataset with Country variations
	data := types.TableData{
		HasHeader: true,
		Header:    []string{"ID", "Name", "Country"},
		Rows: [][]string{
			{"1", "Alice", "USA"},
			{"2", "Bob", "U.S."},
			{"3", "Carol", "United States of America"},
			{"4", "Dave", "United states of america"},
			{"5", "Eve", "Canada"},
			{"6", "Faythe", "U.S.A"},
			{"7", "Grace", "u.s."},
		},
	}

	// build rules: map multiple variants to "USA"
	rule := csvops.ReplaceRule{
		Targets:     []string{"USA", "U.S.", "U.S.A", "United States of America", "United states of america", "u.s."},
		Replacement: "USA",
		// use nil CaseInsensitive to inherit global; set WholeCell true to only match whole cell (we want that)
		WholeCell: func(b bool) *bool { return &b }(true),
	}

	req := csvops.FindReplaceRequest{
		Operation: "find_replace",
		Options: csvops.FindReplaceOptions{
			TrimSpaces:      true,
			CaseInsensitive: true, // default for rules
			Columns:         []string{"Country"},
		},
		Dataset: data,
		Rules:   []csvops.ReplaceRule{rule},
	}

	resp, err := csvops.FindAndReplace(req)
	if err != nil {
		log.Fatalf("FindAndReplace failed: %v", err)
	}

	out, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Fatalf("json marshal failed: %v", err)
	}
	fmt.Println(string(out))
}
