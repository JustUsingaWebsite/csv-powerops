package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
)

type TableData struct {
	HasHeader bool       `json:"hasHeader"`
	Header    []string   `json:"header"`
	Rows      [][]string `json:"rows"`
}

func csvToJSON(csvPath, jsonPath string) error {
	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV: %w", err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("CSV is empty")
	}

	table := struct {
		HasHeader bool       `json:"hasHeader"`
		Header    []string   `json:"header"`
		Rows      [][]string `json:"rows"`
	}{
		HasHeader: true,
		Header:    rows[0],
		Rows:      rows[1:],
	}

	out, err := os.Create(jsonPath)
	if err != nil {
		return fmt.Errorf("failed to create JSON: %w", err)
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(table)
}

func main() {
	csvPath := flag.String("csv", "", "CSV file to convert")
	flag.Parse()

	if *csvPath == "" {
		log.Fatal("Please provide a CSV file using --csv <filename>")
	}

	jsonPath := (*csvPath)[:len(*csvPath)-len(".csv")] + ".json"

	if err := csvToJSON(*csvPath, jsonPath); err != nil {
		log.Fatalf("Error converting %s: %v", *csvPath, err)
	}
	fmt.Printf("Converted %s to %s\n", *csvPath, jsonPath)
}
