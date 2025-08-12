package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"unicode/utf8"

	"github.com/JustUsingaWebsite/csv-powerops/internal/csvops"
)

func main() {
	master := flag.String("master", "", "path to master CSV (required)")
	list := flag.String("list", "", "path to list CSV (required)")
	key := flag.String("key", "", "key column name (or numeric index as string) (required)")
	out := flag.String("out", "", "output CSV path (required)")
	mode := flag.String("mode", "mark", "mode: mark|extract|missing")
	delim := flag.String("delim", ",", "delimiter (single rune, default ',')")
	nocase := flag.Bool("nocase", false, "case-insensitive key matching (default false)")
	notrim := flag.Bool("notrim", false, "do not trim spaces from keys (default trim)")
	foundcol := flag.String("foundcol", "found", "found column name when mode=mark")

	flag.Parse()

	if *master == "" || *list == "" || *key == "" || *out == "" {
		flag.Usage()
		os.Exit(2)
	}

	// delimiter parsing
	r := ','
	if *delim != "" {
		runeVal, _ := utf8.DecodeRuneInString(*delim)
		r = runeVal
	}

	var m csvops.CrossRefMode = csvops.ModeMark
	switch *mode {
	case "mark":
		m = csvops.ModeMark
	case "extract":
		m = csvops.ModeExtract
	case "missing":
		m = csvops.ModeMissing
	default:
		log.Fatalf("unsupported mode: %s", *mode)
	}

	opts := csvops.CrossRefOptions{
		MasterPath:         *master,
		ListPath:           *list,
		Key:                *key,
		OutPath:            *out,
		MasterHasHeader:    true,
		ListHasHeader:      true,
		Delim:              r,
		Mode:               m,
		KeyCaseInsensitive: *nocase,
		TrimSpaces:         !(*notrim),
		FoundColumnName:    *foundcol,
	}

	fmt.Println("Running crossref with opts:", opts)
	res, err := csvops.CrossRef(opts)
	if err != nil {
		log.Fatalf("crossref failed: %v", err)
	}
	fmt.Printf("Done. Processed=%d, Matched=%d, Missing=%d, output=%s, duration_ms=%d\n",
		res.Processed, res.Matched, res.Missing, res.OutputPath, res.DurationMS)
}
