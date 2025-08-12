package csvops

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CrossRefMode controls the output behaviour.
type CrossRefMode string

const (
	ModeMark    CrossRefMode = "mark"    // append a found column
	ModeExtract CrossRefMode = "extract" // output only matching rows
	ModeMissing CrossRefMode = "missing" // output only non-matching rows
)

// CrossRefOptions contains configuration for a cross-reference run.
type CrossRefOptions struct {
	MasterPath         string // path to master CSV
	ListPath           string // path to list CSV to check against master
	Key                string // column name (or numeric index as string) to use as key
	OutPath            string // path to write resulting CSV (required)
	MasterHasHeader    bool   // default true
	ListHasHeader      bool   // default true
	Delim              rune   // default ',' if 0
	Mode               CrossRefMode
	KeyCaseInsensitive bool   // if true, lowercase keys before comparing
	TrimSpaces         bool   // if true, trim spaces on keys
	SortOutput         bool   // if true, sort matches/missing before writing
	FoundColumnName    string // name of appended column when ModeMark; default "found" if empty
}

// ResultSummary is returned after a crossref operation.
type ResultSummary struct {
	Processed  int    // number of rows processed from list file
	Matched    int    // rows matching master
	Missing    int    // rows not found in master
	OutputPath string // path written
	DurationMS int64  // milliseconds taken
}

// CrossRef performs the list cross-referencing operation.
func CrossRef(opts CrossRefOptions) (ResultSummary, error) {
	var res ResultSummary
	start := time.Now()

	// basic validation & defaults
	if opts.MasterPath == "" || opts.ListPath == "" {
		return res, errors.New("master and list paths are required")
	}
	if opts.Key == "" {
		return res, errors.New("key is required (column name or numeric index as string)")
	}
	if opts.OutPath == "" {
		return res, errors.New("outPath is required (where the output CSV will be written)")
	}
	if opts.Delim == 0 {
		opts.Delim = ','
	}
	if opts.FoundColumnName == "" {
		opts.FoundColumnName = "found"
	}
	if opts.Mode == "" {
		opts.Mode = ModeMark
	}

	// -- Build master key set --
	masterSet, mKeyIdx, mHeader, err := buildKeySet(opts.MasterPath, opts.MasterHasHeader, opts.Key, opts.Delim, opts)
	if err != nil {
		return res, err
	}

	// -- Read list --
	lfile, err := os.Open(opts.ListPath)
	if err != nil {
		return res, fmt.Errorf("open list file: %w", err)
	}
	defer lfile.Close()

	lreader := csv.NewReader(bufio.NewReader(lfile))
	lreader.Comma = opts.Delim
	lreader.LazyQuotes = true
	lreader.FieldsPerRecord = -1

	var lHeader []string
	lKeyIdx := -1
	if opts.ListHasHeader {
		lHeader, err = lreader.Read()
		if err != nil {
			return res, fmt.Errorf("read list header: %w", err)
		}
		lKeyIdx = findColIndex(lHeader, opts.Key)
		if lKeyIdx == -1 {
			if idx, perr := strconv.Atoi(opts.Key); perr == nil {
				if idx < 0 || idx >= len(lHeader) {
					return res, fmt.Errorf("list key index %d out of range", idx)
				}
				lKeyIdx = idx
			} else {
				return res, fmt.Errorf("list key column '%s' not found in header", opts.Key)
			}
		}
	} else {
		idx, perr := strconv.Atoi(opts.Key)
		if perr != nil {
			return res, errors.New("list has no header; key must be numeric index like '0'")
		}
		lKeyIdx = idx
	}

	// -- Prepare output writer --
	outf, err := os.Create(opts.OutPath)
	if err != nil {
		return res, fmt.Errorf("create out file: %w", err)
	}
	defer outf.Close()
	writer := csv.NewWriter(outf)
	writer.Comma = opts.Delim
	defer writer.Flush()

	// Optional buffering for sorting
	var buffered [][]string

	// write header
	if opts.ListHasHeader {
		switch opts.Mode {
		case ModeMark:
			hdr := append([]string(nil), lHeader...)
			hdr = append(hdr, opts.FoundColumnName)
			if err := writer.Write(hdr); err != nil {
				return res, fmt.Errorf("write header: %w", err)
			}
		case ModeExtract, ModeMissing:
			if err := writer.Write(lHeader); err != nil {
				return res, fmt.Errorf("write header: %w", err)
			}
		}
	}

	// process list rows
	for {
		rec, err := lreader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return res, fmt.Errorf("reading list csv: %w", err)
		}
		res.Processed++

		keyVal := ""
		if lKeyIdx < len(rec) {
			keyVal = normalize(rec[lKeyIdx], opts)
		}
		_, present := masterSet[keyVal]
		if present {
			res.Matched++
		} else {
			res.Missing++
		}

		switch opts.Mode {
		case ModeMark:
			outRec := append([]string(nil), rec...)
			outRec = append(outRec, boolToStr(present))
			if err := writer.Write(outRec); err != nil {
				return res, fmt.Errorf("write record (mark): %w", err)
			}
		case ModeExtract:
			if present {
				if opts.SortOutput {
					buffered = append(buffered, rec)
				} else {
					if err := writer.Write(rec); err != nil {
						return res, fmt.Errorf("write record (extract): %w", err)
					}
				}
			}
		case ModeMissing:
			if !present {
				if opts.SortOutput {
					buffered = append(buffered, rec)
				} else {
					if err := writer.Write(rec); err != nil {
						return res, fmt.Errorf("write record (missing): %w", err)
					}
				}
			}
		}
	}

	// If sorting is enabled for extract/missing
	if opts.SortOutput && len(buffered) > 0 {
		sort.Slice(buffered, func(i, j int) bool {
			return strings.Join(buffered[i], ",") < strings.Join(buffered[j], ",")
		})
		for _, rec := range buffered {
			if err := writer.Write(rec); err != nil {
				return res, fmt.Errorf("write sorted record: %w", err)
			}
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return res, fmt.Errorf("csv writer error: %w", err)
	}

	res.OutputPath = opts.OutPath
	res.DurationMS = time.Since(start).Milliseconds()
	return res, nil
}

// ---------- helper functions ----------

// buildKeySet loads the master CSV and returns a lookup map.
func buildKeySet(path string, hasHeader bool, key string, delim rune, opts CrossRefOptions) (map[string]struct{}, int, []string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, -1, nil, fmt.Errorf("open master file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	reader.Comma = delim
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	var header []string
	keyIdx := -1
	if hasHeader {
		header, err = reader.Read()
		if err != nil {
			return nil, -1, nil, fmt.Errorf("read master header: %w", err)
		}
		keyIdx = findColIndex(header, key)
		if keyIdx == -1 {
			if idx, perr := strconv.Atoi(key); perr == nil {
				if idx < 0 || idx >= len(header) {
					return nil, -1, nil, fmt.Errorf("master key index %d out of range", idx)
				}
				keyIdx = idx
			} else {
				return nil, -1, nil, fmt.Errorf("master key column '%s' not found in header", key)
			}
		}
	} else {
		idx, perr := strconv.Atoi(key)
		if perr != nil {
			return nil, -1, nil, errors.New("master has no header; key must be numeric index like '0'")
		}
		keyIdx = idx
	}

	masterSet := make(map[string]struct{})
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, -1, nil, fmt.Errorf("reading master csv: %w", err)
		}
		if keyIdx >= len(rec) {
			continue
		}
		k := normalize(rec[keyIdx], opts)
		masterSet[k] = struct{}{}
	}

	return masterSet, keyIdx, header, nil
}

func findColIndex(header []string, key string) int {
	keyTrim := strings.TrimSpace(key)
	for i, c := range header {
		if strings.EqualFold(strings.TrimSpace(c), keyTrim) {
			return i
		}
	}
	return -1
}

// WhitespaceTrimmer removes leading/trailing/multiple spaces and normalizes whitespace.
func WhitespaceTrimmer(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Join(strings.Fields(s), " ") // collapse multiple spaces
	return s
}

func normalize(s string, opts CrossRefOptions) string {
	if opts.TrimSpaces {
		s = WhitespaceTrimmer(s)
	}
	if opts.KeyCaseInsensitive {
		s = strings.ToLower(s)
	}
	return s
}

func boolToStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
