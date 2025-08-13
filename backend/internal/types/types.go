package types

// Shared types used across csvops, extract, etc.

type TableData struct {
	HasHeader bool       `json:"hasHeader"`
	Header    []string   `json:"header"`
	Rows      [][]string `json:"rows"`
}

type ResultSummary struct {
	Processed  int   `json:"processed"`
	Matched    int   `json:"matched"`
	Missing    int   `json:"missing"`
	DurationMS int64 `json:"durationMs"`
}

// Minimal operation options shared by many operations.
// Operation-specific options can live in their own package.
type OpOptions struct {
	TrimSpaces         bool `json:"trim_spaces"`
	KeyCaseInsensitive bool `json:"key_case_insensitive"`
}
