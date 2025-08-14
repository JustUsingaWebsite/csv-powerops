package utils

import (
	"errors"
	"strconv"
	"strings"
	"unicode"

	"github.com/JustUsingaWebsite/csv-powerops/backend/internal/types"
)

// WhitespaceTrimmer removes leading/trailing whitespace and collapses internal whitespace.
func WhitespaceTrimmer(s string) string {
	// Trim + collapse multiple spaces
	s = strings.TrimSpace(s)
	// strings.Fields will collapse all whitespace runs into single spaces
	parts := strings.Fields(s)
	return strings.Join(parts, " ")
}

// ResolveKeyIndex returns the column index for a key which can be a header name or numeric index string.
// If the table has no header, key must be numeric.
func ResolveKeyIndex(tbl types.TableData, key string) (int, error) {
	if tbl.HasHeader {
		keyTrim := strings.TrimSpace(key)
		for i, h := range tbl.Header {
			if strings.EqualFold(strings.TrimSpace(h), keyTrim) {
				return i, nil
			}
		}
		// fallback: maybe key is numeric string
		if idx, err := strconv.Atoi(key); err == nil {
			if idx < 0 || idx >= len(tbl.Header) {
				return -1, errors.New("numeric key index out of range")
			}
			return idx, nil
		}
		return -1, errors.New("key not found in header")
	}
	// no header - key must be numeric
	idx, err := strconv.Atoi(key)
	if err != nil {
		return -1, errors.New("no header: key must be numeric index string")
	}
	return idx, nil
}

// Normalize applies trimming and case normalization according to flags.
func Normalize(val string, trim bool, caseInsensitive bool) string {
	if trim {
		val = WhitespaceTrimmer(val)
	}
	if caseInsensitive {
		val = strings.ToLower(val)
	}
	return val
}

// ParseIndexString attempts to parse s as a non-negative integer index.
// Returns (index, true) if s is a valid integer string like "0", "1", " 2 ".
// Returns (0, false) otherwise (empty string, negative, contains non-digit chars).
func ParseIndexString(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}

	// Ensure every rune is a digit (reject signs, decimals, hex, etc.)
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return 0, false
		}
	}

	// Parse integer (safe because we've checked digits only)
	i, err := strconv.Atoi(s)
	if err != nil || i < 0 {
		return 0, false
	}
	return i, true
}
