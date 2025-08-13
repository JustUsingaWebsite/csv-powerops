package utils

import (
	"errors"
	"strconv"
	"strings"

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
