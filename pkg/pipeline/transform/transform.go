package transform

import (
	"encoding/json"
	"sort"
	"strings"
)

// NormalizeBytesToString returns a shallow-copied map where []byte values are converted to string.
func NormalizeBytesToString(in map[string]interface{}) map[string]interface{} {
	if in == nil {
		return nil
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		switch t := v.(type) {
		case []byte:
			out[k] = string(t)
		default:
			out[k] = v
		}
	}
	return out
}

// JSONDecodeFields parses selected fields that contain JSON strings (object/array) into structured values.
// This is useful when SQL returns JSON_OBJECT/JSON_ARRAYAGG, which often arrives as []byte/string.
// If parsing fails or the field is not a string, it is left unchanged.
func JSONDecodeFields(doc map[string]interface{}, fields []string) {
	if doc == nil || len(fields) == 0 {
		return
	}
	for _, field := range fields {
		f := strings.TrimSpace(field)
		if f == "" {
			continue
		}
		v, ok := doc[f]
		if !ok {
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		s = strings.TrimSpace(s)
		if s == "" || s == "null" {
			continue
		}
		// Only attempt JSON for likely candidates.
		if !(strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[")) {
			continue
		}
		var out interface{}
		if err := json.Unmarshal([]byte(s), &out); err != nil {
			continue
		}
		doc[f] = out
	}
}

// SplitStringField splits a string field into []string by sep, with optional trimming and empty removal.
// If the field does not exist or is not a string, it does nothing.
func SplitStringField(doc map[string]interface{}, field, sep string, trim bool) {
	if doc == nil || field == "" {
		return
	}
	v, ok := doc[field]
	if !ok {
		return
	}
	s, ok := v.(string)
	if !ok {
		return
	}
	if sep == "" {
		sep = ";"
	}
	parts := make([]string, 0, 8)
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || (sep != "" && i+len(sep) <= len(s) && s[i:i+len(sep)] == sep) {
			seg := s[start:i]
			if trim {
				seg = trimSpace(seg)
			}
			if seg != "" {
				parts = append(parts, seg)
			}
			if i < len(s) {
				i += len(sep) - 1
				start = i + 1
			}
		}
	}
	doc[field] = parts
}

// PreviewTopN returns a new map containing at most topN keys (sorted by key name) from doc.
// For []string values, only the first 3 elements are included under key or key+".sample" when truncated.
func PreviewTopN(doc map[string]interface{}, topN int) map[string]interface{} {
	if doc == nil || topN <= 0 {
		return map[string]interface{}{}
	}
	keys := make([]string, 0, len(doc))
	for k := range doc {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if topN > len(keys) {
		topN = len(keys)
	}
	out := make(map[string]interface{}, topN)
	for i := 0; i < topN; i++ {
		k := keys[i]
		v := doc[k]
		switch arr := v.(type) {
		case []string:
			if len(arr) > 3 {
				out[k+".sample"] = arr[:3]
			} else {
				out[k] = arr
			}
		default:
			out[k] = v
		}
	}
	return out
}

// trimSpace is a tiny helper to avoid importing strings in this small package
func trimSpace(s string) string {
	// simple ASCII trim to avoid extra deps; the caller already passes typical ASCII delimiters
	start := 0
	for start < len(s) {
		c := s[start]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			start++
		} else {
			break
		}
	}
	end := len(s)
	for end > start {
		c := s[end-1]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			end--
		} else {
			break
		}
	}
	return s[start:end]
}
