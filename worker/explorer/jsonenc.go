package explorer

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"
)

// JSON output goes into jsonb columns, which reject "\u0000", and into
// text columns, which reject NUL bytes and invalid UTF-8. The appenders below
// produce valid JSON that Postgres always accepts.

const hexDigits = "0123456789abcdef"

// cleanText returns s as a Postgres-safe text value: NUL bytes removed and
// invalid UTF-8 replaced by U+FFFD (Horizon's approach for memos). Used only
// for display text such as memos; binary-looking strings inside JSON use
// appendXDRString, which is lossless.
func cleanText(s string) string {
	if strings.IndexByte(s, 0) < 0 && utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "�")
}

// textSafe reports whether s can be stored verbatim as Postgres text and
// JSON string: valid UTF-8 without NUL.
func textSafe(s string) bool {
	return strings.IndexByte(s, 0) < 0 && utf8.ValidString(s)
}

// appendJSONString appends s as a JSON string. s must be textSafe.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		dst = append(dst, s[start:i]...)
		switch c {
		case '"', '\\':
			dst = append(dst, '\\', c)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		}
		start = i + 1
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// appendXDRString appends an XDR string (arbitrary bytes) losslessly: a JSON
// string when it is valid UTF-8 without NUL, otherwise {"hex": "…"}.
func appendXDRString(dst []byte, s string) []byte {
	if textSafe(s) {
		return appendJSONString(dst, s)
	}
	dst = append(dst, `{"hex":"`...)
	dst = hex.AppendEncode(dst, []byte(s))
	return append(dst, `"}`...)
}

// appendHexString appends b as a quoted lower-case hex string.
func appendHexString(dst []byte, b []byte) []byte {
	dst = append(dst, '"')
	dst = hex.AppendEncode(dst, b)
	return append(dst, '"')
}

// appendKey appends `"key":`, preceded by a comma unless it is the first
// member of an object.
func appendKey(dst []byte, key string, first bool) []byte {
	if !first {
		dst = append(dst, ',')
	}
	dst = appendJSONString(dst, key)
	return append(dst, ':')
}

// appendQuotedInt appends a 64-bit integer as a JSON string: JSON numbers
// lose precision above 2^53 in most consumers.
func appendQuotedInt(dst []byte, v int64) []byte {
	dst = append(dst, '"')
	dst = strconv.AppendInt(dst, v, 10)
	return append(dst, '"')
}

func appendQuotedUint(dst []byte, v uint64) []byte {
	dst = append(dst, '"')
	dst = strconv.AppendUint(dst, v, 10)
	return append(dst, '"')
}

// snakeCase turns a Go identifier into snake_case: "ContractIdPreimage" →
// "contract_id_preimage", "HostFunctionTypeInvokeContract" →
// "host_function_type_invoke_contract".
func snakeCase(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		c := s[i]
		isUpper := c >= 'A' && c <= 'Z'
		if isUpper && i > 0 {
			prev := s[i-1]
			prevLowerOrDigit := (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9')
			nextLower := i+1 < len(s) && s[i+1] >= 'a' && s[i+1] <= 'z'
			prevUpper := prev >= 'A' && prev <= 'Z'
			if prevLowerOrDigit || (prevUpper && nextLower) {
				b.WriteByte('_')
			}
		}
		if isUpper {
			c += 'a' - 'A'
		}
		b.WriteByte(c)
	}
	return b.String()
}
