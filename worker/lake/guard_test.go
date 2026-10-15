package lake

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/versity/versitygw/s3api/middlewares"
)

type fakeChecksumReader struct{ io.Reader }

func (fakeChecksumReader) Algorithm() string { return "crc32c" }
func (fakeChecksumReader) Checksum() string  { return "AAAAAA==" }

func i64(v int64) *int64 { return &v }

func TestGuardLength(t *testing.T) {
	body := strings.Repeat("x", 1000)
	cases := []struct {
		name     string
		declared int64
		wantErr  error
	}{
		{"exact", 1000, nil},
		{"short body (client aborted)", 2000, io.ErrUnexpectedEOF},
		{"long body", 10, errBodyTooLong},
		{"empty", 0, errBodyTooLong},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := guardLength(strings.NewReader(body), i64(c.declared))
			got, err := io.ReadAll(r)
			if c.wantErr == nil {
				if err != nil || string(got) != body {
					t.Fatalf("read %d bytes, err %v", len(got), err)
				}
				return
			}
			if !errors.Is(err, c.wantErr) {
				t.Fatalf("err = %v, want %v", err, c.wantErr)
			}
		})
	}
	if r := guardLength(strings.NewReader(""), i64(0)); mustReadAll(t, r) != "" {
		t.Fatal("empty body with length 0 must read cleanly")
	}
}

func mustReadAll(t *testing.T, r io.Reader) string {
	t.Helper()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestGuardLengthKeepsChecksumReader(t *testing.T) {
	src := fakeChecksumReader{bytes.NewReader([]byte("abc"))}
	r := guardLength(src, i64(3))
	cr, ok := r.(middlewares.ChecksumReader)
	if !ok {
		t.Fatalf("%T lost the middlewares.ChecksumReader interface", r)
	}
	if cr.Algorithm() != "crc32c" || cr.Checksum() != "AAAAAA==" {
		t.Fatalf("checksum not delegated: %q %q", cr.Algorithm(), cr.Checksum())
	}
	if mustReadAll(t, cr) != "abc" {
		t.Fatal("wrong body")
	}
	// A short trailer-checksum body is still caught.
	short := guardLength(fakeChecksumReader{bytes.NewReader([]byte("ab"))}, i64(3))
	if _, err := io.ReadAll(short); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v", err)
	}
	// A plain body stays a plain reader.
	if _, ok := guardLength(strings.NewReader("abc"), i64(3)).(middlewares.ChecksumReader); ok {
		t.Fatal("plain body gained the ChecksumReader interface")
	}
}

func TestGuardLengthPassThrough(t *testing.T) {
	src := strings.NewReader("abc")
	if guardLength(src, nil) != io.Reader(src) {
		t.Fatal("nil content length must not wrap")
	}
	if guardLength(src, i64(-1)) != io.Reader(src) {
		t.Fatal("negative content length must not wrap")
	}
	if guardLength(nil, i64(3)) != nil {
		t.Fatal("nil body must stay nil")
	}
}
