package lake

import (
	"errors"
	"fmt"
	"io"

	"github.com/versity/versitygw/s3api/middlewares"
)

var errBodyTooLong = errors.New("request body longer than its declared length")

// guardLength wraps a PUT body so that a body shorter or longer than the
// declared (decoded) Content-Length fails the read instead of ending in a
// plain io.EOF. Without it, an unsigned-payload PUT whose client aborts is
// committed truncated: fasthttp reports the disconnect as io.EOF and the
// POSIX backend links what it got (rule 4).
//
// If the body is a middlewares.ChecksumReader (aws-chunked with a trailing
// checksum), the wrapper keeps that interface, because the POSIX backend
// type-asserts it to find the trailer checksum.
func guardLength(body io.Reader, contentLength *int64) io.Reader {
	if body == nil || contentLength == nil || *contentLength < 0 {
		return body
	}
	g := &lengthGuard{r: body, want: *contentLength}
	if cr, ok := body.(middlewares.ChecksumReader); ok {
		return &checksumLengthGuard{lengthGuard: g, cr: cr}
	}
	return g
}

type lengthGuard struct {
	r    io.Reader
	want int64
	got  int64
}

func (g *lengthGuard) Read(p []byte) (int, error) {
	n, err := g.r.Read(p)
	g.got += int64(n)
	if g.got > g.want {
		return n, fmt.Errorf("%w: declared %d bytes", errBodyTooLong, g.want)
	}
	if errors.Is(err, io.EOF) && g.got < g.want {
		return n, fmt.Errorf("short request body: got %d of %d bytes: %w", g.got, g.want, io.ErrUnexpectedEOF)
	}
	return n, err
}

// checksumLengthGuard is a lengthGuard that still exposes the original
// body's trailing-checksum interface.
type checksumLengthGuard struct {
	*lengthGuard
	cr middlewares.ChecksumReader
}

func (c *checksumLengthGuard) Algorithm() string { return c.cr.Algorithm() }
func (c *checksumLengthGuard) Checksum() string  { return c.cr.Checksum() }

var _ middlewares.ChecksumReader = (*checksumLengthGuard)(nil)
