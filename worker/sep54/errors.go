package sep54

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"syscall"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// ErrNotFound means the object does not exist in that store. It wraps
// os.ErrNotExist, like the SDK's datastore. For ledger reads it re-routes to
// the next source; it is never a validation failure.
var ErrNotFound = fmt.Errorf("object not found: %w", os.ErrNotExist)

// ErrNoLedgers means a store holds no ledger objects (what the SDK reports
// as datastore.ErrNoValidLedgerFiles).
var ErrNoLedgers = errors.New("no ledger objects in the store")

// ErrUnknownVersion means the object passed its checksums but its
// LedgerCloseMeta layout is unknown to this build. The archiver may upload
// such an object on checksum validation alone and alert.
var ErrUnknownVersion = errors.New("unknown LedgerCloseMeta version")

// InvalidError is a validation failure of a ledger object: a checksum,
// decode, sequence or hash mismatch. It never advances a cursor.
type InvalidError struct {
	Seq uint32
	Err error
}

func (e *InvalidError) Error() string { return fmt.Sprintf("ledger %d is invalid: %v", e.Seq, e.Err) }
func (e *InvalidError) Unwrap() error { return e.Err }

func invalid(seq uint32, format string, args ...any) error {
	return &InvalidError{Seq: seq, Err: fmt.Errorf(format, args...)}
}

// IsInvalid reports whether err is a validation failure.
func IsInvalid(err error) bool {
	var ie *InvalidError
	return errors.As(err, &ie)
}

// UnavailableError means a store or the lake's control endpoint could not be
// reached or failed transiently: connection refused or reset, a timeout, a
// truncated response, or a 5xx. It is not NotFound.
type UnavailableError struct {
	Op  string
	Err error
}

func (e *UnavailableError) Error() string { return fmt.Sprintf("%s: unavailable: %v", e.Op, e.Err) }
func (e *UnavailableError) Unwrap() error { return e.Err }

// IsUnavailable reports whether err means "the other side is down or
// flaky" as opposed to "the answer is no" (NotFound, 4xx, invalid data).
// A cancelled context is never unavailability.
func IsUnavailable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var ue *UnavailableError
	if errors.As(err, &ue) {
		return true
	}
	if status := httpStatus(err); status != 0 {
		return status >= 500 || status == http.StatusTooManyRequests
	}
	var rse *smithyhttp.RequestSendError
	if errors.As(err, &rse) {
		return true
	}
	return isTransportError(err)
}

// isTransportError classifies low-level network failures. An HTTP client
// timeout is a net.Error whose Timeout() is true.
func isTransportError(err error) bool {
	for _, target := range []error{syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ECONNABORTED,
		syscall.EPIPE, syscall.EHOSTUNREACH, syscall.ENETUNREACH, io.EOF, io.ErrUnexpectedEOF} {
		if errors.Is(err, target) {
			return true
		}
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// httpStatus returns the HTTP status of an S3 response error, or 0.
func httpStatus(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// errorCode returns the S3 error code (e.g. "NoSuchKey"), or "".
func errorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// isNotFound reports whether an S3 error means the key does not exist. A
// missing bucket is a configuration error, not a missing object.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	var nf *types.NotFound
	if errors.As(err, &nsk) || errors.As(err, &nf) {
		return true
	}
	if errorCode(err) == "NoSuchBucket" {
		return false
	}
	return httpStatus(err) == http.StatusNotFound
}

// IsPreconditionFailed reports whether err is S3's 412 for a conditional
// PUT (If-None-Match: *) whose key already exists.
func IsPreconditionFailed(err error) bool {
	return errorCode(err) == "PreconditionFailed" || httpStatus(err) == http.StatusPreconditionFailed
}

// IsAccessDenied reports whether err is S3's 403 AccessDenied.
func IsAccessDenied(err error) bool {
	return errorCode(err) == "AccessDenied" || httpStatus(err) == http.StatusForbidden
}
