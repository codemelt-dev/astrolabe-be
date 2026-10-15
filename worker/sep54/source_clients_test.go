package sep54

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"stellar-explorer/worker/config"
)

// captureClient answers every request with an empty listing and keeps the
// request, so we can see how each explicit client signs and addresses.
type captureClient struct {
	mu   sync.Mutex
	reqs []*http.Request
}

func (c *captureClient) Do(r *http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.reqs = append(c.reqs, r.Clone(context.Background()))
	c.mu.Unlock()
	body := `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`
	return &http.Response{StatusCode: 200, Status: "200 OK", Header: http.Header{"Content-Type": {"application/xml"}},
		Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: r}, nil
}

func (c *captureClient) last(t *testing.T) *http.Request {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reqs) == 0 {
		t.Fatal("no request sent")
	}
	return c.reqs[len(c.reqs)-1]
}

func list(t *testing.T, c *s3.Client, bucket string) *http.Request {
	t.Helper()
	cc := &captureClient{}
	_, err := c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket), MaxKeys: aws.Int32(1)},
		func(o *s3.Options) { o.HTTPClient = cc })
	if err != nil {
		t.Fatal(err)
	}
	return cc.last(t)
}

// Every source uses exactly its own endpoint, region, keys and addressing,
// whatever AWS_* variables, profiles or config files the process has
// (never config.LoadDefaultConfig).
func TestClientsIgnoreAWSEnvironment(t *testing.T) {
	for k, v := range map[string]string{
		"AWS_ACCESS_KEY_ID":           "ENVKEY",
		"AWS_SECRET_ACCESS_KEY":       "env-secret",
		"AWS_SESSION_TOKEN":           "env-token",
		"AWS_REGION":                  "eu-west-9",
		"AWS_DEFAULT_REGION":          "eu-west-9",
		"AWS_ENDPOINT_URL":            "http://env.invalid",
		"AWS_ENDPOINT_URL_S3":         "http://env-s3.invalid",
		"AWS_PROFILE":                 "nonexistent",
		"AWS_CONFIG_FILE":             "/nonexistent/config",
		"AWS_SHARED_CREDENTIALS_FILE": "/nonexistent/credentials",
		"AWS_S3_USE_ARN_REGION":       "true",
		"AWS_RETRY_MODE":              "adaptive",
	} {
		t.Setenv(k, v)
	}

	lake := config.Default().Lake
	lake.S3Listen = "127.0.0.1:9000"
	req := list(t, LakeClient(lake, config.GatewayCredentials{AccessKey: "LAKEKEY", SecretKey: "lake-secret"}), "stellar")
	if req.URL.Host != "127.0.0.1:9000" || req.URL.Scheme != "http" || !strings.HasPrefix(req.URL.Path, "/stellar") {
		t.Fatalf("lake request went to %s, want path-style http://127.0.0.1:9000/stellar", req.URL)
	}
	if a := req.Header.Get("Authorization"); !strings.Contains(a, "Credential=LAKEKEY/") || !strings.Contains(a, "/us-east-1/s3/") {
		t.Fatalf("lake Authorization %q", a)
	}
	if req.Header.Get("X-Amz-Security-Token") != "" {
		t.Fatal("the lake request carries the environment's session token")
	}

	arch := config.ArchiveCredentials{Endpoint: "https://hel1.your-objectstorage.com", Region: "hel1", AccessKey: "ARCHKEY", SecretKey: "arch-secret"}
	archive := ArchiveClient(config.Archive{Bucket: "stellar-ledgers-pubnet"}, arch)
	req = list(t, archive, "stellar-ledgers-pubnet")
	if req.URL.Host != "stellar-ledgers-pubnet.hel1.your-objectstorage.com" {
		t.Fatalf("archive request went to %s, want virtual-hosted", req.URL)
	}
	if a := req.Header.Get("Authorization"); !strings.Contains(a, "Credential=ARCHKEY/") || !strings.Contains(a, "/hel1/s3/") {
		t.Fatalf("archive Authorization %q", a)
	}
	if got := archive.Options().RetryMaxAttempts; got != 10 {
		t.Fatalf("archive RetryMaxAttempts %d, want 10", got)
	}
	if !archive.Options().DisableLogOutputChecksumValidationSkipped {
		t.Fatal("archive client logs skipped checksum validation")
	}
	if o := archive.Options(); o.RequestChecksumCalculation != aws.RequestChecksumCalculationWhenSupported ||
		o.ResponseChecksumValidation != aws.ResponseChecksumValidationWhenSupported {
		t.Fatalf("archive client checksum defaults %v/%v, want the SDK's WhenSupported", o.RequestChecksumCalculation, o.ResponseChecksumValidation)
	}
	req = list(t, ArchiveClient(config.Archive{Bucket: "stellar-ledgers-pubnet", PathStyle: true}, arch), "stellar-ledgers-pubnet")
	if req.URL.Host != "hel1.your-objectstorage.com" || !strings.HasPrefix(req.URL.Path, "/stellar-ledgers-pubnet") {
		t.Fatalf("path-style archive request went to %s", req.URL)
	}

	awsLake := AWSLakeClient()
	req = list(t, awsLake, "aws-public-blockchain")
	if req.URL.Host != "aws-public-blockchain.s3.us-east-2.amazonaws.com" || req.URL.Scheme != "https" {
		t.Fatalf("AWS lake request went to %s", req.URL)
	}
	if a := req.Header.Get("Authorization"); a != "" {
		t.Fatalf("AWS lake requests must be anonymous, got Authorization %q", a)
	}
	// s3.New turns AnonymousCredentials into "no credentials": unsigned.
	if c := awsLake.Options().Credentials; c != nil {
		if _, ok := c.(aws.AnonymousCredentials); !ok {
			t.Fatalf("AWS lake credentials %T", c)
		}
	}
}

func responseErr(status int, apiErr error) error {
	return &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      apiErr,
	}}
}

func TestErrorClassification(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	cases := []struct {
		name                                            string
		err                                             error
		notFound, unavailable, precondition, accessDeny bool
	}{
		{"NoSuchKey", responseErr(404, &types.NoSuchKey{}), true, false, false, false},
		{"HEAD 404", responseErr(404, &types.NotFound{}), true, false, false, false},
		{"NoSuchBucket", responseErr(404, &smithy.GenericAPIError{Code: "NoSuchBucket"}), false, false, false, false},
		{"503 SlowDown", responseErr(503, &smithy.GenericAPIError{Code: "SlowDown"}), false, true, false, false},
		{"500", responseErr(500, &smithy.GenericAPIError{Code: "InternalError"}), false, true, false, false},
		{"429", responseErr(429, &smithy.GenericAPIError{Code: "TooManyRequests"}), false, true, false, false},
		{"403", responseErr(403, &smithy.GenericAPIError{Code: "AccessDenied"}), false, false, false, true},
		{"412", responseErr(412, &smithy.GenericAPIError{Code: "PreconditionFailed"}), false, false, true, false},
		{"send error", &smithyhttp.RequestSendError{Err: refused}, false, true, false, false},
		{"connection refused", fmt.Errorf("get: %w", refused), false, true, false, false},
		{"reset", fmt.Errorf("read: %w", syscall.ECONNRESET), false, true, false, false},
		{"truncated body", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), false, true, false, false},
		{"cancelled", fmt.Errorf("get: %w", context.Canceled), false, false, false, false},
		{"invalid ledger", invalid(7, "bad"), false, false, false, false},
	}
	for _, c := range cases {
		if got := isNotFound(c.err); got != c.notFound {
			t.Errorf("%s: isNotFound = %v", c.name, got)
		}
		if got := IsUnavailable(c.err); got != c.unavailable {
			t.Errorf("%s: IsUnavailable = %v", c.name, got)
		}
		if got := IsPreconditionFailed(c.err); got != c.precondition {
			t.Errorf("%s: IsPreconditionFailed = %v", c.name, got)
		}
		if got := IsAccessDenied(c.err); got != c.accessDeny {
			t.Errorf("%s: IsAccessDenied = %v", c.name, got)
		}
	}
}
