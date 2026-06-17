package storage

// s3.go is the ONLY file in the engine that imports the AWS SDK. It adapts an
// S3-compatible object store (MinIO in Docker, real S3 in principle) to the
// ObjectStore contract: PutCAS → If-Match, PutIfAbsent → If-None-Match:"*",
// Put → unconditional, with HTTP 412 mapped to ErrPreconditionFailed and 404 to
// ErrNotFound. Keeping the SDK confined here means the rest of tpuf depends only
// on the small ObjectStore interface and can be tested entirely over MemStore.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Store is an ObjectStore backed by an S3-compatible service. It is bucket
// scoped: every key passed to its methods is an object key within bucket.
type S3Store struct {
	client *s3.Client
	bucket string
}

// compile-time assertion that S3Store satisfies the engine's storage contract.
var _ ObjectStore = (*S3Store)(nil)

// S3Config is the connection configuration for an S3Store. Endpoint, bucket, and
// credentials are required; Region is a placeholder for MinIO (the SDK requires
// a non-empty value but path-style MinIO ignores it).
type S3Config struct {
	Endpoint  string // e.g. "http://localhost:9000"
	Bucket    string // e.g. "tpuf"
	AccessKey string
	SecretKey string
	Region    string // placeholder; defaults to "us-east-1" if empty
}

// NewS3Store builds an S3Store from explicit configuration. It uses static
// credentials and a placeholder region, points the client at cfg.Endpoint, and
// forces path-style addressing — both required for MinIO, which does not do
// virtual-host bucket subdomains.
func NewS3Store(cfg S3Config) (*S3Store, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("s3: endpoint is required")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	awsCfg := aws.Config{
		Region: region,
		Credentials: credentials.NewStaticCredentialsProvider(
			cfg.AccessKey, cfg.SecretKey, "",
		),
	}

	endpoint := cfg.Endpoint
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})

	return &S3Store{client: client, bucket: cfg.Bucket}, nil
}

// NewS3StoreFromEnv builds an S3Store from the tpuf environment variables:
//
//	TPUF_S3_ENDPOINT, TPUF_BUCKET, TPUF_S3_ACCESS_KEY,
//	TPUF_S3_SECRET_KEY, TPUF_S3_REGION (optional).
//
// Endpoint, bucket, and the two keys are required; a missing one is an error.
func NewS3StoreFromEnv() (*S3Store, error) {
	cfg := S3Config{
		Endpoint:  os.Getenv("TPUF_S3_ENDPOINT"),
		Bucket:    os.Getenv("TPUF_BUCKET"),
		AccessKey: os.Getenv("TPUF_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("TPUF_S3_SECRET_KEY"),
		Region:    os.Getenv("TPUF_S3_REGION"),
	}

	var missing []string
	if cfg.Endpoint == "" {
		missing = append(missing, "TPUF_S3_ENDPOINT")
	}
	if cfg.Bucket == "" {
		missing = append(missing, "TPUF_BUCKET")
	}
	if cfg.AccessKey == "" {
		missing = append(missing, "TPUF_S3_ACCESS_KEY")
	}
	if cfg.SecretKey == "" {
		missing = append(missing, "TPUF_S3_SECRET_KEY")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("s3: missing required env: %s", strings.Join(missing, ", "))
	}

	return NewS3Store(cfg)
}

// Get implements ObjectStore. It returns ErrNotFound when the key is absent.
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, string, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, "", fmt.Errorf("get %q: %w", key, classify(err))
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", fmt.Errorf("get %q: reading body: %w", key, err)
	}
	return body, aws.ToString(out.ETag), nil
}

// PutCAS implements ObjectStore using an S3 If-Match conditional write. A 412
// (the ETag no longer matches, or the object was deleted) becomes
// ErrPreconditionFailed, which drives the manifest CAS retry loop.
func (s *S3Store) PutCAS(ctx context.Context, key string, body []byte, ifMatchETag string) (string, error) {
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  &s.bucket,
		Key:     &key,
		Body:    bytes.NewReader(body),
		IfMatch: &ifMatchETag,
	})
	if err != nil {
		return "", fmt.Errorf("put-cas %q: %w", key, classify(err))
	}
	return aws.ToString(out.ETag), nil
}

// PutIfAbsent implements ObjectStore using an S3 If-None-Match: "*" write-once
// guard. A 412 (the key already exists) becomes ErrPreconditionFailed — this is
// the WAL segment race: the loser reloads and rewrites at the next seq.
func (s *S3Store) PutIfAbsent(ctx context.Context, key string, body []byte) (string, error) {
	star := "*"
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s.bucket,
		Key:         &key,
		Body:        bytes.NewReader(body),
		IfNoneMatch: &star,
	})
	if err != nil {
		return "", fmt.Errorf("put-if-absent %q: %w", key, classify(err))
	}
	return aws.ToString(out.ETag), nil
}

// Put implements ObjectStore with an unconditional write. Used only for
// immutable index files written under a fresh epoch prefix where no concurrent
// writer can collide.
func (s *S3Store) Put(ctx context.Context, key string, body []byte) (string, error) {
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return "", fmt.Errorf("put %q: %w", key, classify(err))
	}
	return aws.ToString(out.ETag), nil
}

// List implements ObjectStore, paging through every object whose key starts with
// prefix.
func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: &prefix,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list %q: %w", prefix, classify(err))
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

// deleteForTest removes a single object. It exists so integration tests can
// clean up after themselves without importing the AWS SDK, keeping s3.go the
// only file that touches it. It is best-effort: the returned error is for the
// caller to log or ignore.
func (s *S3Store) deleteForTest(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	return nil
}

// statusCoder is implemented by the SDK's transport error wrappers
// (smithy-go and aws transport http ResponseError both expose HTTPStatusCode).
// Matching against it with errors.As recovers the HTTP status regardless of how
// many middleware layers wrap the error.
type statusCoder interface {
	HTTPStatusCode() int
}

// classify maps an SDK error to the package's sentinel errors. It prefers the
// structured HTTP status (412 → ErrPreconditionFailed, 404 → ErrNotFound) and
// falls back to string matching only when the status code is unavailable — the
// fallback documented in docs/06's "SDK facts". Any other error is returned
// unchanged for the caller to wrap.
func classify(err error) error {
	if err == nil {
		return nil
	}

	var sc statusCoder
	if errors.As(err, &sc) {
		switch sc.HTTPStatusCode() {
		case 412:
			return ErrPreconditionFailed
		case 404:
			return ErrNotFound
		}
		return err
	}

	// String fallback: only reached when the error carries no HTTP status.
	msg := err.Error()
	switch {
	case strings.Contains(msg, "PreconditionFailed") || strings.Contains(msg, "412"):
		return ErrPreconditionFailed
	case strings.Contains(msg, "NoSuchKey") || strings.Contains(msg, "NotFound") || strings.Contains(msg, "404"):
		return ErrNotFound
	}
	return err
}
