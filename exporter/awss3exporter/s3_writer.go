// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter"

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter/internal/upload"
)

// Add middleware to recalculate signature after amending accept-encoding header

type RecalculateV4Signature struct {
	next   http.RoundTripper
	signer *v4.Signer
	cfg    aws.Config
}

func (lt *RecalculateV4Signature) RoundTrip(req *http.Request) (*http.Response, error) {
	val := req.Header.Get("Accept-Encoding")
	req.Header.Del("Accept-Encoding")

	req.Header.Del("Content-Encoding")
	req.Header.Del("X-Amz-Storage-Class")
	req.Header.Del("X-Amz-Content-Sha256")

	timeString := req.Header.Get("X-Amz-Date")
	req.Header.Del("X-Amz-Date")

	timeDate, _ := time.Parse("20060102T150405Z", timeString)
	creds, _ := lt.cfg.Credentials.Retrieve(req.Context())
	err := lt.signer.SignHTTP(req.Context(), creds, req, v4.GetPayloadHash(req.Context()), "s3", lt.cfg.Region, timeDate)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", val)
	fmt.Println("AfterAdjustment")
	rrr, _ := httputil.DumpRequest(req, false)
	fmt.Println(string(rrr))
	return lt.next.RoundTrip(req)
}

func newUploadManager(
	ctx context.Context,
	conf *Config,
	metadata string,
	format string,
) (upload.Manager, error) {
	configOpts := []func(*config.LoadOptions) error{}

	if region := conf.S3Uploader.Region; region != "" {
		configOpts = append(configOpts, config.WithRegion(region))
	}

	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return nil, err
	}

	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.EndpointOptions = s3.EndpointResolverOptions{
				DisableHTTPS: conf.S3Uploader.DisableSSL,
			}
			o.UsePathStyle = conf.S3Uploader.S3ForcePathStyle
		},
	}

	if conf.S3Uploader.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String((conf.S3Uploader.Endpoint))
		})
	}

	if arn := conf.S3Uploader.RoleArn; arn != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.Credentials = stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), arn)
		})
	}

	if endpoint := conf.S3Uploader.Endpoint; endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	// If using GCS endpoint, add the recalculate signature middleware
	if endpoint := conf.S3Uploader.Endpoint; endpoint == "https://storage.googleapis.com" {
		// Assign custom HTTP client with our middleware
		cfg.HTTPClient = &http.Client{
			Transport: &RecalculateV4Signature{
				next:   http.DefaultTransport,
				signer: v4.NewSigner(),
				cfg:    cfg,
			},
		}
		cfg.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	}

	return upload.NewS3Manager(
		conf.S3Uploader.S3Bucket,
		&upload.PartitionKeyBuilder{
			PartitionPrefix:     conf.S3Uploader.S3Prefix,
			PartitionTruncation: conf.S3Uploader.S3Partition,
			FilePrefix:          conf.S3Uploader.FilePrefix,
			Metadata:            metadata,
			FileFormat:          format,
			Compression:         conf.S3Uploader.Compression,
		},
		s3.NewFromConfig(cfg, s3Opts...),
		s3types.StorageClass(conf.S3Uploader.StorageClass),
	), nil
}
