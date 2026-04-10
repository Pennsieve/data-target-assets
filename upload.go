package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const partSize = 64 * 1024 * 1024 // 64 MB per part

// UploadFiles uploads all files to S3 using temporary credentials scoped to the asset prefix.
func UploadFiles(ctx context.Context, creds *UploadCredentials, files []string, inputDir string) error {
	region := creds.Region
	if region == "" {
		region = "us-east-1"
	}

	expiration, _ := time.Parse(time.RFC3339, creds.Expiration)
	log.Printf("Upload credentials expire at %s", expiration.Format("15:04:05"))

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			creds.AccessKeyID,
			creds.SecretAccessKey,
			creds.SessionToken,
		)),
	)
	if err != nil {
		return fmt.Errorf("creating S3 config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = partSize
	})

	for i, localPath := range files {
		rel, _ := relPath(inputDir, localPath)
		s3Key := creds.KeyPrefix + rel
		log.Printf("Uploading %d/%d: %s → s3://%s/%s", i+1, len(files), rel, creds.Bucket, s3Key)

		file, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("opening %s: %w", localPath, err)
		}

		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:            aws.String(creds.Bucket),
			Key:               aws.String(s3Key),
			Body:              file,
			ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
		})
		file.Close()
		if err != nil {
			return fmt.Errorf("uploading %s: %w", rel, err)
		}
	}

	return nil
}

func relPath(base, target string) (string, error) {
	return filepath.Rel(base, target)
}