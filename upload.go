package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const partSize = 64 * 1024 * 1024 // 64 MB per part

// UploadFiles uploads all files to S3 using temporary credentials scoped to the import prefix.
func UploadFiles(ctx context.Context, creds *UploadCredentials, importID string, files []ImportFile, orgID, datasetID string) error {
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

	tags := fmt.Sprintf("OrgId=%s&DatasetId=%s", orgID, datasetID)

	for i, f := range files {
		s3Key := fmt.Sprintf("%s/%s", importID, f.UploadKey)
		log.Printf("Uploading %d/%d: %s → s3://%s/%s", i+1, len(files), f.FilePath, creds.Bucket, s3Key)

		file, err := os.Open(f.LocalPath)
		if err != nil {
			return fmt.Errorf("opening %s: %w", f.LocalPath, err)
		}

		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:            aws.String(creds.Bucket),
			Key:               aws.String(s3Key),
			Body:              file,
			ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
			Tagging:           aws.String(tags),
		})
		file.Close()
		if err != nil {
			return fmt.Errorf("uploading %s: %w", f.FilePath, err)
		}
	}

	return nil
}
