package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
	"time"
)

// UploadFiles uploads all files in parallel using presigned URLs.
// concurrency controls the number of simultaneous uploads.
func UploadFiles(files []ImportFile, getPresignURL func(uploadKey string) (string, error), concurrency int) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
		done int
	)

	sem := make(chan struct{}, concurrency)
	client := &http.Client{Timeout: 5 * time.Minute}

	for _, f := range files {
		wg.Add(1)
		go func(file ImportFile) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if err := uploadFileWithRetry(client, file, getPresignURL, 5); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", file.FilePath, err))
				mu.Unlock()
				return
			}

			mu.Lock()
			done++
			log.Printf("Uploaded %d/%d: %s", done, len(files), file.FilePath)
			mu.Unlock()
		}(f)
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("failed to upload %d files: %v", len(errs), errs)
	}
	return nil
}

func uploadFileWithRetry(client *http.Client, file ImportFile, getPresignURL func(string) (string, error), maxRetries int) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			log.Printf("Retrying %s (attempt %d/%d) after %s...", file.FilePath, attempt+1, maxRetries, backoff)
			time.Sleep(backoff)
		}

		lastErr = uploadFile(client, file, getPresignURL)
		if lastErr == nil {
			return nil
		}
		log.Printf("Upload attempt %d/%d for %s failed: %v", attempt+1, maxRetries, file.FilePath, lastErr)
	}
	return lastErr
}

func uploadFile(client *http.Client, file ImportFile, getPresignURL func(string) (string, error)) error {
	presignURL, err := getPresignURL(file.UploadKey)
	if err != nil {
		return fmt.Errorf("getting presign URL: %w", err)
	}

	f, err := os.Open(file.LocalPath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	req, err := http.NewRequest("PUT", presignURL, f)
	if err != nil {
		return fmt.Errorf("creating upload request: %w", err)
	}
	req.ContentLength = info.Size()

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("upload request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("upload returned HTTP %d", resp.StatusCode)
	}

	return nil
}
