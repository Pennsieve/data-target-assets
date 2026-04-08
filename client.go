package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// PennsieveClient is a minimal HTTP client for the Pennsieve API endpoints
// needed by the asset import target.
type PennsieveClient struct {
	apiHost2       string
	executionRunID string
	callbackToken  string
	httpClient     *http.Client
}

func NewPennsieveClient(apiHost2, executionRunID, callbackToken string) *PennsieveClient {
	return &PennsieveClient{
		apiHost2:       apiHost2,
		executionRunID: executionRunID,
		callbackToken:  callbackToken,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ExecutionRunDetail holds the fields returned by GET /workflows/runs/{runId}.
type ExecutionRunDetail struct {
	Uuid        string                       `json:"uuid"`
	DatasetID   string                       `json:"datasetId"`
	DataSources map[string]DataSourceInput   `json:"dataSources,omitempty"`
}

// DataSourceInput holds the per-data-source inputs for a workflow execution run.
type DataSourceInput struct {
	PackageIDs []string `json:"packageIds"`
	Path       string   `json:"path,omitempty"`
}

// ImportFile represents a file to be included in an import request.
type ImportFile struct {
	UploadKey string `json:"upload_key"`
	FilePath  string `json:"file_path"`
	LocalPath string `json:"-"`
}

// importRequest is the POST body for /import.
type importRequest struct {
	IntegrationID string            `json:"integration_id"`
	PackageID     string            `json:"package_id"`
	ImportType    string            `json:"import_type"`
	Files         []importFileDTO   `json:"files"`
	Options       map[string]string `json:"options"`
}

type importFileDTO struct {
	UploadKey string `json:"upload_key"`
	FilePath  string `json:"file_path"`
}

type importResponse struct {
	ID string `json:"id"`
}

type presignResponse struct {
	URL string `json:"url"`
}

// GetExecutionRun fetches the execution run to resolve data sources and package IDs.
func (c *PennsieveClient) GetExecutionRun(runID string) (*ExecutionRunDetail, error) {
	reqURL := fmt.Sprintf("%s/compute/workflows/runs/%s", c.apiHost2, url.PathEscape(runID))

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating execution run request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	c.setAuthHeader(req)

	var run ExecutionRunDetail
	if err := c.doJSON(req, &run); err != nil {
		return nil, fmt.Errorf("fetching execution run: %w", err)
	}
	return &run, nil
}

// GetPackageID extracts the single package ID from the execution run's data sources.
// Assumes a single data source with a single package.
func GetPackageID(run *ExecutionRunDetail) (string, error) {
	if len(run.DataSources) == 0 {
		return "", fmt.Errorf("execution run has no data sources")
	}
	if len(run.DataSources) > 1 {
		return "", fmt.Errorf("execution run has %d data sources, expected 1", len(run.DataSources))
	}
	for _, ds := range run.DataSources {
		if len(ds.PackageIDs) == 0 {
			return "", fmt.Errorf("data source has no package IDs")
		}
		if len(ds.PackageIDs) > 1 {
			return "", fmt.Errorf("data source has %d packages, expected 1", len(ds.PackageIDs))
		}
		return ds.PackageIDs[0], nil
	}
	return "", fmt.Errorf("unreachable")
}

// CreateImport creates a new import job via POST /import.
func (c *PennsieveClient) CreateImport(datasetID, integrationID, packageID, importType string, files []ImportFile, options map[string]string) (string, error) {
	reqURL := fmt.Sprintf("%s/import?dataset_id=%s", c.apiHost2, url.QueryEscape(datasetID))

	fileDTOs := make([]importFileDTO, len(files))
	for i, f := range files {
		fileDTOs[i] = importFileDTO{
			UploadKey: f.UploadKey,
			FilePath:  f.FilePath,
		}
	}

	body := importRequest{
		IntegrationID: integrationID,
		PackageID:     packageID,
		ImportType:    importType,
		Files:         fileDTOs,
		Options:       options,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshaling import request: %w", err)
	}

	req, err := http.NewRequest("POST", reqURL, bytes.NewReader(jsonBody))
	if err != nil {
		return "", fmt.Errorf("creating import request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuthHeader(req)

	var result importResponse
	if err := c.doJSON(req, &result); err != nil {
		return "", fmt.Errorf("creating import: %w", err)
	}
	return result.ID, nil
}

// GetPresignURL gets a presigned S3 URL for uploading a specific file.
func (c *PennsieveClient) GetPresignURL(importID, datasetID, uploadKey string) (string, error) {
	reqURL := fmt.Sprintf("%s/import/%s/upload/%s/presign?dataset_id=%s",
		c.apiHost2,
		url.PathEscape(importID),
		url.PathEscape(uploadKey),
		url.QueryEscape(datasetID),
	)

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("creating presign request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuthHeader(req)

	var result presignResponse
	if err := c.doJSON(req, &result); err != nil {
		return "", fmt.Errorf("getting presign URL: %w", err)
	}
	return result.URL, nil
}

func (c *PennsieveClient) setAuthHeader(req *http.Request) {
	req.Header.Set("Authorization",
		fmt.Sprintf("Callback workflow-service:%s:%s", c.executionRunID, c.callbackToken))
}

func (c *PennsieveClient) doJSON(req *http.Request, result interface{}) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	return nil
}
