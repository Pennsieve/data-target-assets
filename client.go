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

// UploadCredentials holds temporary AWS credentials for S3 uploads.
type UploadCredentials struct {
	AccessKeyID    string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken   string `json:"session_token"`
	Expiration     string `json:"expiration"`
	Bucket         string `json:"bucket"`
	Region         string `json:"region"`
	KeyPrefix      string `json:"key_prefix"`
}

type createViewerAssetRequest struct {
	Name       string                 `json:"name"`
	AssetType  string                 `json:"asset_type"`
	Properties map[string]interface{} `json:"properties"`
	PackageIDs []string               `json:"package_ids,omitempty"`
}

type viewerAssetResponse struct {
	ID        string `json:"id"`
	DatasetID string `json:"dataset_id"`
	Name      string `json:"name"`
	AssetType string `json:"asset_type"`
	Status    string `json:"status"`
}

type createViewerAssetResponseBody struct {
	Asset             viewerAssetResponse `json:"asset"`
	UploadCredentials UploadCredentials   `json:"upload_credentials"`
}

type updateViewerAssetRequest struct {
	Status *string `json:"status,omitempty"`
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

// GetPackageIDs extracts all package IDs from the execution run's data sources.
func GetPackageIDs(run *ExecutionRunDetail) ([]string, error) {
	if len(run.DataSources) == 0 {
		return nil, fmt.Errorf("execution run has no data sources")
	}
	var packageIDs []string
	for _, ds := range run.DataSources {
		packageIDs = append(packageIDs, ds.PackageIDs...)
	}
	if len(packageIDs) == 0 {
		return nil, fmt.Errorf("execution run has no package IDs")
	}
	return packageIDs, nil
}

// CreateViewerAsset creates a viewer asset and returns upload credentials.
func (c *PennsieveClient) CreateViewerAsset(datasetID, name, assetType string, properties map[string]interface{}, packageIDs []string) (*createViewerAssetResponseBody, error) {
	reqURL := fmt.Sprintf("%s/packages/assets?dataset_id=%s", c.apiHost2, url.QueryEscape(datasetID))

	body := createViewerAssetRequest{
		Name:       name,
		AssetType:  assetType,
		Properties: properties,
		PackageIDs: packageIDs,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling create asset request: %w", err)
	}

	req, err := http.NewRequest("POST", reqURL, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("creating asset request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuthHeader(req)

	var result createViewerAssetResponseBody
	if err := c.doJSON(req, &result); err != nil {
		return nil, fmt.Errorf("creating viewer asset: %w", err)
	}
	return &result, nil
}

// MarkViewerAssetReady marks a viewer asset as ready after upload completes.
func (c *PennsieveClient) MarkViewerAssetReady(assetID, datasetID string) error {
	reqURL := fmt.Sprintf("%s/packages/assets/%s?dataset_id=%s",
		c.apiHost2,
		url.PathEscape(assetID),
		url.QueryEscape(datasetID),
	)

	status := "ready"
	body := updateViewerAssetRequest{Status: &status}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshaling update request: %w", err)
	}

	req, err := http.NewRequest("PATCH", reqURL, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("creating update request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuthHeader(req)

	var result json.RawMessage
	if err := c.doJSON(req, &result); err != nil {
		return fmt.Errorf("marking asset ready: %w", err)
	}
	return nil
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
