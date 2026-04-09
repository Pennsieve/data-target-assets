package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/google/uuid"
)

// loadAssetProperties reads a JSON file and returns it as a map.
// If filename is empty or the file does not exist, returns an empty map.
func loadAssetProperties(inputDir, filename string) (map[string]interface{}, error) {
	if filename == "" {
		return map[string]interface{}{}, nil
	}
	path := filepath.Join(inputDir, filename)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Asset properties file %s not found, using empty properties", path)
			return map[string]interface{}{}, nil
		}
		return nil, fmt.Errorf("reading asset properties file %s: %w", path, err)
	}
	var props map[string]interface{}
	if err := json.Unmarshal(data, &props); err != nil {
		return nil, fmt.Errorf("parsing asset properties file %s: %w", path, err)
	}
	return props, nil
}

// Config holds the environment configuration passed by the orchestrator.
type Config struct {
	// Standard env vars
	InputDir       string
	APIHost2       string
	ExecutionRunID string
	CallbackToken  string

	// Target-specific env vars
	DatasetID      string
	OrganizationID string
	PackageID           string
	ImportType          string
	AssetType           string
	AssetPropertiesFile string
}

// LambdaEvent mirrors the per-invocation payload fields sent by the
// Step Functions Lambda invoke state.
type LambdaEvent struct {
	InputDir       string `json:"inputDir"`
	ExecutionRunID string `json:"executionRunId"`
	IntegrationID  string `json:"integrationId"`
	ComputeNodeID  string `json:"computeNodeId"`
	CallbackToken  string `json:"callbackToken"`
	DatasetID      string `json:"datasetId"`
	OrganizationID string `json:"organizationId"`
	TargetType     string `json:"targetType"`

	// Target-type-specific params (SCREAMING_SNAKE_CASE keys → env vars)
	Params map[string]string `json:"params"`
}

// LambdaResponse is returned to Step Functions after the handler completes.
type LambdaResponse struct {
	Status         string `json:"status"`
	ExecutionRunID string `json:"executionRunId"`
}

func loadConfig() (*Config, error) {
	cfg := &Config{
		InputDir:       os.Getenv("INPUT_DIR"),
		APIHost2:       os.Getenv("PENNSIEVE_API_HOST2"),
		ExecutionRunID: os.Getenv("EXECUTION_RUN_ID"),
		CallbackToken:  os.Getenv("CALLBACK_TOKEN"),
		DatasetID:      os.Getenv("DATASET_ID"),
		OrganizationID: os.Getenv("ORGANIZATION_ID"),
		PackageID:      os.Getenv("PACKAGE_ID"),
		ImportType:     os.Getenv("IMPORT_TYPE"),
		AssetType:           os.Getenv("ASSET_TYPE"),
		AssetPropertiesFile: os.Getenv("ASSET_PROPERTIES_FILE"),
	}

	if cfg.APIHost2 == "" {
		cfg.APIHost2 = "https://api2.pennsieve.net"
	}
	if cfg.ImportType == "" {
		cfg.ImportType = "viewerAssets"
	}
	if cfg.AssetType == "" {
		cfg.AssetType = "parquet-umap-viewer"
	}

	if cfg.InputDir == "" {
		return nil, fmt.Errorf("INPUT_DIR is required")
	}
	if cfg.CallbackToken == "" {
		return nil, fmt.Errorf("CALLBACK_TOKEN is required")
	}
	if cfg.DatasetID == "" {
		return nil, fmt.Errorf("DATASET_ID is required")
	}
	if cfg.ExecutionRunID == "" {
		return nil, fmt.Errorf("EXECUTION_RUN_ID is required")
	}
	if cfg.PackageID == "" {
		return nil, fmt.Errorf("PACKAGE_ID is required")
	}

	return cfg, nil
}

// discoverFiles walks INPUT_DIR and returns all regular file paths.
// It follows symlinks so that processor output written as symlinked
// directories is included.
func discoverFiles(inputDir string) ([]string, error) {
	var files []string
	var walk func(dir string) error
	walk = func(dir string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			path := filepath.Join(dir, e.Name())
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			if info.IsDir() {
				if err := walk(path); err != nil {
					return err
				}
			} else if info.Mode().IsRegular() {
				files = append(files, path)
			}
		}
		return nil
	}
	return files, walk(inputDir)
}

// run contains the core target logic shared between ECS and Lambda modes.
func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	log.Printf("Starting asset-import target")
	log.Printf("  executionRunId: %s", cfg.ExecutionRunID)
	log.Printf("  inputDir:       %s", cfg.InputDir)
	log.Printf("  datasetId:      %s", cfg.DatasetID)
	log.Printf("  organizationId: %s", cfg.OrganizationID)
	log.Printf("  importType:     %s", cfg.ImportType)
	log.Printf("  assetType:      %s", cfg.AssetType)
	log.Printf("  apiHost2:       %s", cfg.APIHost2)

	// Load asset properties from JSON file (if configured)
	assetProperties, err := loadAssetProperties(cfg.InputDir, cfg.AssetPropertiesFile)
	if err != nil {
		return fmt.Errorf("failed to load asset properties: %w", err)
	}
	if cfg.AssetPropertiesFile != "" {
		log.Printf("Loaded asset properties from %s", cfg.AssetPropertiesFile)
	}

	// Discover files from input directory
	files, err := discoverFiles(cfg.InputDir)
	if err != nil {
		return fmt.Errorf("failed to discover files in %s: %w", cfg.InputDir, err)
	}

	// Exclude the properties file from uploads
	if cfg.AssetPropertiesFile != "" {
		propsPath := filepath.Join(cfg.InputDir, cfg.AssetPropertiesFile)
		filtered := files[:0]
		for _, f := range files {
			if f != propsPath {
				filtered = append(filtered, f)
			}
		}
		files = filtered
	}

	if len(files) == 0 {
		log.Printf("No files found in %s, nothing to import", cfg.InputDir)
		return nil
	}

	log.Printf("Discovered %d files to import:", len(files))
	for _, f := range files {
		info, _ := os.Stat(f)
		size := int64(0)
		if info != nil {
			size = info.Size()
		}
		rel, _ := filepath.Rel(cfg.InputDir, f)
		log.Printf("  %s (%d bytes)", rel, size)
	}

	// Step 1: Create Pennsieve client (uses callback auth)
	client := NewPennsieveClient(cfg.APIHost2, cfg.ExecutionRunID, cfg.CallbackToken)

	// Step 2: Resolve package ID — use provided value or look up from execution run
	packageID := cfg.PackageID
	if packageID == "default" {
		log.Printf("PACKAGE_ID is 'default', resolving from execution run %s...", cfg.ExecutionRunID)
		run, err := client.GetExecutionRun(cfg.ExecutionRunID)
		if err != nil {
			return fmt.Errorf("failed to get execution run: %w", err)
		}
		packageID, err = GetPackageID(run)
		if err != nil {
			return fmt.Errorf("failed to resolve package ID: %w", err)
		}
		log.Printf("Resolved package ID: %s", packageID)
	} else {
		log.Printf("Using PACKAGE_ID: %s", packageID)
	}

	// Step 3: Build import file list
	importFiles := make([]ImportFile, len(files))
	for i, f := range files {
		rel, _ := filepath.Rel(cfg.InputDir, f)
		importFiles[i] = ImportFile{
			UploadKey: uuid.New().String(),
			FilePath:  rel,
			LocalPath: f,
		}
	}

	// Step 4: Create import via /import API
	log.Printf("Creating import for dataset %s, package %s...", cfg.DatasetID, packageID)
	importID, err := client.CreateImport(
		cfg.DatasetID,
		cfg.ExecutionRunID,
		packageID,
		cfg.ImportType,
		importFiles,
		map[string]interface{}{
			"asset_type": cfg.AssetType,
			"properties": assetProperties,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create import: %w", err)
	}
	log.Printf("Created import: %s", importID)

	// Step 5: Get scoped S3 credentials for upload
	log.Printf("Getting upload credentials...")
	creds, err := client.GetUploadCredentials(importID, cfg.DatasetID)
	if err != nil {
		return fmt.Errorf("failed to get upload credentials: %w", err)
	}
	log.Printf("Obtained upload credentials for bucket %s", creds.Bucket)

	// Step 6: Upload files directly to S3
	log.Printf("Starting S3 upload of %d files...", len(importFiles))
	if err := UploadFiles(context.Background(), creds, importID, importFiles, cfg.OrganizationID, cfg.DatasetID); err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	// Step 7: Summary
	log.Printf("Import complete: %d files uploaded to import %s", len(files), importID)
	return nil
}

// lambdaHandler bridges the Lambda invocation payload to environment variables,
// then runs the same logic as ECS mode.
func lambdaHandler(_ context.Context, event LambdaEvent) (LambdaResponse, error) {
	log.Printf("Lambda handler invoked")

	os.Setenv("INPUT_DIR", event.InputDir)
	os.Setenv("EXECUTION_RUN_ID", event.ExecutionRunID)
	os.Setenv("CALLBACK_TOKEN", event.CallbackToken)
	os.Setenv("DATASET_ID", event.DatasetID)
	os.Setenv("ORGANIZATION_ID", event.OrganizationID)
	os.Setenv("TARGET_TYPE", event.TargetType)

	for k, v := range event.Params {
		os.Setenv(k, v)
	}

	if err := run(); err != nil {
		return LambdaResponse{Status: "error", ExecutionRunID: event.ExecutionRunID}, err
	}

	return LambdaResponse{Status: "success", ExecutionRunID: event.ExecutionRunID}, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		log.Printf("Detected Lambda runtime, starting RIC handler")
		lambda.Start(lambdaHandler)
	} else {
		log.Printf("Running in ECS/local mode")
		if err := run(); err != nil {
			log.Fatalf("%v", err)
		}
	}
}
