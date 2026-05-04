package asset

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/pennsieve/data-target-assets/internal/shared/clients/pennsieve"
	"github.com/pennsieve/data-target-assets/internal/shared/config"
)

// Run executes the asset-target flow: discover files, create a viewer
// asset, upload all files to its S3 prefix, and mark it ready.
func Run(ctx context.Context, cfg *config.Config, client *pennsieve.Client) error {
	tc := loadTargetConfig()

	slog.Info("starting asset-import target",
		"executionRunId", cfg.ExecutionRunID,
		"inputDir", cfg.InputDir,
		"datasetId", cfg.DatasetID,
		"assetType", tc.AssetType,
		"assetName", tc.AssetName,
		"apiHost2", cfg.APIHost2,
	)

	assetProperties, err := loadAssetProperties(cfg.InputDir, tc.AssetPropertiesFile)
	if err != nil {
		return fmt.Errorf("failed to load asset properties: %w", err)
	}
	if tc.AssetPropertiesFile != "" {
		slog.Info("loaded asset properties", "file", tc.AssetPropertiesFile)
	}

	files, err := discoverFiles(cfg.InputDir)
	if err != nil {
		return fmt.Errorf("failed to discover files in %s: %w", cfg.InputDir, err)
	}

	if tc.AssetPropertiesFile != "" {
		propsPath := filepath.Join(cfg.InputDir, tc.AssetPropertiesFile)
		filtered := files[:0]
		for _, f := range files {
			if f != propsPath {
				filtered = append(filtered, f)
			}
		}
		files = filtered
	}

	if len(files) == 0 {
		slog.Info("no files found, nothing to import", "inputDir", cfg.InputDir)
		return nil
	}

	slog.Info("discovered files to upload", "count", len(files))
	for _, f := range files {
		info, _ := os.Stat(f)
		size := int64(0)
		if info != nil {
			size = info.Size()
		}
		rel, _ := filepath.Rel(cfg.InputDir, f)
		slog.Info("file", "path", rel, "bytes", size)
	}

	slog.Info("resolving package IDs from execution run", "executionRunId", cfg.ExecutionRunID)
	execRun, err := client.GetExecutionRun(cfg.ExecutionRunID)
	if err != nil {
		return fmt.Errorf("failed to get execution run: %w", err)
	}
	packageIDs, err := pennsieve.GetPackageIDs(execRun)
	if err != nil {
		return fmt.Errorf("failed to resolve package IDs: %w", err)
	}
	slog.Info("resolved package IDs", "count", len(packageIDs), "packageIds", packageIDs)

	slog.Info("creating viewer asset", "datasetId", cfg.DatasetID)
	result, err := client.CreateViewerAsset(
		cfg.DatasetID,
		tc.AssetName,
		tc.AssetType,
		assetProperties,
		packageIDs,
	)
	if err != nil {
		return fmt.Errorf("failed to create viewer asset: %w", err)
	}
	assetID := result.Asset.ID
	slog.Info("created viewer asset", "assetId", assetID, "keyPrefix", result.UploadCredentials.KeyPrefix)

	slog.Info("starting S3 upload", "fileCount", len(files))
	if err := pennsieve.UploadFiles(ctx, &result.UploadCredentials, files, cfg.InputDir); err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	slog.Info("marking asset as ready", "assetId", assetID)
	if err := client.MarkViewerAssetReady(assetID, cfg.DatasetID); err != nil {
		return fmt.Errorf("failed to mark asset ready: %w", err)
	}

	slog.Info("asset upload complete", "fileCount", len(files), "assetId", assetID)
	return nil
}

// loadAssetProperties reads a JSON file and returns it as a map. If
// filename is empty or the file does not exist, returns an empty map.
func loadAssetProperties(inputDir, filename string) (map[string]interface{}, error) {
	if filename == "" {
		return map[string]interface{}{}, nil
	}
	path := filepath.Join(inputDir, filename)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("asset properties file not found, using empty properties", "path", path)
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

// discoverFiles walks inputDir and returns all regular file paths.
// Follows symlinks so processor output written as symlinked dirs is included.
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
