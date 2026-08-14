package asset

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/pennsieve/data-target-assets/internal/shared/clients/pennsieve"
	"github.com/pennsieve/data-target-assets/internal/shared/config"
)

// Run executes the asset-target flow: discover files, create a viewer
// asset, upload all files to its S3 prefix, and mark it ready.
func Run(ctx context.Context, cfg *config.Config, client *pennsieve.Client) error {
	tc, err := loadTargetConfig()
	if err != nil {
		return err
	}

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
	slog.Info("loaded asset properties", "file", tc.AssetPropertiesFile)

	uploadRoot, declaredRoot, err := resolveUploadRoot(cfg.InputDir, assetProperties)
	if err != nil {
		return fmt.Errorf("failed to resolve upload root: %w", err)
	}
	if declaredRoot != "" {
		slog.Info("uploading a declared asset root", "rootPath", declaredRoot, "uploadRoot", uploadRoot)
	}

	files, err := discoverFiles(uploadRoot)
	if err != nil {
		return fmt.Errorf("failed to discover files in %s: %w", uploadRoot, err)
	}

	// Exclude the properties file itself from the upload set. A declared root
	// leaves the file above uploadRoot, where the walk never reaches it.
	propsPath := filepath.Join(cfg.InputDir, tc.AssetPropertiesFile)
	filtered := files[:0]
	for _, f := range files {
		if f != propsPath {
			filtered = append(filtered, f)
		}
	}
	files = filtered

	if len(files) == 0 {
		// A producer that names a root and then ships nothing has failed, so say so
		// rather than creating an empty asset.
		if declaredRoot != "" {
			return fmt.Errorf("%s %s holds no files", rootPathKey, declaredRoot)
		}
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
		rel, _ := filepath.Rel(uploadRoot, f)
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

	// Chat-scoped figures are NOT attached to the package. They render inline in
	// the chat (resolved by the assetId reported as a run output below) and the
	// user promotes them to the package on demand via
	// PATCH /packages/assets (package_ids + clear_chat_session). Sending no
	// package IDs here means packages-service creates no viewer_asset_packages
	// link, so the figure stays out of the package's asset listing until
	// promoted. The originating package id is preserved on the chat message's
	// image block (packageNodeId), so promotion can re-attach it.
	if tc.ChatSessionID != "" {
		slog.Info("chat-scoped asset: skipping package link until promoted", "chatSessionId", tc.ChatSessionID, "resolvedPackageIds", packageIDs)
		packageIDs = nil
	}

	slog.Info("creating viewer asset", "datasetId", cfg.DatasetID, "chatSessionId", tc.ChatSessionID)
	result, err := client.CreateViewerAsset(
		cfg.DatasetID,
		tc.AssetName,
		tc.AssetType,
		assetProperties,
		packageIDs,
		tc.ChatSessionID,
	)
	if err != nil {
		return fmt.Errorf("failed to create viewer asset: %w", err)
	}
	assetID := result.Asset.ID
	slog.Info("created viewer asset", "assetId", assetID, "keyPrefix", result.UploadCredentials.KeyPrefix)

	slog.Info("starting S3 upload", "fileCount", len(files))
	if err := pennsieve.UploadFiles(ctx, &result.UploadCredentials, files, uploadRoot); err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	slog.Info("marking asset as ready", "assetId", assetID)
	if err := client.MarkViewerAssetReady(assetID, cfg.DatasetID); err != nil {
		return fmt.Errorf("failed to mark asset ready: %w", err)
	}

	// Report the created asset's UUID back onto the run so completion
	// subscribers (e.g. chat-service) can reference it — the UUID is
	// generated here at runtime and so can't ride the run's frozen completion
	// callbackContext. Non-fatal: the asset is created and ready regardless,
	// and an older workflow-service without this route just 404s. A failure
	// only costs the optional downstream delete-by-id affordance.
	if err := client.ReportOutputs(cfg.ExecutionRunID, map[string]string{"assetId": assetID}); err != nil {
		slog.Warn("failed to report assetId output; downstream delete-by-id won't be available for this asset",
			"assetId", assetID, "error", err)
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

// rootPathKey is the asset-properties key a producer uses to name the directory
// holding the asset. It directs this processor and is not asset metadata, so
// resolveUploadRoot removes it before the properties reach the viewer asset.
const rootPathKey = "root_path"

// resolveUploadRoot returns the directory whose contents become the asset, plus
// the root_path that selected it (empty when the producer declared none).
//
// A producer whose output is one directory names it here, and that directory
// becomes the asset prefix, so every consumer finds the artifact's own root at
// the top of the asset. An absent, empty, or "." value selects inputDir, which
// is what a producer emitting flat files wants. Any other value must name an
// existing directory inside inputDir.
func resolveUploadRoot(inputDir string, props map[string]interface{}) (string, string, error) {
	raw, ok := props[rootPathKey]
	if !ok {
		return inputDir, "", nil
	}
	delete(props, rootPathKey)

	declared, ok := raw.(string)
	if !ok {
		return "", "", fmt.Errorf("%s must be a string, got %T", rootPathKey, raw)
	}
	if declared == "" || declared == "." {
		return inputDir, "", nil
	}
	if filepath.IsAbs(declared) {
		return "", "", fmt.Errorf("%s must be relative to the input directory, got %s", rootPathKey, declared)
	}

	root := filepath.Join(inputDir, declared)
	rel, err := filepath.Rel(inputDir, root)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf("%s must stay inside the input directory, got %s", rootPathKey, declared)
	}

	info, err := os.Stat(root)
	if err != nil {
		return "", "", fmt.Errorf("%s %s: %w", rootPathKey, declared, err)
	}
	if !info.IsDir() {
		return "", "", fmt.Errorf("%s %s is not a directory", rootPathKey, declared)
	}
	return root, declared, nil
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
