package asset

import "os"

// targetConfig holds the asset-target-specific env vars. Loaded from
// environment in addition to the shared config.Config.
type targetConfig struct {
	AssetType           string
	AssetName           string
	AssetPropertiesFile string
}

func loadTargetConfig() *targetConfig {
	tc := &targetConfig{
		AssetType:           os.Getenv("ASSET_TYPE"),
		AssetName:           os.Getenv("ASSET_NAME"),
		AssetPropertiesFile: os.Getenv("ASSET_PROPERTIES_FILE"),
	}
	if tc.AssetType == "" {
		tc.AssetType = "parquet-umap-viewer"
	}
	if tc.AssetName == "" {
		tc.AssetName = tc.AssetType
	}
	return tc
}
