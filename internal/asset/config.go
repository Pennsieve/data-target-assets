package asset

import (
	"fmt"
	"os"
)

// targetConfig holds the asset-target-specific env vars. Loaded from
// environment in addition to the shared config.Config.
type targetConfig struct {
	AssetType           string
	AssetName           string
	AssetPropertiesFile string

	// ChatSessionID is set only for chat-triggered runs. When present it is
	// stamped on the created viewer asset (POST /assets chat_session_id) so
	// the figure is linked to the chat session for lifecycle (FK cascade on
	// session delete) and excluded from the dataset asset listing. Optional:
	// empty for ordinary dataset-scoped asset imports.
	ChatSessionID string
}

// loadTargetConfig reads the asset-target env vars set by the workflow
// orchestrator. All three are required — the front end always provides
// them when configuring a workflow run.
func loadTargetConfig() (*targetConfig, error) {
	tc := &targetConfig{
		AssetType:           os.Getenv("ASSET_TYPE"),
		AssetName:           os.Getenv("ASSET_NAME"),
		AssetPropertiesFile: os.Getenv("ASSET_PROPERTIES_FILE"),
		ChatSessionID:       os.Getenv("CHAT_SESSION_ID"),
	}

	if tc.AssetType == "" {
		return nil, fmt.Errorf("ASSET_TYPE is required")
	}
	if tc.AssetName == "" {
		return nil, fmt.Errorf("ASSET_NAME is required")
	}
	if tc.AssetPropertiesFile == "" {
		return nil, fmt.Errorf("ASSET_PROPERTIES_FILE is required")
	}

	return tc, nil
}
