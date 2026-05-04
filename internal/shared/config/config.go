package config

import (
	"fmt"
	"os"
)

// Config holds the workflow-runtime fields common to every data target.
// Target-specific config (asset name, properties file, etc.) lives in
// the target's own package.
type Config struct {
	InputDir       string
	APIHost2       string
	ExecutionRunID string
	CallbackToken  string
	DatasetID      string
	OrganizationID string
}

func Load() (*Config, error) {
	cfg := &Config{
		InputDir:       os.Getenv("INPUT_DIR"),
		APIHost2:       os.Getenv("PENNSIEVE_API_HOST2"),
		ExecutionRunID: os.Getenv("EXECUTION_RUN_ID"),
		CallbackToken:  os.Getenv("CALLBACK_TOKEN"),
		DatasetID:      os.Getenv("DATASET_ID"),
		OrganizationID: os.Getenv("ORGANIZATION_ID"),
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
	if cfg.APIHost2 == "" {
		return nil, fmt.Errorf("PENNSIEVE_API_HOST2 is required")
	}

	return cfg, nil
}
