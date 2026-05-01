package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/pennsieve/data-target-assets/internal/config"
	"github.com/pennsieve/data-target-assets/internal/pennsieve"
	"github.com/pennsieve/data-target-assets/internal/timeseries"
)

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}
	client := pennsieve.NewClient(cfg.APIHost2, cfg.ExecutionRunID, cfg.CallbackToken)
	return timeseries.Run(context.Background(), cfg, client)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}
