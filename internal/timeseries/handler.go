package timeseries

import (
	"context"
	"errors"

	"github.com/pennsieve/data-target-assets/internal/shared/clients/pennsieve"
	"github.com/pennsieve/data-target-assets/internal/shared/config"
)

// Run is the time-series data target entrypoint. Not yet implemented —
// the asset-flow ingest logic from processor-post-timeseries will be
// ported here in a follow-up PR.
func Run(ctx context.Context, cfg *config.Config, client *pennsieve.Client) error {
	return errors.New("timeseries data target: not implemented")
}
