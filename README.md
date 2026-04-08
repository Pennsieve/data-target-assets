WHat# data-target-assets

A Pennsieve data target that imports asset files (e.g. viewer assets) into the platform via the `/import` API. Runs as an ECS task or AWS Lambda function.

## How It Works

1. Discovers files in `INPUT_DIR`
2. Resolves the target package ID — uses `PACKAGE_ID` directly, or looks it up from the execution run's data source if set to `"default"`
3. Creates an import job via `POST /import` with configurable import type and viewer type
4. Uploads files in parallel using presigned S3 URLs

Authentication uses callback tokens from the workflow orchestrator.

## Configuration

### Required Environment Variables

| Variable | Description |
|----------|-------------|
| `INPUT_DIR` | Directory containing files to import |
| `CALLBACK_TOKEN` | Orchestrator callback token for API auth |
| `EXECUTION_RUN_ID` | Workflow execution run ID |
| `DATASET_ID` | Target Pennsieve dataset ID |
| `PACKAGE_ID` | Target package ID. Set to `"default"` to resolve from the execution run's data source |

### Optional Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PENNSIEVE_API_HOST2` | `https://api2.pennsieve.net` | Pennsieve API endpoint |
| `ORGANIZATION_ID` | | Organization ID (for logging) |
| `IMPORT_TYPE` | `viewerAssets` | Import type sent to the `/import` API |
| `VIEWER_TYPE` | `parquet-umap-viewer` | Viewer type option in the import body |

### Lambda Mode

When running as a Lambda function (`AWS_LAMBDA_RUNTIME_API` is set), the handler accepts a JSON event with:

```json
{
  "inputDir": "/mnt/efs/input",
  "executionRunId": "...",
  "callbackToken": "...",
  "datasetId": "...",
  "organizationId": "...",
  "targetType": "...",
  "params": {
    "IMPORT_TYPE": "viewerAssets",
    "VIEWER_TYPE": "parquet-umap-viewer",
    "PACKAGE_ID": "..."
  }
}
```

Fields in `params` are bridged to environment variables.

## Build

```bash
make build          # build Go binary
make test           # run tests
make docker-build   # build Docker image
make docker-push    # build and push Docker image
make clean          # remove build artifacts
```

## Import API Request

The target sends the following to `POST {apiHost2}/import?dataset_id={datasetId}`:

```json
{
  "integration_id": "<executionRunId>",
  "package_id": "<packageId>",
  "import_type": "<IMPORT_TYPE>",
  "files": [
    {"upload_key": "<uuid>", "file_path": "<filename>"}
  ],
  "options": {
    "viewerType": "<VIEWER_TYPE>"
  }
}
```

Each file is then uploaded via a presigned URL obtained from `GET /import/{importId}/upload/{uploadKey}/presign`.
