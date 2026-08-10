# data-target-assets

A Pennsieve data target that imports asset files (e.g. viewer assets) into the platform via the `/import` API. Runs as an ECS task or AWS Lambda function.

## How It Works

1. Discovers files in `INPUT_DIR`
2. Resolves the target package ID — uses `PACKAGE_ID` directly, or looks it up from the execution run's data source if set to `"default"`
3. Creates an import job via `POST /import` with configurable import type and asset type
4. Gets scoped S3 credentials from the import service (single API call)
5. Uploads files directly to S3 using the AWS SDK

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
| `ASSET_TYPE` | `parquet-umap-viewer` | Asset type option in the import body |
| `ASSET_PROPERTIES_FILE` | | JSON file in INPUT_DIR with asset properties (optional, defaults to `{}`) |

### Asset properties

The file named by `ASSET_PROPERTIES_FILE` holds one JSON object. Its keys become the
viewer asset's `properties`, with one exception.

| Key | Meaning |
|-----|---------|
| `root_path` | Directory, relative to INPUT_DIR, whose **contents** become the asset |

`root_path` directs this processor rather than describing the asset, so it is consumed
here and never stored. Uploading the contents of the named directory makes the asset
prefix the artifact's own root, which is what viewers expect: a Zarr bundle's `zarr.json`
lands at the top of the prefix rather than under a wrapper directory.

Use it when the producing processor writes its output as a single directory. A producer
that writes flat files omits the key, and every file in INPUT_DIR is uploaded as before.
The value must name an existing directory inside INPUT_DIR; an absolute path, a path
escaping INPUT_DIR, a missing directory, or a directory holding no files fails the run.

```json
{ "root_path": "session.zarr", "sample_rate": 512 }
```

uploads `session.zarr/zarr.json` as `zarr.json` and stores `{"sample_rate": 512}` on the
viewer asset.

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
    "ASSET_TYPE": "parquet-umap-viewer",
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
    "asset_type": "<ASSET_TYPE>"
  }
}
```

After creating the import, the target calls `GET /import/{importId}/upload-credentials` once to get temporary S3 credentials scoped to the import prefix, then uploads all files directly to S3 at `s3://{bucket}/{importId}/{uploadKey}`.
