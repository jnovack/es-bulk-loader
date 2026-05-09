# es-bulk-loader

A Go CLI for end-to-end loading of JSON documents into Elasticsearch-compatible clusters.

`es-bulk-loader` can manage index creation, settings, mappings, ingest pipelines, enrich policies, transforms,
document loading, and optional enrich execution as one repeatable CLI or CI workflow.

## Features

- Full index lifecycle support: create, replace data, rebuild, and destructive cleanup
- Managed Elasticsearch resources: settings, mappings, ingest pipelines, enrich policies, and transforms
- Keyed JSON definitions for multiple pipelines or policies in a single file
- Automatic default-pipeline selection from the first declared pipeline when settings leave it unset
- Bulk JSON loading with configurable batch sizes and optional `_id` override
- Enrich execution after load, including source-index refresh before policy execution
- Transform upsert/start lifecycle with source-index filtering from config
- Mapping preflight guard that validates declared root field types and `date_detection` before bulk inserts
- Practical input normalization for wrapped settings, nested `index` settings, and `index.*` keys
- Safe-by-default policy deletion, with an explicit destructive override for dependent pipeline cleanup
- Graceful handling for clusters that do not expose enrich APIs

## Quick Start

### Build

```bash
go build -o es-bulk-loader ./cmd/es-bulk-loader
```

### Run Example

#### Docker

```bash
docker run --rm jnovack/es-bulk-loader:latest \
  -url https://localhost:9200 \
  -insecureSkipVerify=true \
  -index my-index \
  -settings settings.json \
  -mappings mappings.json \
  -pipelines pipelines.json \
  -policies policies.json \
  -data data.json \
  -batch 500 \
  -delete \
  -sync-managed \
  -enrich
```

#### Go

```bash
go run ./cmd/es-bulk-loader \
  -url https://localhost:9200 \
  -insecureSkipVerify=true \
  -index my-index \
  -settings settings.json \
  -mappings mappings.json \
  -pipelines pipelines.json \
  -policies policies.json \
  -data data.json \
  -batch 500 \
  -delete \
  -sync-managed \
  -enrich
```

Sample config file: [examples/es-bulk-loader.conf](examples/es-bulk-loader.conf)

## Library Usage

`es-bulk-loader` can now be called directly in-process from Go via [`pkg/loader`](pkg/loader).

Full lifecycle run:

```go
package main

import (
    "context"
    "log"

    loaderpkg "github.com/jnovack/es-bulk-loader/pkg/loader"
)

func main() {
    result, err := loaderpkg.Run(context.Background(), loaderpkg.Options{
        URL:           "http://localhost:9200",
        Index:         "slugs",
        SettingsFile:  "./examples/slugs/settings.json",
        MappingsFile:  "./examples/slugs/mappings.json",
        PipelinesFile: "./examples/slugs/pipelines.json",
        PoliciesFile:  "./examples/slugs/policies.json",
        DataFile:      "./examples/slugs/slugs.json",
        DeleteIndex:   true,
        SyncManaged:   true,
        Enrich: loaderpkg.EnrichOptions{
            Enabled: true,
            All:     true,
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("index=%s processed=%d succeeded=%d failed=%d warnings=%d",
        result.WriteIndex,
        result.DocumentsProcessed,
        result.DocumentsSucceeded,
        result.DocumentsFailed,
        len(result.Warnings),
    )
}
```

Targeted helpers are also available for orchestration:

- `loader.SyncManaged(ctx, opts)`
- `loader.LoadData(ctx, opts)`
- `loader.ExecuteEnrich(ctx, opts)`

Sentinel error classes are exposed for classification:

- `loader.ErrInvalidOptions`
- `loader.ErrIndexOperation`
- `loader.ErrManagedResource`
- `loader.ErrBulkFailure`
- `loader.ErrEnrichExecution`
- `loader.ErrLoaderExecution`

## Testing

Unit tests stay in the default Go test path:

```bash
go test ./...
```

Canonical E2E command (also available as `make test-e2e`) uses the tagged suite entrypoint:

```bash
go test -tags=e2e ./test -run TestEndToEndScenarios
```

Verbose E2E output:

```bash
go test -tags=e2e ./test -run TestEndToEndScenarios -v
```

If you see `build constraints exclude all Go files` for `./test`, the run is missing `-tags=e2e`.
The E2E package is intentionally build-tagged.

Makefile shortcuts:

```bash
make test-unit
make test-e2e
make test-e2e-verbose
```

## Loader Workflow

The loader now separates three concerns:

1. `-nuke` removes the current index and declared managed resources first.
2. One optional data action, exactly one of `-add`, `-flush`, or `-delete`, controls how documents are handled.
3. `-sync-managed` independently creates or updates declared pipelines, policies, and transforms.

When combined, the execution order is:

1. Run `-nuke` first, if requested.
2. Run the selected data action, if any (one of `-add`, `-flush`, or `-delete`).
3. Create required ingest pipelines for index creation when `index.default_pipeline` is configured.
4. Create the index when needed, applying settings and mappings.
5. Bulk load data, if a data action was selected.
6. Update alias pointers, when alias mode creates a new concrete index.
7. Create or update enrich policies during `-sync-managed` once the source index/alias is ready.
8. Refresh the source index and execute enrich policies when `-enrich` is requested.
9. Create or update selected transforms during `-sync-managed`.
10. Start selected transforms after enrich execution (or immediately after transform upsert when enrich is disabled).

That ordering matters. The loader is intentionally opinionated so CI runs and operator workflows stay predictable.

When `-mappings` is provided, the loader derives a pre-bulk validation plan
from the declared mapping and validates effective index mappings before loading
documents:

- Optional `mappings.date_detection` is checked when explicitly declared.
- Declared top-level field `type` mappings are verified against effective index mappings.

This fails early when effective mappings drift from declared mapping intent.

## Command-Line Flags

Settings can be loaded from a configuration file (e.g. `-config es-bulk-loader.conf`), the environment,
or from the command-line.

| Flag | Description |
| --- | --- |
| `-config` | Path to configuration file with settings |
| `-url` | Endpoint URL (e.g., `http://localhost:9200`) |
| `-insecureSkipVerify` | Skip TLS verification for HTTPS |
| `-index` | Target index name (**required**) |
| `-alias` | Treat `-index` as an alias and create timestamped indices as `<alias>-YYYYMMDDHHMMSS` when creating a new index |
| `-keep-last` | With `-alias`, keep only the newest N timestamped indices matching `<alias>-YYYYMMDDHHMMSS` (default: 0, disabled) |
| `-settings` | Optional path to JSON file with index settings |
| `-mappings` | Optional path to JSON file with index mappings |
| `-pipelines` | Optional path to JSON file with one or more ingest pipeline definitions |
| `-policies` | Optional path to JSON file with one or more enrich policy definitions |
| `-transforms` | Optional path to JSON file with one or more transform definitions |
| `-batch` | Number of documents per bulk insert (default: 1000) |
| `-bulk-retry-attempts` | Total bulk API request attempts for retryable failures, including the first attempt (default: 4) |
| `-bulk-retry-backoff-base` | Base backoff duration for retryable bulk failures (default: 500ms) |
| `-bulk-retry-backoff-max` | Maximum backoff duration for retryable bulk failures (default: 5s) |
| `-add` | Append data to an existing index or create the index first if it does not exist |
| `-flush` | Delete all documents from an existing index without deleting the index, then load replacement data |
| `-delete` | Recreate data target before loading data: deletes concrete index in normal mode; rolls alias to a new timestamped index in `-alias` mode |
| `-data` | Path to JSON array of documents to load (**required with** `-add`, `-flush`, or `-delete`) |
| `-sync-managed` | Create or update declared ingest pipelines, enrich policies, and transforms |
| `-nuke` | Delete the current index and declared managed resources first, including dependent pipelines that reference declared enrich policies |
| `-id` | Field to use in the document to override _id (default: not set) |
| `-enrich` | Run enrich policies after the bulk insert; omit value for all or pass a comma-separated list |
| `-user` / `-pass` | Username and password for Basic Auth |
| `-apiKey` | Elasticsearch API key |
| `-level` | Log level filter: `trace`, `debug`, `info`, `warn`, or `error` (default: `info`) |
| `-version` | Print version and exit |

## Behavior Summary

`-add`, `-flush`, and `-delete` are mutually exclusive.

| Flags | Effect |
| --- | --- |
| `-add` | Append data to an existing index, or create the index and load data if it does not exist |
| `-flush` | Remove existing documents, keep the existing index structure, then load replacement data |
| `-delete` | Recreate data target and reload data: concrete index is deleted/recreated in normal mode; alias mode creates a new timestamped index and repoints alias |
| `-sync-managed` | Create or update declared pipelines, policies, and transforms without changing document data by itself |
| `-nuke` | Remove the current index and declared managed resources first; if combined with another action, that action runs afterward |
| `-alias` | Treat `-index` as an alias and use timestamped concrete index names (`<alias>-YYYYMMDDHHMMSS`) when a new index is created |
| `-keep-last` | With `-alias`, prune older timestamped concrete indices after the run and keep only the newest N by parsed timestamp suffix |

Common combinations:

- `-add -sync-managed`: append documents and ensure declared pipelines, policies, and transforms exist
- `-flush -sync-managed`: replace documents, keep settings and mappings, and ensure declared pipelines, policies, and transforms exist
- `-delete -sync-managed`: rebuild the index from scratch and recreate declared pipelines, policies, and transforms
- `-nuke -delete -sync-managed`: force a full teardown first, then rebuild cleanly
- `-nuke`: remove the current index and declared managed resources without loading new data
- `-delete -alias -keep-last 2`: roll to a new timestamped index, repoint alias, then keep only the newest two generations

## Alias Mode

When `-alias` is set, `-index` is interpreted as an Elasticsearch alias name.
The loader uses Elasticsearch terms in logs:

- `alias`: logical name provided by `-index` (for example `cards`)
- `index`: concrete index name (for example `cards-20260319130459`)

Concrete index names created by alias mode always use a numeric suffix:

- `<alias>-YYYYMMDDHHMMSS`

Data-action behavior in alias mode:

- `-delete`: create a new timestamped index, load data, then point the alias to the new index (older timestamped indices can be pruned with `-keep-last`)
- `-flush`: flush documents from current alias target indices, then load replacement data
- `-add`: append to current alias target index; if the alias has no index yet, create a first timestamped index and load data

`-keep-last` only applies when `-alias` is enabled. It parses and sorts timestamped concrete index names, then deletes older ones so only the newest N remain.
In alias mode, `-delete` keeps existing managed resources (pipelines/policies/transforms) and relies on `-sync-managed` to upsert definitions; use `-nuke` when you want destructive managed-resource cleanup.
If `-delete -alias` is run without `-sync-managed`, the loader logs a warning and assumes `-sync-managed` for that run. Add `-sync-managed` explicitly in automation for clarity.
If `-delete -alias` is run without `-keep-last`, the loader logs a warning that no old generations were deleted and storage use can grow over time.

Example progression with `-keep-last 2`:

1. Run 1: keep `cards-20260319130000`
2. Run 2: keep `cards-20260319130000`, `cards-20260319130500`
3. Run 3: create `cards-20260319131000`, then prune oldest so remaining are `cards-20260319130500`, `cards-20260319131000`

## Enrich Policies

Use `-enrich` after a bulk load when enrich policy backing indices need to be rebuilt.

When `-pipelines` and `-policies` are supplied, the loader imports those definitions as part of the run:

- `-sync-managed` creates pipelines, attempts to create declared enrich policies, and upserts matching transforms.
- Declared enrich policies are managed by content hash during `-sync-managed`: each logical policy key is resolved to `<logical>-<sha256[:6]>`.
- If a policy definition is unchanged, the same managed policy name is reused.
- If a policy definition changes, a new managed policy name is created and pipelines are rewritten to reference it.
- Old managed policy versions are garbage-collected when they are unreferenced by any pipeline.
- `-delete` fails loudly if a declared enrich policy is still referenced by another ingest pipeline. This is the safe default because those references may belong to another index workflow.
- `-nuke` keeps deleting through that conflict by finding and deleting ingest pipelines that reference the declared enrich policy. If one of those pipelines is still configured as `index.default_pipeline` on another index, the loader clears that setting on the dependent index before retrying the pipeline delete.
- `-add` and `-flush` affect documents only. They do not modify pipelines, policies, or transforms unless `-sync-managed` is also set.

> [!CAUTION]
> Use `-nuke` carefully. Unlike the normal one-index workflow, it can remove ingest pipelines outside the current index's declared pipeline file and clear `index.default_pipeline` on dependent indices if those pipelines reference a declared enrich policy you are deleting.

```bash
go run cmd/es-bulk-loader/main.go \
  -url https://localhost:9200 \
  -index my-index \
  -sync-managed \
  -data data.json \
  -enrich
```

When `-enrich` is provided without an explicit policy list and `-policies` is also provided, the loader executes the resolved managed policy names declared for that run. If no policy file is provided, it falls back to all enrich policies currently available in the cluster.

Run only specific policies:

```bash
go run cmd/es-bulk-loader/main.go \
  -url https://localhost:9200 \
  -index my-index \
  -sync-managed \
  -data data.json \
  -enrich=policy-a,policy-b
```

Unknown policy names are logged as warnings and skipped. When a policy file is provided, explicit logical policy names are mapped to the resolved managed policy names automatically.
If the cluster does not expose the enrich APIs at all, the loader logs a warning and skips enrich policy create/delete/execute operations instead of aborting the whole load.

## Transforms

When `-transforms` is provided, definitions are read from a keyed JSON object:

- JSON key: transform ID.
- `source_index`: logical source index name that must match the current `-index`.
- `body`: native Elasticsearch transform definition passed directly to the transform APIs.

Only transforms whose `source_index` matches the current `-index` are selected for the run.
During `-sync-managed`, selected transforms are stopped, upserted after data load and alias updates, and then started after enrich execution (or immediately after upsert when enrich is disabled).
During `-nuke`, selected transforms are force-deleted.

Definition file formats are documented in [docs/PIPELINES.md](docs/PIPELINES.md) and [docs/POLICIES.md](docs/POLICIES.md).

When `-pipelines` is supplied during index creation, the loader preserves the JSON key order from the pipeline file. If the settings file does not already define a default pipeline, the first declared pipeline becomes the index default pipeline automatically.

Definition files support variable expansion before they are parsed. `${INDEX}` is populated from the current `-index` value (alias name when `-alias` is enabled), and other placeholders fall back to environment variables when present.

## JSON Formats

### `data.json`

```json
[
  { "id": 1, "name": "Alice" },
  { "id": 2, "name": "Bob" }
]
```

### `settings.json` (optional)

```json
{
  "number_of_shards": 1,
  "number_of_replicas": 1
}
```

Wrapped settings are also accepted:

```json
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1
  }
}
```

Nested `index` settings are also accepted and normalized:

```json
{
  "settings": {
    "index": {
      "number_of_shards": 2,
      "number_of_replicas": 1
    }
  }
}
```

Dotted `index.*` keys are also accepted:

```json
{
  "index.number_of_shards": 2,
  "index.number_of_replicas": 1,
  "index.default_pipeline": "my-default-pipeline"
}
```

All of these shapes are normalized into the create-index request body the loader sends to Elasticsearch.

### `mappings.json` (optional)

```json
{
  "properties": {
    "id":   { "type": "integer" },
    "name": { "type": "text" }
  }
}
```

Wrapped mappings are also accepted:

```json
{
  "mappings": {
    "properties": {
      "id": { "type": "integer" },
      "name": { "type": "text" }
    }
  }
}
```

## 🛡 Requirements

- Go 1.25+
- Elasticsearch 7.x, 8.x, or 9.x (tested with v9 client) or Opensearch (does not support enrich policies)

## 👥 License

MIT License © 2025-2026

## Disclaimer

This project is an independent, open-source tool and is not affiliated with, endorsed by, or
sponsored by Elastic. “Elasticsearch” and “Elastic” are trademarks of Elasticsearch B.V.
