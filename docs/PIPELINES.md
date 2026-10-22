# Pipelines

Elasticsearch ingest pipelines are created one pipeline at a time at `/_ingest/pipeline/<name>`.
This repository supports organizing multiple pipeline definitions for a
single index in one local JSON file.

## File Shape

Use a top-level JSON object where each key is the pipeline name and each value
is the exact Elasticsearch pipeline body for that pipeline.
The loader preserves the key order in this file. When it creates an index and
the settings do not already define a default pipeline, it uses the first
pipeline in the file as the default pipeline for that index.

```json
{
  "my-index-default": {
    "description": "Default pipeline for my index",
    "processors": [
      {
        "set": {
          "field": "ingested_by",
          "value": "es-bulk-loader"
        }
      }
    ]
  },
  "my-index-secondary": {
    "description": "Optional secondary pipeline",
    "processors": [
      {
        "remove": {
          "field": "debug"
        }
      }
    ]
  }
}
```

## How It Is Used

The loader reads the file, preserves the JSON key order, and sends one
`PUT /_ingest/pipeline/<name>` request per entry when `-sync-managed` is set.

The E2E fixture keeps pipeline definitions per index:

- [index1-pipelines.json](../test/fixtures/index1-pipelines.json)
- [index2-pipelines.json](../test/fixtures/index2-pipelines.json)

The loader, not the shell fixtures, is the primary path under test.

## Default Pipeline Behavior

If you pass `-pipelines` during index creation:

- The first pipeline in the JSON object is treated as the default candidate.
- If your settings already define a default pipeline, the loader leaves that
  value alone.
- If your settings do not define one, the loader injects the first pipeline as
  the index default pipeline.

Settings files may express that default in several accepted shapes, including:

```json
{
  "default_pipeline": "my-index-default"
}
```

```json
{
  "index.default_pipeline": "my-index-default"
}
```

```json
{
  "settings": {
    "index": {
      "default_pipeline": "my-index-default"
    }
  }
}
```

## Notes

- Consolidation is local-file organization only. Elasticsearch still requires
  one API call per pipeline.
- The first pipeline in the JSON object becomes the index default pipeline
  during index creation unless the settings file already sets one.
- Keeping definitions per index is useful when one index owns several pipelines.
- Each value must already match the Elasticsearch ingest pipeline request body format.
