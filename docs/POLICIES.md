# Policies

Elasticsearch enrich policies are created one policy at a time at
`/_enrich/policy/<name>`.
This repository supports organizing multiple policy definitions for a single
index in one local JSON file.

## File Shape

Use a top-level JSON object where each key is the logical policy name and each
value is the exact Elasticsearch enrich policy body for that policy.

```json
{
  "my-index-match-policy": {
    "match": {
      "indices": "my-source-index",
      "match_field": "lookup_id",
      "enrich_fields": [
        "name",
        "status"
      ]
    }
  },
  "my-index-range-policy": {
    "range": {
      "indices": "my-range-index",
      "match_field": "start",
      "enrich_fields": [
        "label"
      ]
    }
  }
}
```

## How It Is Used

The loader reads the file and reconciles one policy at a time when
`-sync-managed` is set:

- The logical policy key is resolved to a managed Elasticsearch policy name
  using content hash: `<logical>-<sha256[:6]>`.
- If the resolved managed policy does not exist, it is created.
- Pipelines are rewritten to reference resolved managed policy names.
- Older managed policy versions for the same logical key are deleted when they
  are unreferenced by any pipeline.
- If a policy key is already in managed name form and collides with existing
  data, the loader fails loudly.

The E2E fixture keeps source-index enrich policies in:

- [index1-policies.json](../test/fixtures/index1-policies.json)

The loader, not the shell fixtures, is the primary path under test.

## Execution Behavior

Creating an enrich policy is not the same as executing it.

- `-sync-managed` resolves logical policy names to managed policy names and
  reconciles those policies.
- `-enrich` executes enrich policies after the bulk load completes.
- Before execution, the loader refreshes the source index so the enrich
  backing index is built from visible documents.
- If `-enrich` is passed with no explicit value and `-policies` is also
  supplied, the loader executes the resolved managed policy names declared for
  that run.
- If `-enrich` is passed without `-policies`, the loader falls back to the
  enrich policies currently available in the cluster.
- Unknown policy names are warned and skipped.
- If the cluster does not expose enrich APIs, the loader warns and skips
  enrich create/delete/execute operations instead of aborting the whole run.

## Variable Expansion

Definition files are templated before they are parsed.

- `${INDEX}` expands to the current `-index` value.
- Other `${NAME}` placeholders fall back to environment variables when
  present.
- Only the braced `${NAME}` form is expanded. A bare `$NAME` (for example
  `$HOME`) is left verbatim.
- A `${NAME}` that is in neither the variable map nor the environment fails
  the run with `ErrInvalidOptions` before any cluster change. The error lists
  every unresolved name per file, and every supplied template file is
  checked, including one this run does not otherwise read.

Example:

```json
{
  "my-policy": {
    "match": {
      "indices": "${INDEX}",
      "match_field": "lookup_id",
      "enrich_fields": ["name"]
    }
  }
}
```

## Notes

- Consolidation is local-file organization only. Elasticsearch still requires
  one API call per policy.
- Policy bodies must match the Elasticsearch enrich policy request format.
- `${INDEX}` is expanded by `es-bulk-loader` to the current `-index` value
  before the policy file is parsed.
- Managed policy names use the first 6 hex characters of the SHA-256 hash of
  canonicalized policy JSON.
- A created policy still needs to be executed before its enrich backing index
  exists.
