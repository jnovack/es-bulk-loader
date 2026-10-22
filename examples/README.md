# Examples

## Slugs Fixture

The repository includes a concrete example under [/examples/slugs](slugs). It exercises:

- wrapped settings with nested `settings.index.*`
- a keyed pipeline file
- a keyed enrich policy file using `${INDEX}`
- bulk loading with `-id sanitized`
- enrich execution after the load

Run it with Docker:

```bash
docker run --rm \
  -v "$PWD/examples/slugs:/data:ro" \
  es-bulk-loader:dev \
  -url https://localhost:9200 \
  -insecureSkipVerify=true \
  -index slugs \
  -settings /data/settings.json \
  -mappings /data/mappings.json \
  -pipelines /data/pipelines.json \
  -policies /data/policies.json \
  -data /data/slugs.json \
  -id sanitized \
  -delete \
  -enrich
```

After the run:

- the `slugs` index exists with the configured mappings and normalized settings
- the first declared pipeline becomes the index default pipeline when settings
  do not already set one
- source documents have `inclusion_percent` populated by the ingest pipeline
- the `slugs-by-name` enrich policy is created and executed

### `settings.conf` (optional)

```ini
url=http://localhost:9200
insecureSkipVerify=true
index=e2e-source-index
settings=test/fixtures/index1-settings.json
mappings=test/fixtures/index1-mappings.json
pipelines=test/fixtures/index1-pipelines.json
policies=test/fixtures/index1-policies.json
data=test/fixtures/index1-data.json
delete=true
enrich=e2e-source-policy
```
