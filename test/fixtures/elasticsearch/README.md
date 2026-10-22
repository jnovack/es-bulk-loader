# Recorded Elasticsearch responses

Observed facts, Elasticsearch 9.3.1 (basic license), recorded 2026-10-22:

- The enrich task (`type: enrich`, `action: policy_execution`) carries an
  optional `task.status.step` next to `task.status.phase`. Exact `step`
  strings observed while `phase` was `RUNNING`: `CreateIndexRequest`,
  `ReindexRequest`, `ForceMergeRequest`, `UpdateSettingsRequest`. Polling
  missed the brief later steps (`RefreshRequest`, alias promotion); those
  names come from `EnrichPolicyRunner.java` at tag `v9.3.1`.
- While the step is `ReindexRequest`, the reindex runs as a child task:
  `GET _tasks?parent_task_id=<task>&detailed=true&group_by=none` returns
  `{"tasks":[...]}` with one task whose `action` is
  `cluster:admin/xpack/enrich/reindex`. Its `status` has `total`, `updated`,
  `created`, `deleted`, `batches`, `version_conflicts`, `noops`, `retries`,
  `throttled_millis`, `requests_per_second` and `throttled_until_millis`.
  After the reindex the list is `{"tasks":[]}`.
- After completion `GET _tasks/<task>` returns HTTP 404
  (`resource_not_found_exception`, no `.tasks` index), not `completed: true`.
- Transform `_stats` (`GET _transform/<id>/_stats`): a running batch
  transform's `checkpointing.next.checkpoint_progress` has only
  `docs_indexed` and `docs_processed`; a continuous transform's also has
  `docs_remaining`, `total_docs` and `percent_complete`. When idle,
  `checkpointing.next` is absent. A batch transform ends `state: stopped`
  with `checkpointing.last.checkpoint: 1`; a continuous one ends
  `state: started`.

## Provenance

- Server: `docker.elastic.co/elasticsearch/elasticsearch:9.3.1`, single node,
  `xpack.security.enabled=false`, `xpack.license.self_generated.type=basic`.
  A disposable container on an ephemeral host port, removed after capture.
- Data: 500,000 small documents (`lookup_id` keyword, `base_value` long,
  `label` keyword) loaded by `es-bulk-loader -delete` into
  `esbl-progress-test-src`.
- Enrich: a match policy `esbl-progress-test-policy` on that index, executed
  with `POST _enrich/policy/<p>/_execute?wait_for_completion=false`, then
  `GET _tasks/<task>` and the child-task list polled with `curl` every
  0.01-0.2s. A step was recorded the first time it was seen.
- Transform: a `terms` pivot on `lookup_id` over the same index, batch and
  continuous (`sync.time` on `base_value`), started with `_start` and polled
  on `_stats`.
- Each `*.json` is the full response body. The sibling `*.headers` file holds
  the HTTP status line and headers from `curl -D`.
- Ids, node names and timestamps are as captured and are not meaningful.

Unit tests that need a response shape not listed above (an unknown step,
malformed or ambiguous child lists, a transform `_stats` with a missing
`total_docs`) use small synthetic inputs, marked as synthetic in the test.
