# ADR 0001: Derive number_of_replicas from cluster node count at index creation

## Status

Accepted

## Date

2026-05-09

## Context

Index settings files (`settings.json`) shipped with consumers of `es-bulk-loader`
hardcode `"number_of_replicas": "0"`. That value is correct for a single-node
development cluster — Elasticsearch cannot place a replica on the same node as the
primary shard, so `0` is the only valid choice.

On a multi-node cluster, `number_of_replicas: 0` means no replica shards exist.
If the node holding the primary shard goes down, the shard becomes unavailable
and the index is red until that node recovers. Consumers have no signal in their
settings files to distinguish the two topologies, so the wrong value ships to
production silently.

Requiring each consumer to manage this setting correctly per environment is
fragile. The loader already holds an authenticated ES client at index-creation
time and can query the cluster topology directly.

## Decision

When creating an index, `es-bulk-loader` will query `GET /_nodes` to count the
number of data nodes in the cluster. It will then compute `number_of_replicas`
as follows:

- **1 node** → `0` replicas (single-node; replicas are not possible)
- **2+ nodes** → `1` replica

This computed value overrides whatever `number_of_replicas` is present in the
consumer's `settings.json`. The override is logged at info level so operators can
observe it.

Consumer-supplied `settings.json` files may omit `number_of_replicas` entirely;
the loader will supply the correct value in all cases.

## Consequences

Positive:

- Index replica count is always topology-correct without consumer configuration.
- Single-node development clusters continue to work without changes to existing
  settings files.
- Multi-node clusters automatically get `1` replica, providing basic shard
  redundancy without operator intervention.
- The `_nodes` call is cheap and happens once per index creation, not per
  document.

Trade-offs:

- Consumers that intentionally set a replica count higher than `1` (e.g. `2` for
  a 5-node cluster) will have that value silently overridden to `1`. This is an
  acceptable limitation for the initial implementation.
- The loader now makes an extra network call per index creation. In practice this
  is negligible given that index creation is an infrequent, administrative
  operation.

## Future Work

The fixed `single-node = 0, multi-node = 1` rule is intentionally conservative.
A follow-on change tracked in `TODO.md` should make the replica count
configurable — either as a loader flag, an environment variable, or a template
variable injected into `settings.json` — so consumers with larger clusters can
request higher redundancy without forking the loader.
