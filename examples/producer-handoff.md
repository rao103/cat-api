# Existing producer handoff

No new producer contract is required. Complete and close every Parquet file, finish every Blob upload, then publish your existing PascalCase artifact envelope. Use BlobPath for one file or ShardPaths for the complete set of shards. Preserve the exact RunId, ExtractionToken, metadata and paths on retries.

For a successful no-event extraction publish that same envelope pointing to a valid zero-row Parquet file. The message's existing batch/window/full-snapshot semantics still apply; empty incremental data does not clear prior canonical events. If no message is published, downstream cannot infer that extraction completed.

Production retains its required DataDate. Alarm, Outage and Curtailment do not gain one. The coordinator computes artifact and child IDs; the ingestor needs no artifact manifest or completion flag. Adjustment continues to use its transactional outbox and complete parent/membership snapshots as specified in v11.
