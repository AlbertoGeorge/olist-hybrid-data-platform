# Streaming Checkpoints

This folder stores the Structured Streaming checkpoint data used by Databricks streaming jobs.

## Purpose

- Track the progress of streaming queries.
- Support fault tolerance and recovery.
- Resume processing from the last successfully processed offset.
- Prevent reprocessing of previously processed data.

------------------------------------------------------------------------------------------------------------------------------------------------------

# Auto Loader Schemas

This folder stores the Databricks Auto Loader schema metadata.

## Purpose

- Store inferred schemas for incoming files.
- Support schema evolution when new columns are detected.
- Maintain consistent schema management across ingestion runs.
- Enable reliable incremental ingestion with Auto Loader.

