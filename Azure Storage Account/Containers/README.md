## Azure Storage Account
Storage Account Name: olistprojectdatalake
The storage account contains two containers that support both batch and streaming data processing.

------------------------------------------------------------------------------------------------------------------------------------------------------

### 1. Container: Olist (Batch Data)

This container stores data for the batch processing pipeline and includes the following folders:

- Raw_Data – Landing area for source files.
- Bronze_Layer – Raw ingested data.
- Silver_Layer – Cleaned and transformed data.
- Gold_Layer – Business-ready and analytics-ready data.
- Quarantine – Invalid or rejected records.
- checkpoints – Checkpoint data used for pipeline processing and recovery.

------------------------------------------------------------------------------------------------------------------------------------------------------
### 2. Container: oliststreaming (Streaming Data)

This container stores data for the streaming pipeline and includes the following folders:

- Bronze_Layer – Streamed raw data.
- Silver_Layer – Cleaned and validated streaming data.
- checkpoints – Streaming checkpoint data for fault tolerance and recovery.

### Note

The **Raw_Data** folder ------> in the **Olist container** serves as the single source of incoming files for both the *(Batch_Data)* and *(Streaming_Data)* pipelines.
