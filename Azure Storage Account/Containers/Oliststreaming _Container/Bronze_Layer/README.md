# Bronze Layer

This folder stores the Bronze Layer tables for the streaming pipeline.

- Source CSV files are read by a Python producer application (Same source of 'batch_data_pipeline'---> Raw_Data), converted into DataFrames, transformed into JSON events, and sent to Azure Event Hubs.
- Databricks then consumes these events using Structured Streaming and stores the ingested data as Delta tables in the Bronze_Layer directory.
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
## Purpose

- Receive streaming events from Azure Event Hubs.
- Persist raw streaming data with minimal transformations.
- Store data in Delta Lake format.
- Provide the source data for downstream Silver Layer transformations.
