# Bronze Layer

This folder stores the Bronze Layer data for the batch pipeline.

After running the Bronze Layer notebook, the pipeline ingests data from Azure Data Lake Storage (ADLS) using Databricks Auto Loader with Structured Streaming (readStream). The ingested data is then written and stored in Delta Lake format within the Bronze_Layer folder.

## Process

- Read source files from ADLS using Auto Loader (readStream).
- Automatically detect and process newly arrived files.
- Ingest the raw data into the Bronze layer with minimal transformations.
- Store the data as Delta tables in the Bronze_Layer directory.
- Maintain checkpoint information to support incremental processing and fault recovery.

## Purpose

The Bronze Layer serves as the initial ingestion layer, preserving the raw source data in a reliable and scalable Delta Lake format for downstream Silver and Gold transformations.
