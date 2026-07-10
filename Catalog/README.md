# Unity Catalog ( Olist__Batch__Data )

This catalog contains the managed metadata and table definitions for the Olist Batch Data Platform.

## Overview

- The pipeline stores the actual data files in Azure Data Lake Storage Gen2 (ADLS).
- To make these files accessible through Databricks, ---->> An External Location is created and linked to the storage account.

Using this external location, the catalog Olist__Batch__Data can reference and query the Delta tables directly from ADLS while managing their metadata within Unity Catalog.

--------------------------------------------------------------------------------------------------------------------------------------------------------
## Data Flow

- Azure Storage Account: Stores the real data files and pipeline-related metadata.

- External Location: Provides secure access from Databricks to the ADLS paths.

- Unity Catalog: Registers and manages the tables that point to the data stored in ADLS.
--------------------------------------------------------------------------------------------------------------------------------------------------------

## Schemas

The catalog contains schemas that mirror the Medallion Architecture layers:

- bronze : Raw ingested Delta tables.
- silver : Cleaned and validated Delta tables.
- gold : Business-ready fact and dimension tables.
- quarantine : Invalid or rejected records that failed validation or business rules.
--------------------------------------------------------------------------------------------------------------------------------------------------------
## Purpose

Centralize metadata management using Unity Catalog.

Securely access data stored in Azure Data Lake Storage Gen2 through External Locations.

Organize tables into Bronze, Silver, Gold, and Quarantine schemas.

Provide a consistent and governed interface for querying and managing the Olist datasets.
