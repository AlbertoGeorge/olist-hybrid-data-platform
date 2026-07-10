### Catalogs

### (1) Olist__Batch__Data

This catalog contains the managed metadata and table definitions for the Batch Pipeline.

### Schemas

- bronze : Raw ingested Delta tables.
- silver : Cleaned and validated Delta tables.
- gold : Business-ready fact and dimension tables.
- quarantine : Invalid or rejected records that failed validation or business rules.
--------------------------------------------------------------------------------------------------------------------------------------------------------

### (2) Olist_Stream_Data

This catalog contains the managed metadata and table definitions for the Streaming Pipeline.

### Schemas

- bronze - Raw streaming Delta tables.
- silver - Cleaned and validated streaming Delta tables.

Note: The streaming pipeline does not have a separate gold schema
**Both the batch and streaming pipelines share the same Gold Layer, which is served through Olist__Batch__Data.gold to provide a single, consistent business-ready data model.**

--------------------------------------------------------------------------------------------------------------------------------------------------------
### Overview

- The actual data files are stored in Azure Data Lake Storage Gen2 (ADLS).
- To make these files accessible from Databricks, an External Location is created and linked to the storage account.
- Using this external location, Unity Catalog can securely reference and query the Delta tables stored in ADLS while centrally managing metadata, governance, and access control.

--------------------------------------------------------------------------------------------------------------------------------------------------------
### Data Flow

- Azure Storage Account: Stores the actual data files and pipeline-related metadata.
- External Location: Provides secure access from Databricks to the ADLS paths.
- Unity Catalog: Registers and manages the Delta tables that point to the data stored in ADLS.
- Users and Applications: Query the data through the appropriate catalog and schema.

--------------------------------------------------------------------------------------------------------------------------------------------------------
### Purpose

- Centralize metadata management using Unity Catalog.
- Securely access data stored in Azure Data Lake Storage Gen2 through External Locations.
- Organize tables according to the Medallion Architecture.
- Maintain separate catalogs for Batch and Streaming workloads.
- Provide a single, governed Gold Layer for analytics, reporting, and business intelligence across both pipelines.
