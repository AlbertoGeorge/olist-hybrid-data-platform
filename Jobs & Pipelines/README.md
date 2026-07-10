## Batch Pipeline Job

- Job Name: `Olist__ETL__Job ( Friday at 12:00 PM )`
- Pipeline Type: Batch ETL
- Trigger Type: Scheduled Trigger
- Schedule: Every Friday at 12:00 PM
- Retry Policy: Retry up to 2 times if a task fails.
--------------------------------------------------------------------------------------------------------------------------------------------------------
### Workflow

The job executes the following notebooks in sequence:

- Bronze Layer : Ingest source files from ADLS using Databricks Auto Loader and store them as Delta tables.
- Silver Layer : Validate, clean, standardize, and transform the Bronze data while writing invalid records to Quarantine tables.
- Gold Layer : Create business-ready datasets and dimensional models using a Galaxy Schema (Fact Constellation) design.

--------------------------------------------------------------------------------------------------------------------------------------------------------

### Task Dependencies

- Silver Layer runs after the successful completion of Bronze Layer.
- Gold Layer runs after the successful completion of Silver Layer.
-  Execution Order: Bronze → Silver → Gold.

--------------------------------------------------------------------------------------------------------------------------------------------------------
### Notifications

The job is configured to send notifications for important events, including:

- Job start
- Job success.
- Job failure.
- Task failure after all retry attempts are exhausted.
--------------------------------------------------------------------------------------------------------------------------------------------------------

### Purpose

- Automate the execution of the batch ETL pipeline.
- Ensure tasks run in the correct dependency order.
- Provide fault tolerance through retry attempts.
- Notify stakeholders about pipeline execution results.
- Deliver consistent and reliable datasets for analytics and reporting.
