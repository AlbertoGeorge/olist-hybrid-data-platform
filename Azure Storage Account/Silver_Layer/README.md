# Silver Layer

This folder stores the Silver Layer data for the batch pipeline.

After reading the data from the Bronze Layer, the pipeline applies a series of data cleansing, validation, and transformation steps to improve data quality before storing the results as Delta tables.

## Processing Steps

- ***Read Bronze Tables:*** Load the ingested data from the Bronze Layer.

- ***Schema Validation:*** Verify that the incoming columns match the expected schema from the source or API.

- ***Data Type Casting:*** Ensure each column has the correct data type, such as Integer, Double, Date, Timestamp, and String.

- ***Remove Duplicates:*** Remove duplicate records based on business keys or full-row duplicates.

- ***Handle Missing Values:*** Remove, fill, or otherwise handle null and missing values according to business rules.

- ***Data Standardization:*** Trim extra spaces from strings, standardize formats and values, and normalize categorical values (for example, Male, M, and male become male).

- ***Data Quality Checks & Business Logic:*** Validate that values fall within acceptable ranges and identify invalid records, such as negative counts where they are not allowed.

- ***Write Quarantine Tables:*** Store records that fail validation or quality checks in the Quarantine directory for further investigation and reprocessing.

- ***Write Silver Tables:*** Store the cleaned and validated data as Delta tables in the Silver_Layer directory.

--------------------------------------------------------------------------------------------------------------------------------------------------------------

## Purpose

The Silver Layer transforms raw Bronze data into clean, validated, and standardized datasets.
It serves as the trusted foundation for downstream analytics, reporting, and Gold Layer transformations.
