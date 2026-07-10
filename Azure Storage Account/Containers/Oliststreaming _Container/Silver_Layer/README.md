# Silver Layer

This folder stores the Silver Layer tables for the streaming pipeline.

Data read from the Bronze Layer is validated, cleaned, standardized, and transformed before being written as Delta tables.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

## Processing Steps

- Read data from the Bronze Layer.
- Validate the schema.
- Cast columns to the appropriate data types.
- Remove duplicate records.
- Handle missing values.
- Standardize and clean data values.
- Apply data quality checks and business rules.
- Write the validated data to Silver_Layer.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

## Purpose

- Provide clean and trusted streaming datasets.
- Prepare data for downstream analytics and Gold Layer processing.
- Store the results in Delta Lake format.
