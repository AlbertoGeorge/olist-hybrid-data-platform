# Quarantine

This folder stores the Quarantine Tables for the batch pipeline.

Records that fail schema validation, data quality checks, or business rules during Silver Layer processing are written to this location instead of being loaded into the Silver tables. The data is stored in Delta table format to support auditing, troubleshooting, and reprocessing.

## Purpose

- Store invalid, corrupted, or rejected records.
- Prevent bad data from entering the trusted Silver Layer.
- Provide visibility into data quality issues.
- Support debugging, auditing, and root-cause analysis.
- Allow failed records to be corrected and reprocessed later.

------------------------------------------------------------------------------------------------------------------------------------------------------

## Typical Reasons for Quarantine

- Schema mismatches.
- Invalid or missing required values.
- Incorrect data types.
- Values outside acceptable business ranges.
- Records that violate data quality or business rules.

------------------------------------------------------------------------------------------------------------------------------------------------------
