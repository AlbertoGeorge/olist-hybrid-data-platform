# Raw Data

- This folder represents the Raw (Landing) Layer in the Azure Storage Account.
- It contains 9 subfolders, each named after one of the source datasets.
- This structure ensures that all files belonging to the same dataset are stored together.

------------------------------------------------------------------------------------------------------------------------------
### Purpose

If the company receives new data files in the future, each file should be uploaded to the folder that matches its dataset name.
For example:
             - customers.csv → Raw__Data/customers/
             - orders.csv → Raw__Data/orders/
             - products.csv → Raw__Data/products/

------------------------------------------------------------------------------------------------------------------------------
### Benefits

(1) Organizes raw data in a clear and consistent way.
(2) Makes data ingestion and maintenance easier.
(3) Supports incremental and repeated file uploads.
(4) Ensures that all files for the same dataset are stored in one location.
(5) Provides a scalable landing area for ETL/ELT and streaming pipelines.
