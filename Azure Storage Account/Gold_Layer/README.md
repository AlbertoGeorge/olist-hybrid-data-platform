# Gold Layer

This folder stores the Gold Layer data for the batch pipeline.

After the data has been cleaned, validated, and standardized in the Silver Layer, it is transformed into business-ready datasets optimized for analytics and reporting. The final data is stored as Delta tables in the Gold_Layer directory.

## Processing Steps

- ***Read Silver Tables:*** Load the cleaned and validated data from the Silver Layer.
- ***Apply Business Transformations:*** Perform business rules, calculations, and aggregations required for analytical use cases.
- ***Build Dimensional Models:*** Create dimension and fact tables following a Galaxy Schema (Fact Constellation) design, where multiple fact tables share common dimensions to support different business processes and analytical use cases.
- ***Optimize for Analytics:*** Organize the data for efficient querying, dashboarding, and reporting.
- ***Write Gold Tables:*** Store the final curated datasets as Delta tables in the Gold_Layer directory.

------------------------------------------------------------------------------------------------------------------------------------------------------
## Purpose

The Gold Layer serves as the consumption layer of the data platform. It provides high-quality, business-ready data that can be used by Power BI, SQL queries, dashboards, and other analytics applications to support reporting and decision-making.
