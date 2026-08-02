# Databricks Sales Pipeline

🌎 **Language:** English | [Português](README-PT.md)

End-to-end Data Engineering project built using Databricks, Apache Spark, and Delta Lake.

The objective is to design a complete sales data pipeline following the Medallion Architecture pattern:

- Bronze: raw data ingestion, initial validation, and quarantine handling
- Silver: data cleansing, standardization, enrichment, and optimization
- Gold: dimensional model designed for analytics consumption

## Arquitetura

CSV Files
    |
    |
Auto Loader
    |
    |
Bronze Delta Tables
    |
    |
Data Quality Rules
    |
    |
Silver Delta Tables
    |
    |
Gold Layer
    |
    |
Analytics


## Technologies

- Databricks
- Apache Spark
- PySpark
- Delta Lake
- Unity Catalog
- SQL
- Python


## Dataset

The project uses a simulated sales dataset containing:

- Sales transactions
- Products
- Stores
- Customers


## Implementations


## Bronze Layer

Features:

- Streaming ingestion using Databricks Auto Loader
- Incremental data processing
- Checkpoint management
- Delta Table storage
- Invalid data handling through quarantine layers


## Data Quality and Quarantine Pattern

The Bronze layer implements two types of validations:


### Technical Quarantine

Responsible for capturing structural data issues.

Examples:

- Unexpected schema changes
- Unknown fields
- Data captured in the `_rescued_data` column


### Business Quarantine

Responsible for capturing records that have a valid structure but violate business rules.

Examples:

- Non-existing products
- Invalid stores
- Non-existing customers
- Negative values
- Missing mandatory fields


Invalid records are isolated without stopping pipeline execution, ensuring data traceability and allowing further analysis.


## Silver Layer

Implemented features:

- Data type standardization
- Data cleansing
- Deduplication
- Business rule application
- Data enrichment
- Delta optimization using `OPTIMIZE`


### Performance Optimization

Silver tables are periodically optimized using the Delta Lake `OPTIMIZE` command.

This process reduces small files and improves read performance for downstream analytical workloads.


## Gold Layer

Dimensional model implemented:


### Dimensions

- dim_product
- dim_store
- dim_customer
- dim_sales_status


### Fact

- fact_sales


## Consumption Strategy

The Gold layer was implemented using Materialized Views, providing:

- Incremental data refresh
- Reduced computational cost for analytical queries
- Improved performance for dashboards and reporting
- Processing only changed data from lower layers


Materialized Views are automatically refreshed as new data becomes available in the Bronze and Silver layers.


## Data Quality

Implemented validation rules:


| Rule | Action |
|-|-|
| Negative values | Quarantine |
| Missing mandatory fields | Quarantine |
| Unexpected schema fields captured by `_rescued_data` | Quarantine |


## How to Run

1. Import notebooks into Databricks
2. Upload the dataset files
3. Configure Catalog and Schema using Unity Catalog
4. Execute the pipeline


## Key Concepts Applied

- Medallion Architecture
- Delta Lake
- Incremental Data Processing
- Data Quality Framework
- Quarantine Pattern
- Dimensional Modeling
- Delta Lake Optimization with OPTIMIZE
- Materialized Views


## Autor

Felipe Porfirio

Senior Data Engineer