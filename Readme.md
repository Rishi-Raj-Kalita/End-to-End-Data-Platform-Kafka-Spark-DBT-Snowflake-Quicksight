# Problem Statement

## Technical Context

The platform must support a **multi-tenant SaaS architecture** using **Medallion Architecture** (Bronze-Silver-Gold) for automated expense tracking, split calculation, and settlement reconciliation across distributed customer networks. The system processes semi-structured financial transaction data (CSV with embedded JSON) through a data lake pipeline, maintains dimension integrity via CDC from Aurora MySQL, and provides real-time analytical capabilities in Snowflake while ensuring complete tenant data isolation.

## **Functional Requirements**

| Requirement | Technical Implementation |
|-------------|-------------------------|
| **Medallion Processing** | Bronze (raw) → Silver (validated) → Gold (business) layer processing with complete lineage |
| **JSON Parsing** | AWS Glue DynamicFrame JSON parsing with error handling and schema validation |
| **CDC Integration** | Aurora MySQL CDC → S3 Bronze → Silver SCD Type 2 processing |
| **External Tables** | Snowflake external tables pointing to S3 Silver layer with auto-refresh |
| **DBT Transformations** | Incremental DBT models for fact generation with surrogate key management |
| **Tenant Isolation** | Multi-tenant data isolation across S3, Glue, and Snowflake layers |
| **Data Quality** | Automated quality scoring and monitoring across all pipeline stages |


## Architecture
![Architecture Diag.](./Architecture/DataPlatform.png "Architecture Diag.")

**Data Platform:** Real-time CDC from SQL Server via Debezium → Kafka topics partitioned by tenant
→ S3 Bronze layer with Apache Iceberg for schema evolution → AWS Glue ETL for JSON parsing and
SCD processing → S3 Silver/Gold layers → Snowflake external tables for zero-copy analytics → DBT
transformations for business logic and incremental fact generation → QuickSight dashboards with real-
time debt tracking and settlement monitoring. Apache Airflow orchestrates end-to-end workflow management
with dependency resolution, SLA monitoring, and automated retry mechanisms across all pipeline stages.

– **Technologies:** Data Ingestion Streaming: SQL Server, Debezium CDC, Apache Kafka, Apache Spark
Data Lake Storage: Amazon S3, Apache Iceberg table format ETL Processing: AWS Glue, Apache Spark, dbt
transformations Analytics Warehousing: Snowflake MPP, External Tables Visualization: Amazon QuickSight
dashboards Orchestration DevOps: Apache Airflow, Github Actions, CI/CD pipelines
