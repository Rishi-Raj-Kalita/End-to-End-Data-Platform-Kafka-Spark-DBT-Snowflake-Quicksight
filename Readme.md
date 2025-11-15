# Problem Statement

## Technical Context

The platform must support a **multi-tenant SaaS architecture** using **Medallion Architecture** (Bronze-Silver-Gold) for automated expense tracking, split calculation, and settlement reconciliation across distributed customer networks. The system processes semi-structured financial transaction data (CSV with embedded JSON) through a data lake pipeline, maintains dimension integrity via CDC from Aurora MySQL, and provides real-time analytical capabilities in Snowflake while ensuring complete tenant data isolation.

## **Functional Requirements**

| Requirement | Technical Implementation |
|-------------|-------------------------|
| **FR-1: Medallion Processing** | Bronze (raw) → Silver (validated) → Gold (business) layer processing with complete lineage |
| **FR-2: JSON Parsing** | AWS Glue DynamicFrame JSON parsing with error handling and schema validation |
| **FR-3: CDC Integration** | Aurora MySQL CDC → S3 Bronze → Silver SCD Type 2 processing |
| **FR-4: External Tables** | Snowflake external tables pointing to S3 Silver layer with auto-refresh |
| **FR-5: dbt Transformations** | Incremental dbt models for fact generation with surrogate key management |
| **FR-6: Tenant Isolation** | Multi-tenant data isolation across S3, Glue, and Snowflake layers |
| **FR-7: Data Quality** | Automated quality scoring and monitoring across all pipeline stages |


## Architecture
![Architecture Diag.](./Architecture/DataPlatform.png "Architecture Diag.")
