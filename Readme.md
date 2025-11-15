# Problem Statement

## Technical Context

The platform must support a **multi-tenant SaaS architecture** using **Medallion Architecture** (Bronze-Silver-Gold) for automated expense tracking, split calculation, and settlement reconciliation across distributed customer networks. The system processes semi-structured financial transaction data (CSV with embedded JSON) through a data lake pipeline, maintains dimension integrity via CDC from Aurora MySQL, and provides real-time analytical capabilities in Snowflake while ensuring complete tenant data isolation.

## System Challenges

### 1. **Medallion Architecture Data Pipeline Complexity**
- **Bronze Layer Processing**: Ingest heterogeneous sources (CSV transactions, Aurora CDC, settlement files) into S3 raw layer with metadata enrichment
- **Silver Layer Validation**: Implement comprehensive data quality scoring, JSON parsing, and SCD Type 2 processing in AWS Glue
- **Gold Layer Analytics**: Transform Silver layer data via Snowflake external tables and dbt for business-ready fact tables
- **Cross-Layer Lineage**: Track data transformations across Bronze → Silver → Gold with complete audit trails

```
Data Flow: CSV/Aurora → S3 Bronze → Glue ETL → S3 Silver → Snowflake External Tables → dbt → Gold Facts
```

### 2. **JSON Allocation Processing in AWS Glue**
- **Variable Schema Parsing**: Handle dynamic JSON structures in `Others_Involved_JSON` column with 2-50 participants per transaction
- **Glue DynamicFrame Challenges**: Parse nested JSON arrays while maintaining data lineage and error handling
- **Percentage Validation Logic**: Implement constraint validation (`SUM(others[].share) ≤ 100`) with automated payer share calculation
- **Allocation Normalization**: Flatten JSON arrays into structured `SILVER_ALLOCATIONS_NORMALIZED` table

```python
# Glue ETL JSON processing challenge
transaction_json = '{"others":[{"customer_id":"ALICE","share":50.0},{"customer_id":"BOB","share":25.0}]}'
# Must parse → individual allocation records with percentage validation
```

### 3. **Aurora MySQL CDC Integration**
- **Real-time Dimension Sync**: Process Aurora CDC events for customer, account, vendor, category, and settlement method changes
- **SCD Type 2 Implementation**: Maintain historical dimension records with effective/end dates in Silver layer
- **CDC Event Processing**: Handle INSERT/UPDATE/DELETE operations from Aurora binlog via AWS DMS or custom CDC

### 4. **Snowflake External Tables Architecture**
- **S3-Snowflake Integration**: Configure external tables pointing to S3 Silver layer without data duplication
- **Auto-refresh Challenges**: Implement S3 event notifications to trigger Snowflake external table metadata refresh
- **Query Performance**: Optimize external table partitioning and file formats for analytical workloads
- **Cost Management**: Balance between external table query costs vs. data loading costs

```sql
-- External table challenge
CREATE EXTERNAL TABLE EXT_SILVER_TRANSACTIONS (
    transaction_id VARCHAR,
    others_involved_json VARIANT,  -- JSON parsing in Snowflake
    ...
) LOCATION = 's3://bucket/silver/transactions/'
AUTO_REFRESH = TRUE;
```

### 5. **Multi-Tenant Data Isolation Across Layers**
- **Bronze Layer Isolation**: Partition S3 raw data by `tenant_id` with strict access controls
- **Silver Layer Processing**: Ensure Glue ETL jobs process tenant data in isolation with no cross-contamination
- **Snowflake RLS**: Implement row-level security policies on external tables and gold facts
- **dbt Macro Isolation**: Create tenant-filtering macros for all dbt models to prevent cross-tenant queries

```sql
-- Multi-tenant challenge across all layers
S3: s3://bucket/bronze/transactions/tenant_id=TENANT_A/
Glue: WHERE tenant_id = 'TENANT_A' in all transformations
Snowflake: CREATE ROW ACCESS POLICY tenant_isolation AS (tenant_id = CURRENT_USER())
```

### 6. **dbt Transformation Complexity**
- **Incremental Processing**: Implement incremental dbt models for large fact tables (millions of records)
- **Surrogate Key Management**: Generate and maintain surrogate keys across dimension lookups
- **Business Logic Implementation**: Complex settlement logic, debt offsetting, and ledger generation in dbt
- **Data Quality Testing**: Comprehensive dbt tests for referential integrity, balance reconciliation, percentage validation

```sql
-- dbt challenge: Incremental fact processing
{{ config(materialized='incremental', unique_key='allocation_sk') }}
SELECT 
    {{ dbt_utils.surrogate_key(['transaction_id', 'beneficiary_customer_id']) }} as allocation_sk,
    -- Complex business logic for settlement status updates
FROM {{ ref('ext_silver_allocations') }}
{% if is_incremental() %}
WHERE processed_at > (SELECT MAX(dbt_processed_at) FROM {{ this }})
{% endif %}
```

### 7. **Double-Entry Ledger Generation**
- **Atomic Ledger Creation**: Generate balanced ledger entries (debits = credits) for each transaction
- **Multi-party Allocation Logic**: Create N+1 ledger entries (1 payer debit + N beneficiary credits) per transaction
- **Settlement Processing**: Update ledger with settlement entries and maintain running balances
- **Reconciliation Controls**: Implement automated balance checks and exception handling

```sql
-- Ledger generation challenge
Transaction: ₹300, JOHN pays, ALICE 50%, BOB 30%, JOHN 20%
Ledger Entries Required:
  JOHN   DR ₹300 (payment)
  ALICE  CR ₹150 (allocation)  
  BOB    CR ₹90  (allocation)
  JOHN   CR ₹60  (self-allocation)
  SUM = ₹0 (must balance)
```

### 8. **Data Quality and Monitoring Implementation**
- **Cross-Layer Quality Scoring**: Implement data quality metrics from Bronze → Silver → Gold
- **Glue Job Monitoring**: Track ETL job performance, error rates, and data volume metrics
- **dbt Test Automation**: Automated testing for business rules, referential integrity, balance validation
- **Lineage Tracking**: Complete data lineage from source CSV to final QuickSight dashboards

## Data Processing Complexity

### **Input Data Characteristics**
| Layer | Volume | Processing | Technology | Challenges |
|--------|--------|------------|------------|------------|
| **Bronze** | 100K+ records/day | Batch ingestion | S3 + Glue | CDC processing, deduplication |
| **Silver** | 500K+ records/day | Validation & normalization | Glue ETL | JSON parsing, SCD Type 2 |
| **Gold** | 2M+ records/day | Business transformation | dbt + Snowflake | Complex joins, incremental processing |

### **Processing Pipeline Constraints**
```
Bronze Processing: 15-minute Glue job runtime limit per batch
Silver Processing: JSON parsing performance (50 participants max per transaction)
Gold Processing: dbt incremental runs must complete within 30 minutes
External Tables: S3 metadata refresh latency (5-10 minutes)
```

### **Multi-Source Integration**
```
Source Complexity:
├── CSV Transactions (semi-structured with JSON)
├── Aurora CDC Events (structured dimension changes)  
├── Settlement Files (manual reconciliation data)
└── External API Data (Splitwise/GPay imports)
```

## Architectural Requirements

### **Functional Requirements**

| Requirement | Technical Implementation |
|-------------|-------------------------|
| **FR-1: Medallion Processing** | Bronze (raw) → Silver (validated) → Gold (business) layer processing with complete lineage |
| **FR-2: JSON Parsing** | AWS Glue DynamicFrame JSON parsing with error handling and schema validation |
| **FR-3: CDC Integration** | Aurora MySQL CDC → S3 Bronze → Silver SCD Type 2 processing |
| **FR-4: External Tables** | Snowflake external tables pointing to S3 Silver layer with auto-refresh |
| **FR-5: dbt Transformations** | Incremental dbt models for fact generation with surrogate key management |
| **FR-6: Tenant Isolation** | Multi-tenant data isolation across S3, Glue, and Snowflake layers |
| **FR-7: Data Quality** | Automated quality scoring and monitoring across all pipeline stages |

