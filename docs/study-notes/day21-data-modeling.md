# Day 21: Data Modeling

## Overview
Study notes on data modeling techniques, best practices, and patterns for Databricks Professional certification.

## Key Concepts

### 1. Dimensional Modeling
- **Star Schema**: Fact tables surrounded by dimension tables
- **Snowflake Schema**: Normalized dimension tables
- **Fact Tables**: Measurable, quantitative data
- **Dimension Tables**: Descriptive attributes
- **Slowly Changing Dimensions (SCD)**: Managing historical changes

### 2. Data Vault Modeling
- **Hubs**: Business keys and metadata
- **Links**: Relationships between hubs
- **Satellites**: Descriptive attributes with history
- Supports auditability and scalability
- Good for enterprise data warehouses

### 3. Medallion Architecture
- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Cleaned and conformed data
- **Gold Layer**: Business-level aggregates
- Common in data lakehouse implementations

### 4. Data Normalization
- **1NF**: Atomic values, no repeating groups
- **2NF**: No partial dependencies
- **3NF**: No transitive dependencies
- **BCNF**: Boyce-Codd Normal Form
- Trade-offs between normalization and query performance

## Star Schema Design

### Fact Table Characteristics
- Contains foreign keys to dimension tables
- Contains measures/metrics (numeric values)
- Grain: Level of detail in the fact table
- Additive, semi-additive, or non-additive facts

### Dimension Table Characteristics
- Contains descriptive attributes
- Typically denormalized
- Contains surrogate keys
- May include hierarchies

### Example: Sales Data Model

```sql
-- Fact Table
CREATE TABLE fact_sales (
    sale_id BIGINT,
    date_key INT,
    product_key INT,
    customer_key INT,
    store_key INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2)
) USING DELTA;

-- Dimension Tables
CREATE TABLE dim_date (
    date_key INT,
    date DATE,
    day_of_week STRING,
    month STRING,
    quarter STRING,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
) USING DELTA;

CREATE TABLE dim_product (
    product_key INT,
    product_id STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    supplier STRING
) USING DELTA;

CREATE TABLE dim_customer (
    customer_key INT,
    customer_id STRING,
    customer_name STRING,
    email STRING,
    segment STRING,
    region STRING,
    country STRING
) USING DELTA;
```

## Slowly Changing Dimensions

### Type 1: Overwrite
- Simply update the existing record
- No history preserved
- Simplest approach

```sql
UPDATE dim_customer
SET email = 'newemail@example.com'
WHERE customer_id = '12345';
```

### Type 2: Add New Row
- Insert new row with updated values
- Preserve full history
- Use effective dates or version flags

```sql
CREATE TABLE dim_customer_scd2 (
    customer_key INT,
    customer_id STRING,
    customer_name STRING,
    email STRING,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
) USING DELTA;

-- When updating:
-- 1. Set is_current = FALSE and end_date for old record
-- 2. Insert new record with is_current = TRUE
```

### Type 3: Add New Column
- Add column to track previous value
- Limited history (typically one previous value)

```sql
ALTER TABLE dim_customer ADD COLUMN previous_email STRING;
```

## Delta Lake Table Design

### Partitioning Strategy
```sql
-- Partition by date for time-series data
CREATE TABLE events (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    event_date DATE,
    event_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date);

-- Multi-level partitioning
CREATE TABLE sales (
    sale_id BIGINT,
    amount DECIMAL(10,2),
    region STRING,
    sale_date DATE
)
USING DELTA
PARTITIONED BY (region, sale_date);
```

### Z-Ordering for Performance
```sql
-- Optimize for queries on multiple columns
OPTIMIZE events
ZORDER BY (user_id, event_type);
```

## Data Modeling Best Practices

### 1. Choose Appropriate Grain
- Define the lowest level of detail
- Balance between detail and performance
- Document grain explicitly

### 2. Use Surrogate Keys
- Integer keys for better performance
- Protect against source system changes
- Enable SCD implementations

### 3. Design for Query Patterns
- Understand reporting requirements
- Denormalize for read performance
- Pre-aggregate when beneficial

### 4. Partition Wisely
- Partition on frequently filtered columns
- Avoid over-partitioning (too many small files)
- Consider cardinality of partition columns
- Typical: partition by date or region

### 5. Implement Data Quality
- Define constraints and validations
- Use Delta constraints
- Implement data quality checks

```sql
-- Add constraints
ALTER TABLE customers ADD CONSTRAINT valid_email CHECK (email LIKE '%@%.%');

ALTER TABLE orders ADD CONSTRAINT positive_amount CHECK (amount > 0);
```

## Medallion Architecture Implementation

### Bronze Layer (Raw)
```python
# Ingest raw data
df = spark.read.json("s3://raw-data/events/")
df.write.format("delta").mode("append").save("/mnt/bronze/events")
```

### Silver Layer (Cleansed)
```python
# Clean and standardize
from pyspark.sql.functions import col, to_timestamp, regexp_replace

bronze_df = spark.read.format("delta").load("/mnt/bronze/events")

silver_df = bronze_df \
    .withColumn("email", regexp_replace(col("email"), "\\s+", "")) \
    .withColumn("timestamp", to_timestamp(col("event_time"))) \
    .filter(col("user_id").isNotNull()) \
    .dropDuplicates(["event_id"])

silver_df.write.format("delta").mode("append").save("/mnt/silver/events")
```

### Gold Layer (Aggregated)
```python
# Create business-level aggregates
from pyspark.sql.functions import count, sum, avg

silver_df = spark.read.format("delta").load("/mnt/silver/events")

gold_df = silver_df.groupBy("user_id", "event_date") \
    .agg(
        count("*").alias("event_count"),
        count(col("purchase_amount")).alias("purchase_count"),
        sum("purchase_amount").alias("total_revenue")
    )

gold_df.write.format("delta").mode("overwrite").save("/mnt/gold/user_daily_metrics")
```

## Common Anti-Patterns to Avoid

### 1. Over-Normalization in Analytics
- Too many joins hurt query performance
- Denormalize for read-heavy workloads

### 2. Lack of Documentation
- Document table purposes and grain
- Maintain data dictionaries
- Use comments and tags

### 3. Ignoring Data Lineage
- Track data sources and transformations
- Use Unity Catalog for lineage

### 4. Poor Partition Design
- Too many partitions (small files)
- Partitioning on high-cardinality columns
- Not partitioning time-series data

### 5. Missing Data Quality Checks
- No validation rules
- No completeness checks
- No consistency verification

## Practice Questions
1. What are the differences between star schema and snowflake schema?
2. When should you use SCD Type 2 vs. Type 1?
3. How do you choose partition columns for a Delta table?
4. What is the medallion architecture and its benefits?
5. How do you implement data quality constraints in Delta Lake?
6. What are surrogate keys and why use them?

## References
- Data Warehouse Toolkit by Kimball
- Delta Lake Best Practices
- Databricks Medallion Architecture Guide
- Unity Catalog Data Modeling Guide
