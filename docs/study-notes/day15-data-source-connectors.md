# Day 15: Data Source Connectors

## Overview
Study notes on various data source connectors in Databricks for Professional Data Engineer certification.

## Key Topics

### 1. JDBC/ODBC Connectors
- Connection properties and authentication
- Query pushdown optimization
- Partitioning strategies for parallel reads

### 2. Cloud Storage Connectors
- Azure Data Lake Storage (ADLS)
- AWS S3
- Google Cloud Storage
- Mount points vs. direct access

### 3. Streaming Connectors
- Apache Kafka integration
- Azure Event Hubs
- AWS Kinesis
- Auto Loader for incremental ingestion

### 4. NoSQL Connectors
- MongoDB
- Cassandra
- Cosmos DB
- DynamoDB

### 5. Delta Sharing
- Open cross-platform sharing
- Security and governance
- Recipients and providers

## Best Practices
- Use connection pooling for JDBC sources
- Implement partition columns for large tables
- Leverage push-down predicates
- Configure appropriate batch sizes
- Use secrets management for credentials

## Practice Questions
1. What are the advantages of query pushdown?
2. How do you configure parallelism for JDBC reads?
3. When should you use Auto Loader vs. streaming sources?

## References
- Databricks Documentation: Data Sources
- Apache Spark SQL Guide
- Delta Lake Connector Documentation
