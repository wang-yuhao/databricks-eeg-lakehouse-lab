# Day 16: Delta Sharing

## Overview
Study notes on Delta Sharing for cross-platform data sharing in Databricks Professional certification.

## Key Concepts

### 1. Delta Sharing Fundamentals
- Open protocol for secure data sharing
- Cross-platform and cross-cloud compatibility
- No data duplication required
- REST API-based architecture

### 2. Core Components
- **Sharing Server**: Hosts shared data
- **Provider**: Organization sharing data
- **Recipient**: Organization receiving shared data
- **Share**: Collection of tables to share
- **Schema**: Logical grouping within a share

### 3. Recipient Types
- Databricks-to-Databricks sharing
- Open sharing (non-Databricks platforms)
- Delta Sharing connectors for various tools

### 4. Security Features
- Bearer token authentication
- Fine-grained access control
- Audit logging
- Data encryption in transit
- No direct access to storage

### 5. Federation
- Unity Catalog integration
- Lakehouse Federation capabilities
- Cross-workspace sharing
- Multi-cloud support

## Delta Sharing Workflow

1. **Provider creates a share**
   - Define tables to share
   - Configure access permissions
   
2. **Add recipients**
   - Generate activation links
   - Distribute credentials securely
   
3. **Recipients access data**
   - Use Delta Sharing client
   - Query shared tables
   - Read-only access

## Best Practices
- Use Unity Catalog for managing shares
- Implement row-level and column-level security
- Monitor access through audit logs
- Rotate credentials periodically
- Document shared datasets clearly
- Version shared schemas carefully

## Commands & Syntax

```sql
-- Create a share
CREATE SHARE IF NOT EXISTS my_share;

-- Add table to share
ALTER SHARE my_share ADD TABLE catalog.schema.table;

-- Create recipient
CREATE RECIPIENT my_recipient;

-- Grant share to recipient
GRANT SELECT ON SHARE my_share TO RECIPIENT my_recipient;
```

## Practice Questions
1. What are the differences between Databricks-to-Databricks and open sharing?
2. How does Delta Sharing ensure data security without copying data?
3. What authentication mechanisms does Delta Sharing support?
4. How can you implement column-level filtering in Delta Sharing?

## References
- Delta Sharing Protocol Specification
- Unity Catalog Delta Sharing Documentation
- Databricks Lakehouse Federation Guide
