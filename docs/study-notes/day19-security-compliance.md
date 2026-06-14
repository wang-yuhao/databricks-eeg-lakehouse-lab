# Day 19: Security and Compliance

## Overview
Study notes on security, governance, and compliance features in Databricks for Professional certification.

## Key Concepts

### 1. Unity Catalog Security
- **Metastore**: Centralized metadata repository
- **Catalog**: Top-level namespace for organizing data
- **Schema**: Container for tables, views, and functions
- **Three-level namespace**: catalog.schema.table

### 2. Access Control Models

#### Table Access Control (Legacy)
- Database, table, and view level permissions
- GRANT and REVOKE statements
- Object ownership model

#### Unity Catalog Access Control
- Fine-grained permissions (SELECT, MODIFY, CREATE, etc.)
- Inheritance from parent objects
- Securable objects hierarchy
- Data lineage and audit logging

### 3. Authentication Methods
- **Single Sign-On (SSO)**: SAML, OAuth 2.0
- **Personal Access Tokens (PAT)**: API authentication
- **Service Principals**: Application authentication
- **Azure Active Directory**: Cloud identity integration
- **SCIM Provisioning**: User and group management

### 4. Authorization Levels

#### Workspace-Level
- Workspace access control
- Cluster policies
- Job access control
- Notebook permissions

#### Data-Level
- Row-level security
- Column-level security (masking)
- Dynamic views for access control
- Attribute-based access control (ABAC)

### 5. Network Security
- **VNet Injection**: Deploy clusters in customer VNet
- **Private Link**: Secure connectivity to Azure services
- **IP Access Lists**: Restrict workspace access by IP
- **Secure Cluster Connectivity**: No public IP on workers

## Unity Catalog Governance

### Data Discovery
- Search and browse data assets
- Column-level tags and comments
- Data classification
- Business glossary integration

### Data Lineage
- Automatic capture of lineage
- Upstream and downstream dependencies
- Impact analysis
- Compliance reporting

### Audit Logging
- User activity tracking
- Data access auditing
- Compliance monitoring
- Integration with SIEM tools

## Security Best Practices

### Secrets Management
```python
# Use Databricks secrets
dbutils.secrets.get(scope="my_scope", key="my_key")

# Never hardcode credentials
# DON'T: password = "mypassword123"
# DO: password = dbutils.secrets.get(scope="credentials", key="db_password")
```

### Cluster Security
- Enable cluster access control
- Use cluster policies to enforce standards
- Implement automatic termination
- Enable credential passthrough for Unity Catalog
- Use instance profiles for AWS access

### Data Encryption
- **At Rest**: Encrypted storage (ADLS, S3)
- **In Transit**: TLS/SSL encryption
- **Managed Keys**: Customer-managed encryption keys (CMEK)

## Permissions and Grants

### Unity Catalog Permissions
```sql
-- Grant catalog usage
GRANT USE CATALOG ON CATALOG my_catalog TO `user@company.com`;

-- Grant schema usage and creation
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA my_catalog.my_schema TO `data_engineers`;

-- Grant table access
GRANT SELECT ON TABLE my_catalog.my_schema.my_table TO `analysts`;

-- Grant modify permissions
GRANT MODIFY ON TABLE my_catalog.my_schema.my_table TO `data_engineers`;

-- Revoke permissions
REVOKE SELECT ON TABLE my_catalog.my_schema.my_table FROM `user@company.com`;
```

### Row and Column Level Security
```sql
-- Create view with row-level filtering
CREATE VIEW filtered_data AS
SELECT *
FROM my_table
WHERE region = current_user();

-- Create view with column masking
CREATE VIEW masked_data AS
SELECT 
  id,
  CASE 
    WHEN is_member('admin') THEN email
    ELSE '***@***.**'
  END AS email,
  name
FROM users;
```

## Compliance Features

### Data Retention
- Delta Lake time travel
- Configurable retention periods
- VACUUM command for cleanup
- GDPR compliance support

### Data Privacy
- PII detection and masking
- Data anonymization
- Right to be forgotten (DELETE)
- Data access request handling

### Regulatory Compliance
- SOC 2 Type II certified
- HIPAA compliance
- GDPR compliance
- ISO 27001 certified

## Cluster Policies

### Policy Definition
```json
{
  "spark_version": {
    "type": "fixed",
    "value": "11.3.x-scala2.12"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": ["Standard_DS3_v2", "Standard_DS4_v2"]
  },
  "autotermination_minutes": {
    "type": "range",
    "maxValue": 120
  }
}
```

## Practice Questions
1. What are the differences between Table ACLs and Unity Catalog?
2. How do you implement row-level security in Databricks?
3. What authentication methods are available for service-to-service communication?
4. How does data lineage help with compliance?
5. What are the components of the Unity Catalog three-level namespace?
6. How do you manage secrets securely in Databricks?

## References
- Unity Catalog Documentation
- Databricks Security and Compliance Guide
- Azure Databricks Security Best Practices
- Data Governance with Unity Catalog
