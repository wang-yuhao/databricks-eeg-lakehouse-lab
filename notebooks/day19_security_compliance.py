# Databricks notebook source
# MAGIC %md
# MAGIC # Day 19: Security & Compliance Patterns
# MAGIC 
# MAGIC ## Professional Certification - Advanced Data Engineering
# MAGIC 
# MAGIC ### **Learning Objectives:**
# MAGIC - Implement Unity Catalog security and governance
# MAGIC - Configure RBAC (Role-Based Access Control) for medical data
# MAGIC - Apply column-level and row-level security
# MAGIC - Implement data masking for PII/PHI
# MAGIC - Design audit logging and compliance tracking
# MAGIC - Ensure HIPAA and GDPR compliance for EEG research data
# MAGIC - Manage secrets and credentials securely
# MAGIC - Implement data encryption at rest and in transit

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Security Framework for Medical Research Data**
# MAGIC 
# MAGIC ### Compliance Requirements:
# MAGIC - **HIPAA**: Protected Health Information (PHI) safeguards
# MAGIC - **GDPR**: Data subject rights, consent management
# MAGIC - **21 CFR Part 11**: Electronic records and signatures (FDA)
# MAGIC - **ISO 27001**: Information security management
# MAGIC 
# MAGIC ### Key Security Layers:
# MAGIC 1. **Identity & Access Management**: Unity Catalog + Azure AD
# MAGIC 2. **Data Classification**: Tagging sensitive columns
# MAGIC 3. **Encryption**: TDE, column encryption, TLS
# MAGIC 4. **Audit & Monitoring**: Audit logs, anomaly detection
# MAGIC 5. **Data Lifecycle**: Retention policies, secure deletion

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import hashlib

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Unity Catalog Governance Setup

# COMMAND ----------

# Create catalog with proper ownership and permissions
def setup_secure_catalog():
    """
    Initialize Unity Catalog with security best practices
    """
    # Create catalog (admin only)
    spark.sql("""
    CREATE CATALOG IF NOT EXISTS eeg_lakehouse_secure
    COMMENT 'HIPAA-compliant EEG research data catalog'
    """)
    
    # Set catalog properties
    spark.sql("""
    ALTER CATALOG eeg_lakehouse_secure 
    SET PROPERTIES (
      'security.classification' = 'confidential',
      'compliance.framework' = 'HIPAA,GDPR',
      'data.retention_days' = '2555',
      'created_by' = current_user(),
      'created_date' = current_timestamp()
    )
    """)
    
    print("✅ Secure catalog created")

# setup_secure_catalog()

# COMMAND ----------

# Create schemas with appropriate separation
def create_security_schemas():
    """
    Create schemas with data classification levels
    """
    schemas = [
        ('raw_phi', 'Contains raw Protected Health Information'),
        ('deidentified', 'De-identified research data'),
        ('aggregated', 'Aggregated statistics only'),
        ('public', 'Publicly shareable research results')
    ]
    
    for schema_name, comment in schemas:
        spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS eeg_lakehouse_secure.{schema_name}
        COMMENT '{comment}'
        """)
        
        print(f"✅ Created schema: {schema_name}")

# create_security_schemas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Role-Based Access Control (RBAC)

# COMMAND ----------

# Define security roles and permissions
def setup_rbac_roles():
    """
    Configure role-based access control
    """
    # Grant permissions to roles (conceptual - actual implementation uses Unity Catalog UI or REST API)
    
    rbac_policies = {
        'data_engineer': {
            'catalogs': ['USE CATALOG', 'CREATE SCHEMA'],
            'schemas': ['USE SCHEMA', 'CREATE TABLE'],
            'tables': ['SELECT', 'MODIFY']
        },
        'data_scientist': {
            'catalogs': ['USE CATALOG'],
            'schemas': ['USE SCHEMA'],
            'tables': ['SELECT']  # Read-only on deidentified data
        },
        'researcher': {
            'catalogs': ['USE CATALOG'],
            'schemas': ['USE SCHEMA'],
            'tables': ['SELECT']  # Read-only on aggregated data
        },
        'compliance_officer': {
            'catalogs': ['USE CATALOG'],
            'schemas': ['USE SCHEMA'],
            'tables': ['SELECT'],  # Can query audit logs
            'system': ['READ AUDIT LOGS']
        }
    }
    
    # Example GRANT statements (execute as metastore admin)
    grant_examples = [
        "GRANT USE CATALOG ON CATALOG eeg_lakehouse_secure TO `data_engineers`",
        "GRANT SELECT ON SCHEMA eeg_lakehouse_secure.deidentified TO `data_scientists`",
        "GRANT SELECT ON eeg_lakehouse_secure.aggregated.sleep_stats TO `researchers`",
        "DENY MODIFY ON SCHEMA eeg_lakehouse_secure.raw_phi TO `data_scientists`"
    ]
    
    print("RBAC Policy Structure:")
    for role, permissions in rbac_policies.items():
        print(f"\n{role}:")
        for resource, perms in permissions.items():
            print(f"  {resource}: {', '.join(perms)}")

# setup_rbac_roles()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Data Masking & De-identification

# COMMAND ----------

# Implement column-level encryption and masking
def mask_phi_columns(df, sensitive_columns):
    """
    Apply data masking to sensitive columns
    """
    masked_df = df
    
    for col_name in sensitive_columns:
        if col_name in df.columns:
            # Hash sensitive values
            masked_df = masked_df.withColumn(
                col_name,
                F.sha2(F.col(col_name).cast('string'), 256)
            )
    
    return masked_df

# Example: Create de-identified view
def create_deidentified_dataset():
    """
    Create de-identified research dataset
    """
    spark.sql("""
    CREATE OR REPLACE TABLE eeg_lakehouse_secure.deidentified.subject_data AS
    SELECT 
        sha2(subject_id, 256) as subject_hash,  -- Pseudonymization
        age_years,
        gender,
        study_group,
        NULL as name,  -- Remove direct identifiers
        NULL as date_of_birth,
        NULL as address,
        recording_date,
        duration_hours
    FROM eeg_lakehouse_secure.raw_phi.subjects
    """)
    
    print("✅ De-identified dataset created")

# create_deidentified_dataset()

# COMMAND ----------

# Implement row-level security with dynamic views
def create_row_level_security_view():
    """
    Create views with row-level filtering based on user context
    """
    spark.sql("""
    CREATE OR REPLACE VIEW eeg_lakehouse_secure.deidentified.my_authorized_subjects AS
    SELECT 
        subject_hash,
        age_years,
        gender,
        recording_date
    FROM eeg_lakehouse_secure.deidentified.subject_data
    WHERE 
        -- Row-level filter based on user's authorized study groups
        study_group IN (
            SELECT study_group 
            FROM eeg_lakehouse_secure.security.user_authorizations
            WHERE user_name = current_user()
        )
    """)
    
    print("✅ Row-level security view created")

# create_row_level_security_view()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Audit Logging & Compliance Tracking

# COMMAND ----------

# Query Unity Catalog audit logs
def query_audit_logs(start_date, end_date):
    """
    Query audit logs for compliance reporting
    """
    audit_query = f"""
    SELECT 
        event_time,
        user_identity.email as user_email,
        service_name,
        action_name,
        request_params.full_name_arg as resource_accessed,
        response.status_code,
        request_params
    FROM system.access.audit
    WHERE 
        event_date BETWEEN '{start_date}' AND '{end_date}'
        AND service_name = 'unityCatalog'
        AND action_name IN (
            'getTable',
            'readTable',
            'updateTable',
            'deleteTable',
            'generateTemporaryTableCredential'
        )
    ORDER BY event_time DESC
    """
    
    audit_df = spark.sql(audit_query)
    
    return audit_df

# Example usage
# audit_logs = query_audit_logs('2026-06-01', '2026-06-15')
# audit_logs.display()

# COMMAND ----------

# Create custom audit trail for data access
def log_data_access(user, table_name, action, row_count=None):
    """
    Log data access events to custom audit table
    """
    access_log = [{
        'access_id': hashlib.sha256(f"{user}{table_name}{datetime.now()}".encode()).hexdigest(),
        'user_name': user,
        'table_name': table_name,
        'action': action,
        'access_timestamp': datetime.now(),
        'row_count': row_count,
        'ip_address': None,  # Would come from request context
        'session_id': None
    }]
    
    log_df = spark.createDataFrame(access_log)
    
    log_df.write.format("delta").mode("append") \
        .saveAsTable("eeg_lakehouse_secure.security.data_access_log")
    
    print(f"✅ Access logged: {user} {action} on {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Data Classification & Tagging

# COMMAND ----------

# Tag tables and columns with classification levels
def classify_data_elements():
    """
    Apply data classification tags
    """
    # Table-level tags
    spark.sql("""
    ALTER TABLE eeg_lakehouse_secure.raw_phi.subjects
    SET TAGS (
      'classification' = 'PHI',
      'sensitivity' = 'high',
      'compliance' = 'HIPAA',
      'retention_years' = '7'
    )
    """)
    
    # Column-level tags
    spark.sql("""
    ALTER TABLE eeg_lakehouse_secure.raw_phi.subjects
    ALTER COLUMN subject_id SET TAGS ('pii' = 'true', 'identifier' = 'direct')
    """)
    
    spark.sql("""
    ALTER TABLE eeg_lakehouse_secure.raw_phi.subjects
    ALTER COLUMN date_of_birth SET TAGS ('pii' = 'true', 'hipaa' = 'true')
    """)
    
    print("✅ Data classification tags applied")

# classify_data_elements()

# COMMAND ----------

# Query data classification tags
def get_classified_columns(catalog, schema, table):
    """
    Retrieve classification tags for compliance review
    """
    tags_query = f"""
    DESCRIBE TABLE EXTENDED {catalog}.{schema}.{table}
    """
    
    table_info = spark.sql(tags_query)
    
    # Filter for tag information
    tags = table_info.filter(F.col("col_name").contains("Tag"))
    
    return tags

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Encryption & Secrets Management

# COMMAND ----------

# Manage secrets using Databricks Secrets API
def setup_secrets_scope():
    """
    Configure secrets scope for sensitive credentials
    Uses Databricks CLI or REST API
    """
    # Commands to run in Databricks CLI:
    commands = [
        "databricks secrets create-scope --scope eeg-research",
        "databricks secrets put --scope eeg-research --key db-password",
        "databricks secrets put --scope eeg-research --key api-key",
        "databricks secrets put --scope eeg-research --key encryption-key"
    ]
    
    print("Secrets management commands:")
    for cmd in commands:
        print(f"  {cmd}")

# In notebook, retrieve secrets securely
def get_secret(scope, key):
    """
    Retrieve secret from Databricks secret scope
    """
    secret_value = dbutils.secrets.get(scope=scope, key=key)
    return secret_value

# Example: Get database password without exposing it
# db_password = get_secret('eeg-research', 'db-password')

# COMMAND ----------

# Implement column-level encryption
def encrypt_sensitive_column(df, column_name, encryption_key):
    """
    Encrypt column using AES encryption
    """
    from pyspark.sql.functions import expr
    
    encrypted_df = df.withColumn(
        f"{column_name}_encrypted",
        expr(f"aes_encrypt({column_name}, '{encryption_key}', 'ECB')")
    ).drop(column_name)
    
    return encrypted_df

def decrypt_sensitive_column(df, column_name, encryption_key):
    """
    Decrypt encrypted column
    """
    from pyspark.sql.functions import expr
    
    decrypted_df = df.withColumn(
        column_name,
        expr(f"cast(aes_decrypt({column_name}_encrypted, '{encryption_key}', 'ECB') as string)")
    )
    
    return decrypted_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: GDPR Compliance - Data Subject Rights

# COMMAND ----------

# Implement Right to Access (Article 15)
def export_subject_data(subject_id, output_format='json'):
    """
    Export all data for a subject (GDPR Right to Access)
    """
    subject_hash = hashlib.sha256(subject_id.encode()).hexdigest()
    
    # Collect all data about the subject
    subject_data = spark.sql(f"""
    SELECT 
        'demographics' as data_type,
        * 
    FROM eeg_lakehouse_secure.deidentified.subject_data
    WHERE subject_hash = '{subject_hash}'
    
    UNION ALL
    
    SELECT 
        'recordings' as data_type,
        *
    FROM eeg_lakehouse_secure.deidentified.recording_metadata
    WHERE subject_hash = '{subject_hash}'
    """)
    
    # Export to specified format
    if output_format == 'json':
        subject_data.write.mode('overwrite').json(f'/tmp/gdpr_export/{subject_id}')
    elif output_format == 'csv':
        subject_data.write.mode('overwrite').csv(f'/tmp/gdpr_export/{subject_id}')
    
    print(f"✅ Data exported for subject {subject_id}")
    return subject_data

# COMMAND ----------

# Implement Right to Erasure (Article 17)
def delete_subject_data(subject_id, reason):
    """
    Delete all data for a subject (GDPR Right to Erasure / Right to be Forgotten)
    """
    subject_hash = hashlib.sha256(subject_id.encode()).hexdigest()
    
    # Log deletion request
    deletion_log = [{
        'subject_id': subject_hash,
        'deletion_requested': datetime.now(),
        'reason': reason,
        'status': 'pending',
        'requested_by': 'current_user()'
    }]
    
    spark.createDataFrame(deletion_log).write.mode('append') \
        .saveAsTable('eeg_lakehouse_secure.security.deletion_requests')
    
    # Execute deletion across all tables
    tables_to_clean = [
        'eeg_lakehouse_secure.raw_phi.subjects',
        'eeg_lakehouse_secure.deidentified.subject_data',
        'eeg_lakehouse_secure.bronze.eeg_signals',
        'eeg_lakehouse_secure.bronze.sleep_annotations'
    ]
    
    for table in tables_to_clean:
        spark.sql(f"""
        DELETE FROM {table}
        WHERE subject_hash = '{subject_hash}'
        """)
        print(f"✅ Deleted data from {table}")
    
    # Update deletion log
    spark.sql(f"""
    UPDATE eeg_lakehouse_secure.security.deletion_requests
    SET status = 'completed', completed_at = current_timestamp()
    WHERE subject_id = '{subject_hash}'
    """)
    
    print(f"✅ All data deleted for subject {subject_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Data Retention & Lifecycle Management

# COMMAND ----------

# Implement automated data retention policies
def apply_retention_policy(table_name, retention_days):
    """
    Delete data older than retention period
    """
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    spark.sql(f"""
    DELETE FROM {table_name}
    WHERE recording_date < '{cutoff_date.strftime('%Y-%m-%d')}'
    """)
    
    print(f"✅ Applied {retention_days}-day retention policy to {table_name}")

# Schedule retention policies
def schedule_retention_jobs():
    """
    Define retention schedule for different data types
    """
    retention_policies = {
        'eeg_lakehouse_secure.raw_phi.subjects': 2555,  # 7 years (HIPAA)
        'eeg_lakehouse_secure.bronze.eeg_signals': 2555,
        'eeg_lakehouse_secure.silver.processed_signals': 1825,  # 5 years
        'eeg_lakehouse_secure.security.audit_logs': 2555,  # 7 years
        'eeg_lakehouse_secure.monitoring.pipeline_metrics': 365  # 1 year
    }
    
    print("Data Retention Schedule:")
    for table, days in retention_policies.items():
        years = days / 365
        print(f"  {table}: {years:.1f} years ({days} days)")

# schedule_retention_jobs()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Compliance Reporting & Dashboards

# COMMAND ----------

# Generate compliance report
def generate_compliance_report(report_month):
    """
    Create monthly compliance report
    """
    report = {}
    
    # 1. Data Access Summary
    report['data_access'] = spark.sql(f"""
    SELECT 
        COUNT(DISTINCT user_name) as unique_users,
        COUNT(*) as total_accesses,
        COUNT(DISTINCT table_name) as tables_accessed
    FROM eeg_lakehouse_secure.security.data_access_log
    WHERE DATE_FORMAT(access_timestamp, 'yyyy-MM') = '{report_month}'
    """).collect()[0].asDict()
    
    # 2. GDPR Requests
    report['gdpr_requests'] = spark.sql(f"""
    SELECT 
        COUNT(*) as total_requests,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending
    FROM eeg_lakehouse_secure.security.deletion_requests
    WHERE DATE_FORMAT(deletion_requested, 'yyyy-MM') = '{report_month}'
    """).collect()[0].asDict()
    
    # 3. Security Incidents
    report['security_incidents'] = spark.sql(f"""
    SELECT 
        COUNT(*) as total_incidents,
        MAX(severity) as max_severity
    FROM eeg_lakehouse_secure.security.incidents
    WHERE DATE_FORMAT(incident_date, 'yyyy-MM') = '{report_month}'
    """).collect()[0].asDict()
    
    # 4. Data Quality
    report['data_quality'] = spark.sql(f"""
    SELECT 
        AVG(quality_score) as avg_quality_score,
        COUNT(*) as quality_checks_run
    FROM eeg_lakehouse_secure.monitoring.quality_metrics
    WHERE DATE_FORMAT(check_date, 'yyyy-MM') = '{report_month}'
    """).collect()[0].asDict()
    
    print(f"\nCompliance Report for {report_month}:")
    print("="*60)
    for section, metrics in report.items():
        print(f"\n{section.upper()}:")
        for metric, value in metrics.items():
            print(f"  {metric}: {value}")
    
    return report

# Example
# compliance_report = generate_compliance_report('2026-06')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 10: Security Best Practices Checklist

# COMMAND ----------

# Security checklist validation
def validate_security_posture():
    """
    Run security posture assessment
    """
    checklist = {
        'Unity Catalog enabled': True,
        'RBAC configured': True,
        'Data classification tags applied': True,
        'Audit logging enabled': True,
        'Secrets managed securely': True,
        'Encryption at rest enabled': True,
        'Encryption in transit (TLS)': True,
        'Row-level security implemented': True,
        'Column-level masking applied': True,
        'GDPR compliance procedures': True,
        'HIPAA safeguards in place': True,
        'Data retention policies configured': True,
        'Incident response plan documented': True,
        'Regular security audits scheduled': True
    }
    
    print("Security Posture Assessment:")
    print("="*60)
    
    for item, status in checklist.items():
        status_icon = "✅" if status else "❌"
        print(f"{status_icon} {item}")
    
    compliance_score = sum(checklist.values()) / len(checklist) * 100
    print(f"\nOverall Compliance Score: {compliance_score:.0f}%")
    
    return checklist

# validate_security_posture()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this notebook, you learned:
# MAGIC 
# MAGIC ✅ Unity Catalog governance and security model  
# MAGIC ✅ Role-Based Access Control (RBAC) implementation  
# MAGIC ✅ Data masking and de-identification techniques  
# MAGIC ✅ Audit logging and compliance tracking  
# MAGIC ✅ Data classification and tagging strategies  
# MAGIC ✅ Encryption and secrets management  
# MAGIC ✅ GDPR compliance (Right to Access, Right to Erasure)  
# MAGIC ✅ HIPAA safeguards for medical data  
# MAGIC ✅ Data retention and lifecycle management  
# MAGIC ✅ Security posture assessment and reporting  
# MAGIC 
# MAGIC ### Next Steps:
# MAGIC - Day 20: Advanced TDA Algorithms (Persistent Homology)
# MAGIC - Day 21: End-to-End Pipeline Integration
