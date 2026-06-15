# Databricks notebook source
# MAGIC %md
# MAGIC # Day 16: Delta Sharing & Federation
# MAGIC 
# MAGIC ## Professional Certification - Advanced Data Engineering
# MAGIC 
# MAGIC **Learning Objectives:**
# MAGIC - Understand Delta Sharing protocol and architecture
# MAGIC - Configure Delta Sharing Server and recipients
# MAGIC - Implement cross-workspace and cross-cloud sharing
# MAGIC - Integrate Delta Sharing with BI tools (Power BI, Tableau)
# MAGIC - Manage shares, recipients, and permissions
# MAGIC - Monitor and audit shared data access
# MAGIC - Implement federation patterns for data mesh
# MAGIC 
# MAGIC **Exam Relevance:**
# MAGIC - Data Sharing & Collaboration: 15%
# MAGIC - Data Governance & Security: 20%
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Delta Sharing Fundamentals
# MAGIC 
# MAGIC ### Key Concepts:
# MAGIC - Open protocol for secure data sharing
# MAGIC - No data movement/duplication
# MAGIC - Support for tables, views, and notebooks
# MAGIC - Fine-grained access control
# MAGIC - Cross-platform compatibility

# COMMAND ----------

import json
import pandas as pd
from delta_sharing import SharingClient, load_as_pandas, load_as_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Setting Up Delta Sharing as a Provider
# MAGIC 
# MAGIC ### 2.1: Create a Share

# SQL to create a share
spark.sql("""
CREATE SHARE IF NOT EXISTS eeg_research_share
COMMENT 'Share EEG sleep research data with academic partners'
""")

# View all shares
display(spark.sql("SHOW SHARES"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2: Add Tables to Share

# Add multiple tables to the share
spark.sql("""
ALTER SHARE eeg_research_share 
ADD TABLE catalog.eeg_data.anonymized_recordings 
COMMENT 'De-identified sleep EEG recordings'
""")

spark.sql("""
ALTER SHARE eeg_research_share 
ADD TABLE catalog.eeg_data.sleep_metrics
PARTITION (diagnosis = 'insomnia')
COMMENT 'Sleep quality metrics for insomnia patients only'
""")

spark.sql("""
ALTER SHARE eeg_research_share 
ADD TABLE catalog.eeg_data.subject_demographics
COMMENT 'Anonymized subject demographic information'
""")

# View tables in share
display(spark.sql("SHOW ALL IN SHARE eeg_research_share"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3: Create Recipients

# Create recipient for external organization
spark.sql("""
CREATE RECIPIENT IF NOT EXISTS research_university_berlin
USING ID 'berlin-uni-research-group'
COMMENT 'Charité Sleep Research Lab'
""")

spark.sql("""
CREATE RECIPIENT IF NOT EXISTS pharma_partner_trial
USING ID 'pharma-clinical-trial-001'
COMMENT 'Phase 3 clinical trial partner'
""")

# View all recipients
display(spark.sql("SHOW RECIPIENTS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4: Grant Access to Recipients

# Grant share to recipient
spark.sql("""
GRANT SELECT ON SHARE eeg_research_share 
TO RECIPIENT research_university_berlin
""")

spark.sql("""
GRANT SELECT ON SHARE eeg_research_share 
TO RECIPIENT pharma_partner_trial
""")

# View grants
display(spark.sql("""
SHOW GRANT ON SHARE eeg_research_share
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5: Generate Activation Link

# Generate activation link for recipient
activation_link_query = """
DESCRIBE RECIPIENT research_university_berlin
"""

recipient_info = spark.sql(activation_link_query)
display(recipient_info)

# The activation link can be shared securely with the recipient
# They use it to generate their Delta Sharing credentials

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Consuming Shared Data as a Recipient
# MAGIC 
# MAGIC ### 3.1: Configure Delta Sharing Profile

# Example Delta Sharing profile (JSON)
profile_config = {
    "shareCredentialsVersion": 1,
    "endpoint": "https://your-workspace.cloud.databricks.com/api/2.0/delta-sharing/",
    "bearerToken": "your-bearer-token-here",  # Provided by data provider
    "expirationTime": "2026-12-31T23:59:59Z"
}

# Save profile to DBFS
profile_path = "/dbfs/mnt/config/delta-sharing-profile.json"

with open(profile_path, 'w') as f:
    json.dump(profile_config, f, indent=2)

print(f"Profile saved to: {profile_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2: List Available Shares

# Using delta-sharing Python library
from delta_sharing import SharingClient

# Create client with profile
client = SharingClient(profile_path)

# List all shares available to this recipient
shares = client.list_shares()

for share in shares:
    print(f"Share: {share.name}")
    
    # List schemas in share
    schemas = client.list_schemas(share)
    for schema in schemas:
        print(f"  Schema: {schema.name}")
        
        # List tables in schema
        tables = client.list_tables(schema)
        for table in tables:
            print(f"    Table: {table.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3: Read Shared Data as Pandas DataFrame

# Read shared table as Pandas DataFrame
table_url = f"{profile_path}#eeg_research_share.eeg_data.anonymized_recordings"

df_pandas = load_as_pandas(table_url)

print(f"Loaded {len(df_pandas)} rows")
print(df_pandas.head())
print(df_pandas.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4: Read Shared Data as Spark DataFrame

# Read as Spark DataFrame for large-scale processing
df_spark = (spark.read
    .format("deltaSharing")
    .option("responseFormat", "delta")  # Use Delta format for better performance
    .load(table_url))

print(f"Total records: {df_spark.count()}")
df_spark.printSchema()
df_spark.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5: Query Shared Data with Filters

# Predicate pushdown is supported
filtered_df = (spark.read
    .format("deltaSharing")
    .option("responseFormat", "delta")
    .load(table_url)
    .filter(col("recording_date") >= "2025-01-01")
    .filter(col("sleep_stage").isin("REM", "N3"))
    .select("subject_id", "recording_date", "sleep_stage", "duration_minutes"))

filtered_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Advanced Delta Sharing Features
# MAGIC 
# MAGIC ### 4.1: Sharing Views with Row-Level Security

# Create a view with row-level filtering
spark.sql("""
CREATE OR REPLACE VIEW catalog.eeg_data.filtered_recordings_view
AS
SELECT 
    subject_id,
    recording_date,
    channel,
    sleep_stage,
    duration_minutes,
    quality_score
FROM catalog.eeg_data.anonymized_recordings
WHERE 
    data_quality = 'HIGH'
    AND consent_status = 'APPROVED'
    AND age BETWEEN 18 AND 65
""")

# Add view to share
spark.sql("""
ALTER SHARE eeg_research_share 
ADD VIEW catalog.eeg_data.filtered_recordings_view
COMMENT 'Pre-filtered high-quality recordings with consent'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2: Sharing Change Data Feed (CDF)

# Enable CDF on source table
spark.sql("""
ALTER TABLE catalog.eeg_data.anonymized_recordings
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Add table with CDF to share
spark.sql("""
ALTER SHARE eeg_research_share 
ADD TABLE catalog.eeg_data.anonymized_recordings
WITH CHANGE DATA FEED
""")

# Recipients can now query changes
# df_changes = spark.read
#     .format("deltaSharing")
#     .option("readChangeFeed", "true")
#     .option("startingVersion", 0)
#     .load(table_url)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3: Partition-Level Sharing

# Share specific partitions only
spark.sql("""
ALTER SHARE eeg_research_share 
ADD TABLE catalog.eeg_data.recordings_by_diagnosis
PARTITION (diagnosis = 'sleep_apnea')
PARTITION (diagnosis = 'insomnia')
COMMENT 'Only sleep disorders data, excluding healthy controls'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Cross-Cloud and Cross-Platform Sharing
# MAGIC 
# MAGIC ### 5.1: Share Across Cloud Providers

# Delta Sharing works across:
# - Azure Databricks -> AWS Databricks
# - AWS Databricks -> GCP Databricks
# - Any cloud -> On-premises
# - Databricks -> Non-Databricks (Pandas, Spark, etc.)

# Example: Share from Azure to AWS recipient
spark.sql("""
CREATE RECIPIENT IF NOT EXISTS aws_research_partner
USING ID 'aws-recipient-unique-id'
COMMENT 'AWS-based research institution'
""")

spark.sql("""
GRANT SELECT ON SHARE eeg_research_share 
TO RECIPIENT aws_research_partner
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2: Integration with Power BI

# Power BI can connect to Delta Sharing via:
# 1. Python script visual using delta-sharing library
# 2. Custom connector (community-developed)
# 3. Export to supported format

# Example: Prepare data for Power BI
shared_table_url = f"{profile_path}#eeg_research_share.eeg_data.sleep_metrics"

# Load as Pandas for Power BI Python visual
df_for_powerbi = load_as_pandas(shared_table_url)

# This DataFrame can be used in Power BI Python visual
print("Data loaded for Power BI")
print(df_for_powerbi.info())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3: Integration with Tableau

# Tableau integration options:
# 1. Delta Sharing Connector for Tableau
# 2. Export to Parquet and use Tableau Parquet connector
# 3. Use Databricks connector with shared tables

# Example: Export for Tableau
output_path = "/dbfs/mnt/exports/tableau_export.parquet"

df_for_tableau = spark.read\
    .format("deltaSharing")\
    .load(shared_table_url)

df_for_tableau.write\
    .mode("overwrite")\
    .parquet(output_path)

print(f"Data exported for Tableau: {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Monitoring and Auditing
# MAGIC 
# MAGIC ### 6.1: Track Share Access

# Query system tables for share access logs
audit_query = """
SELECT 
    event_time,
    user_identity.email as accessor,
    request_params.share_name as share_accessed,
    request_params.table_name as table_accessed,
    response.status_code,
    source_ip_address
FROM system.access.audit
WHERE action_name = 'deltaSharing.getTable'
    AND event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY event_time DESC
"""

audit_df = spark.sql(audit_query)
display(audit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2: Monitor Share Usage Metrics

# Create dashboard query for share usage
usage_query = """
SELECT 
    DATE(event_time) as access_date,
    request_params.share_name,
    request_params.table_name,
    COUNT(*) as access_count,
    COUNT(DISTINCT user_identity.email) as unique_users,
    SUM(CASE WHEN response.status_code = 200 THEN 1 ELSE 0 END) as successful_accesses,
    SUM(CASE WHEN response.status_code != 200 THEN 1 ELSE 0 END) as failed_accesses
FROM system.access.audit
WHERE action_name LIKE 'deltaSharing.%'
    AND event_date >= current_date() - INTERVAL 30 DAYS
GROUP BY 1, 2, 3
ORDER BY access_date DESC, access_count DESC
"""

usage_metrics = spark.sql(usage_query)
display(usage_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3: Set Up Alerts for Unusual Access Patterns

# Detect anomalous access patterns
anomalous_access = spark.sql("""
WITH daily_access_counts AS (
    SELECT 
        DATE(event_time) as access_date,
        user_identity.email,
        COUNT(*) as daily_access_count
    FROM system.access.audit
    WHERE action_name LIKE 'deltaSharing.%'
        AND event_date >= current_date() - INTERVAL 90 DAYS
    GROUP BY 1, 2
),
average_access AS (
    SELECT 
        email,
        AVG(daily_access_count) as avg_daily_access,
        STDDEV(daily_access_count) as stddev_daily_access
    FROM daily_access_counts
    GROUP BY email
)
SELECT 
    dac.access_date,
    dac.email,
    dac.daily_access_count,
    aa.avg_daily_access,
    aa.stddev_daily_access,
    CASE 
        WHEN dac.daily_access_count > (aa.avg_daily_access + 3 * aa.stddev_daily_access)
        THEN 'ANOMALY: High access'
        ELSE 'NORMAL'
    END as status
FROM daily_access_counts dac
JOIN average_access aa ON dac.email = aa.email
WHERE dac.access_date >= current_date() - INTERVAL 7 DAYS
    AND dac.daily_access_count > (aa.avg_daily_access + 3 * aa.stddev_daily_access)
ORDER BY dac.access_date DESC, dac.daily_access_count DESC
""")

display(anomalous_access)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Data Federation and Mesh Patterns
# MAGIC 
# MAGIC ### 7.1: Implement Data Mesh with Delta Sharing

# Data Mesh pattern: Domain-owned data products shared via Delta Sharing

# Domain 1: Clinical Data (owned by clinical team)
spark.sql("""
CREATE SHARE IF NOT EXISTS clinical_data_product
COMMENT 'Clinical domain data product for research'
""")

spark.sql("""
ALTER SHARE clinical_data_product
ADD TABLE catalog.clinical_domain.patient_records
ADD TABLE catalog.clinical_domain.diagnosis_history
""")

# Domain 2: Biosignals Data (owned by data engineering team)
spark.sql("""
CREATE SHARE IF NOT EXISTS biosignals_data_product
COMMENT 'EEG and biosignals data product'
""")

spark.sql("""
ALTER SHARE biosignals_data_product
ADD TABLE catalog.biosignals_domain.eeg_recordings
ADD TABLE catalog.biosignals_domain.sleep_stages
""")

# Cross-domain analytics team consumes both data products

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2: Federated Query Across Multiple Shares

# Read from multiple shares in a single query
clinical_profile = "/dbfs/mnt/config/clinical-share-profile.json"
biosignals_profile = "/dbfs/mnt/config/biosignals-share-profile.json"

# Load from clinical domain
df_clinical = spark.read\
    .format("deltaSharing")\
    .load(f"{clinical_profile}#clinical_data_product.clinical_domain.patient_records")

# Load from biosignals domain
df_biosignals = spark.read\
    .format("deltaSharing")\
    .load(f"{biosignals_profile}#biosignals_data_product.biosignals_domain.eeg_recordings")

# Join across domains
federated_analysis = df_clinical.join(
    df_biosignals,
    "patient_id",
    "inner"
).select(
    df_clinical["patient_id"],
    df_clinical["age"],
    df_clinical["diagnosis"],
    df_biosignals["sleep_efficiency"],
    df_biosignals["rem_percentage"]
)

federated_analysis.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Security and Compliance
# MAGIC 
# MAGIC ### 8.1: Implement Data Masking for Shared Tables

# Create masked view for sharing
spark.sql("""
CREATE OR REPLACE VIEW catalog.eeg_data.masked_patient_data
AS
SELECT 
    SHA2(patient_id, 256) as anonymized_patient_id,  -- Hash PII
    CAST(age / 10 AS INT) * 10 as age_bracket,  -- Age buckets
    gender,
    LEFT(postal_code, 2) as postal_region,  -- Partial location
    diagnosis,
    recording_date,
    sleep_efficiency
FROM catalog.eeg_data.patient_recordings
WHERE consent_for_research = TRUE
""")

# Share the masked view
spark.sql("""
ALTER SHARE eeg_research_share 
ADD VIEW catalog.eeg_data.masked_patient_data
COMMENT 'De-identified patient data for research'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2: Implement Time-Based Access Control

# Create recipient with expiration
spark.sql("""
CREATE RECIPIENT IF NOT EXISTS temp_research_partner
USING ID 'temporary-partner-2026'
COMMENT 'Access expires on 2026-12-31'
""")

# Note: Actual expiration is managed through recipient credential rotation
# and can be automated via Databricks API

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3: Audit Compliance Requirements

# Generate compliance report
compliance_report = spark.sql("""
SELECT 
    s.share_name,
    s.comment as share_description,
    r.recipient_name,
    r.created_at as recipient_created,
    r.created_by,
    g.privilege,
    g.granted_on,
    g.granted_by
FROM (
    SELECT name as share_name, comment, created_at, created_by
    FROM (SHOW SHARES)
) s
CROSS JOIN (
    SELECT name as recipient_name, created_at, created_by
    FROM (SHOW RECIPIENTS)
) r
LEFT JOIN (
    SELECT share_name, recipient_name, privilege, granted_on, granted_by
    FROM (SHOW GRANTS ON SHARE eeg_research_share)
) g ON s.share_name = g.share_name AND r.recipient_name = g.recipient_name
ORDER BY s.share_name, r.recipient_name
""")

display(compliance_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Performance Optimization
# MAGIC 
# MAGIC ### 9.1: Use Delta Format Response

# For best performance, use Delta format response
df_optimized = (spark.read
    .format("deltaSharing")
    .option("responseFormat", "delta")  # vs "parquet"
    .option("ignoreDeletes", "true")  # Skip deleted rows for faster read
    .option("ignoreChanges", "true")  # Skip updates for faster read
    .load(table_url))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2: Cache Shared Data for Repeated Access

# Cache frequently accessed shared data
df_shared_cached = spark.read\
    .format("deltaSharing")\
    .load(table_url)\
    .cache()

print(f"Cached {df_shared_cached.count()} records")

# Subsequent queries use cached data
df_shared_cached.filter(col("sleep_stage") == "REM").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 10: Best Practices and Troubleshooting
# MAGIC 
# MAGIC ### 10.1: Best Practices

# Best Practices Summary:
practices = """
1. **Data Quality:**
   - Share only validated, high-quality data
   - Use views with quality filters
   - Document data lineage and refresh schedules

2. **Security:**
   - Apply row-level security before sharing
   - Mask PII in shared views
   - Use recipient credentials rotation
   - Monitor access patterns

3. **Performance:**
   - Use Delta format response
   - Leverage partition pruning
   - Cache frequently accessed data
   - Optimize shared table layout (OPTIMIZE, Z-ORDER)

4. **Governance:**
   - Document share purpose and SLAs
   - Implement approval workflows
   - Regular access reviews
   - Audit trail monitoring

5. **Communication:**
   - Provide clear documentation
   - Notify recipients of schema changes
   - Establish support channels
   - Share data dictionaries
"""

print(practices)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2: Common Troubleshooting

def troubleshoot_delta_sharing():
    """
    Common issues and solutions
    """
    issues = {
        "Profile not found": "Check profile path and file permissions",
        "Authentication failed": "Verify bearer token and expiration",
        "Table not in share": "Use SHOW ALL IN SHARE to verify tables",
        "Slow reads": "Use responseFormat=delta and enable caching",
        "Permission denied": "Check GRANT statements and recipient access",
        "Schema mismatch": "Recipient may have cached old schema - refresh"
    }
    
    for issue, solution in issues.items():
        print(f"Issue: {issue}")
        print(f"Solution: {solution}")
        print()

troubleshoot_delta_sharing()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Exam Tips
# MAGIC 
# MAGIC ### Key Takeaways:
# MAGIC 
# MAGIC 1. **Delta Sharing Protocol:**
# MAGIC    - Open, cross-platform protocol
# MAGIC    - No data duplication
# MAGIC    - REST API based
# MAGIC    - Supports tables, views, and CDF
# MAGIC 
# MAGIC 2. **Provider Setup:**
# MAGIC    - CREATE SHARE
# MAGIC    - ADD TABLE/VIEW to share
# MAGIC    - CREATE RECIPIENT
# MAGIC    - GRANT permissions
# MAGIC 
# MAGIC 3. **Consumer Access:**
# MAGIC    - Delta Sharing profile configuration
# MAGIC    - Read as Pandas or Spark DataFrame
# MAGIC    - Predicate pushdown supported
# MAGIC    - Cross-cloud compatible
# MAGIC 
# MAGIC 4. **Security:**
# MAGIC    - Row-level security via views
# MAGIC    - Column masking
# MAGIC    - Partition-level sharing
# MAGIC    - Audit logging
# MAGIC 
# MAGIC 5. **Integration:**
# MAGIC    - Power BI, Tableau
# MAGIC    - Python, Spark, Pandas
# MAGIC    - Cross-cloud providers
# MAGIC    - Data mesh patterns
# MAGIC 
# MAGIC ### Exam Focus Areas:
# MAGIC - Share creation and management commands
# MAGIC - Recipient configuration
# MAGIC - Security and access control
# MAGIC - Cross-platform integration
# MAGIC - Monitoring and auditing
# MAGIC - Performance optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice Exercises
# MAGIC 
# MAGIC 1. Create a share with 3 tables and 2 views
# MAGIC 2. Set up row-level security using a view
# MAGIC 3. Configure a recipient and generate activation link
# MAGIC 4. Read shared data from a different workspace
# MAGIC 5. Implement federated query across 2 shares
# MAGIC 6. Create audit dashboard for share access
# MAGIC 7. Set up Delta Sharing for Power BI integration
# MAGIC 
# MAGIC ## Next Steps
# MAGIC - Day 17: Monitoring & Observability
# MAGIC - Day 18: PhysioNet Dataset Integration
# MAGIC - Day 19: Security & Compliance
