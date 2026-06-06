# Databricks notebook source
# notebooks/day08_unity_catalog.py
# =============================================================================
# DAY 8 - Unity Catalog: Setup, Governance, GRANT, Row Filters
# =============================================================================
# EXAM DOMAINS: UC hierarchy, managed vs external tables, GRANT/REVOKE,
#               row-level security, column masking, Volumes, lineage
# RESEARCH: GDPR-compliant EEG data governance for clinical research data
# =============================================================================

# COMMAND ----------
# %md
# ## Day 8: Unity Catalog Setup and Governance
#
# ### UC Object Hierarchy
# ```
# Metastore (one per region, set by account admin)
#   Catalog  (eeg_lakehouse)
#     Schema / Database  (bronze, silver, gold)
#       Table / View / Volume / Function
# ```
#
# ### Key Exam Facts
# - A **metastore** is the top-level container; one per Databricks account region
# - **Catalogs** are the first namespace level (replaces Hive 'database')
# - **Volumes** store unstructured files (EDF, CSV) with governance
# - **GRANT** syntax: `GRANT privilege ON object TO principal`
# - **Row filters** and **column masks** for fine-grained access control

# COMMAND ----------
# %md ### Step 1: Create Catalog, Schemas, Volumes

# EXAM NOTE: Only account admins or catalog owners can CREATE CATALOG
# In a shared workspace, run this once; idempotent with IF NOT EXISTS

# %sql
# -- =========================================================
# -- 1. CREATE CATALOG
# -- =========================================================
# CREATE CATALOG IF NOT EXISTS eeg_lakehouse
#   COMMENT 'EEG Sleep Research Lakehouse - TDA Memory Consolidation Project';
#
# USE CATALOG eeg_lakehouse;
#
# -- =========================================================
# -- 2. CREATE SCHEMAS (Bronze / Silver / Gold)
# -- =========================================================
# CREATE SCHEMA IF NOT EXISTS bronze
#   COMMENT 'Raw EDF binary files and metadata';
#
# CREATE SCHEMA IF NOT EXISTS silver
#   COMMENT 'Cleaned EEG epochs and detected sleep events';
#
# CREATE SCHEMA IF NOT EXISTS gold
#   COMMENT 'ML-ready feature tables and aggregated metrics';
#
# CREATE SCHEMA IF NOT EXISTS ml
#   COMMENT 'MLflow experiments and registered models';
#
# -- =========================================================
# -- 3. CREATE VOLUMES (for unstructured EDF files)
# -- EXAM NOTE: Volumes are UC-governed file paths.
# --   MANAGED volume: Databricks controls storage location
# --   EXTERNAL volume: You specify a cloud storage path
# -- =========================================================
# CREATE VOLUME IF NOT EXISTS bronze.raw_edf
#   COMMENT 'Sleep-EDF Expanded EDF files from PhysioNet';
#
# -- External volume pointing to ADLS Gen2:
# -- CREATE EXTERNAL VOLUME bronze.raw_edf_external
# --   LOCATION 'abfss://eeg@youraccount.dfs.core.windows.net/raw/'
# --   COMMENT 'External EDF files on Azure ADLS Gen2';

print("Run the SQL above in a %sql cell on Databricks with UC-enabled cluster")

# COMMAND ----------
# %md ### Step 2: GRANT and REVOKE
# EXAM NOTE: Three privilege levels: USE (navigate), SELECT (read), MODIFY (write)

# %sql
# -- =========================================================
# -- GRANT PRIVILEGES
# -- =========================================================
# -- Give data scientists read access to Silver and Gold
# GRANT USE CATALOG ON CATALOG eeg_lakehouse TO `data-scientists`;
# GRANT USE SCHEMA  ON SCHEMA eeg_lakehouse.silver TO `data-scientists`;
# GRANT USE SCHEMA  ON SCHEMA eeg_lakehouse.gold   TO `data-scientists`;
# GRANT SELECT      ON SCHEMA eeg_lakehouse.silver TO `data-scientists`;
# GRANT SELECT      ON SCHEMA eeg_lakehouse.gold   TO `data-scientists`;
#
# -- Give pipeline service principal write access to Bronze
# GRANT USE CATALOG ON CATALOG eeg_lakehouse TO `eeg-pipeline-sp`;
# GRANT USE SCHEMA  ON SCHEMA eeg_lakehouse.bronze TO `eeg-pipeline-sp`;
# GRANT CREATE TABLE, MODIFY ON SCHEMA eeg_lakehouse.bronze TO `eeg-pipeline-sp`;
#
# -- READ FILES on Volume (for Auto Loader)
# GRANT READ VOLUME ON VOLUME eeg_lakehouse.bronze.raw_edf TO `eeg-pipeline-sp`;
#
# -- REVOKE example
# -- REVOKE SELECT ON TABLE eeg_lakehouse.gold.eeg_features FROM `contractor-group`;

print("Run GRANT/REVOKE SQL in a UC-enabled cluster")

# COMMAND ----------
# %md ### Step 3: Row-Level Security (Row Filters)
# Clinical research use case: analysts can only see their own site's subjects

# %sql
# -- =========================================================
# -- ROW FILTER FUNCTION
# -- EXAM NOTE: Row filters are SQL functions applied per-user at query time
# -- =========================================================
# CREATE OR REPLACE FUNCTION eeg_lakehouse.gold.site_row_filter(site_id STRING)
# RETURNS BOOLEAN
# RETURN
#   is_account_group_member('admin') OR  -- admins see everything
#   CURRENT_USER() = site_id;            -- users see only their site
#
# -- Apply filter to Gold table
# ALTER TABLE eeg_lakehouse.gold.eeg_features
#   SET ROW FILTER eeg_lakehouse.gold.site_row_filter ON (site_id);

print("Row filter demonstration - run in UC cluster")

# COMMAND ----------
# %md ### Step 4: Column Masking
# GDPR: mask subject names for non-privileged users

# %sql
# -- =========================================================
# -- COLUMN MASK
# -- EXAM NOTE: Masks a column value based on caller's identity
# -- =========================================================
# CREATE OR REPLACE FUNCTION eeg_lakehouse.gold.mask_subject_name(subject_name STRING)
# RETURNS STRING
# RETURN
#   CASE
#     WHEN is_account_group_member('clinical-team') THEN subject_name
#     ELSE CONCAT(LEFT(subject_name, 2), '****')   -- partial mask
#   END;
#
# ALTER TABLE eeg_lakehouse.gold.eeg_features
#   ALTER COLUMN subject_name
#   SET MASK eeg_lakehouse.gold.mask_subject_name;

print("Column masking demonstration")

# COMMAND ----------
# %md ### Step 5: SHOW and DESCRIBE in UC

# Inspect catalog structure (run on Databricks)
queries = [
    "SHOW CATALOGS",
    "SHOW SCHEMAS IN eeg_lakehouse",
    "SHOW TABLES IN eeg_lakehouse.gold",
    "SHOW VOLUMES IN eeg_lakehouse.bronze",
    "DESCRIBE TABLE EXTENDED eeg_lakehouse.gold.eeg_features",
]

for q in queries:
    print(f"-- {q}")
    # spark.sql(q).show()  # uncomment on Databricks

# COMMAND ----------
# %md ### Step 6: Table Properties and Tags (for discoverability)

# %sql
# -- EXAM NOTE: Tags enable data discovery and classification in UC
# ALTER TABLE eeg_lakehouse.gold.eeg_features
#   SET TAGS ('project' = 'eeg-tda', 'pii' = 'false', 'layer' = 'gold');
#
# ALTER TABLE eeg_lakehouse.silver.eeg_events
#   SET TAGS ('sensitivity' = 'internal', 'data_owner' = 'neuroscience-team');
#
# -- View tags
# SHOW TAGS ON TABLE eeg_lakehouse.gold.eeg_features;

# COMMAND ----------
# %md
# ## UC Exam Quick Reference
#
# | Concept | Key fact |
# |---------|----------|
# | Metastore | 1 per region; assigned to workspace by account admin |
# | Catalog | First namespace; default catalog = `main` |
# | External location | UC-governed cloud path for external tables |
# | Volume | UC-governed path for FILES (not tables) |
# | `is_account_group_member()` | Check group membership in row filters |
# | `CURRENT_USER()` | Returns caller's email in SQL |
# | Row filter | Function on table; filters rows per user |
# | Column mask | Function on column; transforms value per user |
# | Lineage | Auto-tracked: table -> column level in UC |
# | `GRANT SELECT` | Read-only access to a table |
# | `GRANT MODIFY` | Insert/Update/Delete on a table |
# | `GRANT ALL PRIVILEGES` | Full access (owner-level) |
#
# ### EEG Research Governance Map
# | Layer | Who can read | Who can write |
# |-------|-------------|---------------|
# | Bronze | pipeline-SP, admins | pipeline-SP |
# | Silver | data-scientists, admins | pipeline-SP |
# | Gold | analysts, data-scientists, admins | pipeline-SP |
# | ML | ml-engineers, admins | ml-SP |

print("Day 8 complete! Proceed to Day 9: Structured Streaming.")
