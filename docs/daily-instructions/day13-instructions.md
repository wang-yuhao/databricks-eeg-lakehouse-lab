# Day 13: Unity Catalog Governance — Lineage, Column-Level Security, and Row Filters

| Field | Value |
|---|---|
| **Notebook** | `notebooks/day13_unity_catalog.py` |
| **Exam domains** | Domain 1 — Databricks Lakehouse Platform; Domain 5 — Security and Governance |
| **Time estimate** | 4–5 hours |
| **Prerequisite** | Days 1–12 completed; Unity Catalog metastore attached to the workspace; Bronze, Silver, and Gold tables exist in `eeg_lakehouse` catalog |

---

## Section 1: Environment Setup

Complete every step in this section before opening any notebook cell. A reader starting from a blank Databricks workspace must follow these steps in order.

### 1.1 Create a GitHub Personal Access Token

1. Sign in to [github.com](https://github.com).
2. Navigate to **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)**.
3. Click **Generate new token (classic)**.
4. Set the following fields:

   | Field | Value |
   |---|---|
   | Note | `databricks-eeg-lab` |
   | Expiration | 90 days |
   | Scopes | `repo` (full), `workflow` |

5. Click **Generate token**, copy it immediately, and store it in a password manager.

### 1.2 Configure Databricks Git Integration

1. Click your username in the top-right corner and select **User Settings**.
2. Select the **Git integration** tab.
3. Enter the following:

   | Field | Value |
   |---|---|
   | Git provider | GitHub |
   | Git provider username | Your GitHub username |
   | Personal access token | Token from step 1.1 |

4. Click **Save**.

### 1.3 Clone the Repository into Databricks Repos

1. In the left sidebar, click **Repos** → **Add repo**.
2. Enter the following:

   | Field | Value |
   |---|---|
   | Git repository URL | `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab` |
   | Git provider | GitHub |
   | Repo name | `databricks-eeg-lakehouse-lab` |

3. Click **Create repo**.

### 1.4 Create a Cluster with Unity Catalog Enabled

1. In the left sidebar, click **Compute** → **Create compute**.
2. Apply the following configuration:

   | Setting | Value |
   |---|---|
   | Cluster name | `eeg-lab-day13` |
   | Cluster mode | Single node |
   | Databricks Runtime | **14.3 LTS** |
   | Node type | `i3.xlarge` (AWS) or `Standard_DS4_v2` (Azure) |
   | Terminate after | 60 minutes of inactivity |
   | Unity Catalog | Enabled (Single user access mode) |

3. Click **Create compute** and wait for the cluster to reach the **Running** state.

> **Access mode requirement**: Column-level security and row filters require **Single user** or **Shared** access mode. Do not use the legacy **No isolation shared** mode.

### 1.5 Install Required Libraries

All required packages are part of DBR 14.3 LTS. No additional libraries are needed.

```python
# Cell 1: verify Unity Catalog access
print(spark.sql("SELECT current_catalog(), current_database()").collect())
spark.sql("USE CATALOG eeg_lakehouse")
print("Unity Catalog active. Current catalog: eeg_lakehouse")
```

### 1.6 Open and Attach the Notebook

1. In the left sidebar, click **Repos** → `databricks-eeg-lakehouse-lab` → `notebooks`.
2. Click `day13_unity_catalog.py` to open it.
3. Click **Connect** (top-right) → select `eeg-lab-day13`.
4. Confirm the cluster name appears in the toolbar.

---

## Section 2: Learning Objectives

| Objective | Exam domain mapping |
|---|---|
| Understand Unity Catalog three-level namespace | Domain 1 — Lakehouse Platform |
| Grant and revoke privileges at table, schema, and catalog levels | Domain 5 — Security and Governance |
| Apply column masking for PII columns | Domain 5 — Column-level security |
| Apply row-level security with row filters | Domain 5 — Row-level security |
| Query the Unity Catalog system tables for lineage | Domain 1 — Data lineage |
| Tag sensitive columns with column tags | Domain 5 — Data classification |
| Configure external locations for cloud storage access | Domain 1 — Storage credentials |

---

## Section 3: Background

### Unity Catalog Object Hierarchy

| Level | Object type | Example | Governed by |
|---|---|---|---|
| 1 | Metastore | `unity_metastore` | Account admin |
| 2 | Catalog | `eeg_lakehouse` | Catalog owner |
| 3 | Schema (database) | `eeg_lakehouse.silver` | Schema owner |
| 4 | Table / View / Function | `eeg_lakehouse.silver.eeg_silver_advanced` | Table owner |

### Unity Catalog Privilege Types

| Privilege | Applies to | Description |
|---|---|---|
| `USE CATALOG` | Catalog | Required to access any object in the catalog |
| `USE SCHEMA` | Schema | Required to access any object in the schema |
| `SELECT` | Table / View | Read rows from the object |
| `MODIFY` | Table | Insert, update, delete rows |
| `CREATE TABLE` | Schema | Create tables within the schema |
| `CREATE SCHEMA` | Catalog | Create schemas within the catalog |
| `ALL PRIVILEGES` | Any | Grants all applicable privileges |

---

## Section 4: Part 1 — Privilege Management

### Step 1 — Create Schemas for the EEG Lakehouse

```sql
-- Cell 2: ensure all required schemas exist with comments
CREATE SCHEMA IF NOT EXISTS eeg_lakehouse.bronze
    COMMENT 'Bronze layer: raw EEG recordings ingested from the PhysioNet landing zone.';

CREATE SCHEMA IF NOT EXISTS eeg_lakehouse.silver
    COMMENT 'Silver layer: validated and cleaned EEG recordings.';

CREATE SCHEMA IF NOT EXISTS eeg_lakehouse.gold
    COMMENT 'Gold layer: aggregated EEG features for analytics and ML.';

CREATE SCHEMA IF NOT EXISTS eeg_lakehouse.monitoring
    COMMENT 'Monitoring layer: audit logs, quality metrics, and pipeline health views.';

SHOW SCHEMAS IN eeg_lakehouse;
```

---

### Step 2 — Grant Read Access to the Data Science Group

```sql
-- Cell 3: grant read-only access to data scientists
-- Replace 'data_scientists' with the actual group name in your Unity Catalog metastore
GRANT USE CATALOG ON CATALOG eeg_lakehouse TO `data_scientists`;
GRANT USE SCHEMA  ON SCHEMA  eeg_lakehouse.gold TO `data_scientists`;
GRANT SELECT      ON TABLE   eeg_lakehouse.gold.eeg_gold_patient_summary TO `data_scientists`;

-- Verify the grant
SHOW GRANTS ON TABLE eeg_lakehouse.gold.eeg_gold_patient_summary;
```

---

### Step 3 — Grant Write Access to the Engineering Group

```sql
-- Cell 4: grant write access to data engineers
GRANT USE CATALOG  ON CATALOG eeg_lakehouse             TO `data_engineers`;
GRANT USE SCHEMA   ON SCHEMA  eeg_lakehouse.silver       TO `data_engineers`;
GRANT USE SCHEMA   ON SCHEMA  eeg_lakehouse.bronze       TO `data_engineers`;
GRANT SELECT       ON SCHEMA  eeg_lakehouse.silver       TO `data_engineers`;
GRANT MODIFY       ON SCHEMA  eeg_lakehouse.silver       TO `data_engineers`;
GRANT CREATE TABLE ON SCHEMA  eeg_lakehouse.silver       TO `data_engineers`;

SHOW GRANTS ON SCHEMA eeg_lakehouse.silver;
```

---

### Step 4 — Revoke Privileges

```sql
-- Cell 5: revoke a privilege example
REVOKE MODIFY ON SCHEMA eeg_lakehouse.silver FROM `data_scientists`;

-- Confirm the revocation
SHOW GRANTS ON SCHEMA eeg_lakehouse.silver;
```

---

## Section 5: Part 2 — Column-Level Security

### Step 5 — Add PII Tags to Sensitive Columns

```sql
-- Cell 6: tag the patient_id column as PII
ALTER TABLE eeg_lakehouse.silver.eeg_silver_advanced
    ALTER COLUMN patient_id SET TAGS ('pii' = 'true', 'sensitivity' = 'high');

-- Verify the tag
DESCRIBE EXTENDED eeg_lakehouse.silver.eeg_silver_advanced patient_id;
```

---

### Step 6 — Create a Column Masking Function

Column masking uses a SQL function to replace sensitive column values based on the calling user’s group membership.

```sql
-- Cell 7: create a column masking function for patient_id
CREATE OR REPLACE FUNCTION eeg_lakehouse.silver.mask_patient_id(patient_id STRING)
    RETURN
        CASE
            WHEN is_member('data_engineers') THEN patient_id
            ELSE CONCAT(SUBSTR(patient_id, 1, 1), '***', SUBSTR(patient_id, -1, 1))
        END;
```

---

### Step 7 — Apply the Masking Function to the Column

```sql
-- Cell 8: apply the column mask to patient_id
ALTER TABLE eeg_lakehouse.silver.eeg_silver_advanced
    ALTER COLUMN patient_id
    SET MASK eeg_lakehouse.silver.mask_patient_id;

-- Test: as a non-engineer user, patient_id will appear masked
SELECT patient_id, signal_quality, has_seizure
FROM eeg_lakehouse.silver.eeg_silver_advanced
LIMIT 5;
```

Expected output for non-engineer users:

| patient_id | signal_quality | has_seizure |
|---|---|---|
| P***1 | 0.87 | 0 |
| P***4 | 0.92 | 1 |

---

### Step 8 — Drop the Column Mask

```sql
-- Cell 9: remove the column mask (as table owner or data engineer)
ALTER TABLE eeg_lakehouse.silver.eeg_silver_advanced
    ALTER COLUMN patient_id DROP MASK;

-- Confirm mask is removed
DESCRIBE EXTENDED eeg_lakehouse.silver.eeg_silver_advanced patient_id;
```

---

## Section 6: Part 3 — Row-Level Security with Row Filters

### Step 9 — Create a Row Filter Function

```sql
-- Cell 10: row filter — restrict non-engineers to rows where signal_quality >= 0.7
CREATE OR REPLACE FUNCTION eeg_lakehouse.silver.filter_by_quality(signal_quality DOUBLE)
    RETURN
        CASE
            WHEN is_member('data_engineers') THEN TRUE
            ELSE signal_quality >= 0.7
        END;
```

---

### Step 10 — Apply the Row Filter to the Silver Table

```sql
-- Cell 11: attach the row filter to the Silver table
ALTER TABLE eeg_lakehouse.silver.eeg_silver_advanced
    SET ROW FILTER eeg_lakehouse.silver.filter_by_quality ON (signal_quality);

-- Test: data scientists see only high-quality rows
SELECT COUNT(*) AS visible_rows FROM eeg_lakehouse.silver.eeg_silver_advanced;
```

> **Exam note**: Row filters are applied transparently at query time. Users are unaware of the filter unless the table metadata explicitly documents it. The filter function must be owned by the table owner or a privileged user.

---

### Step 11 — Drop the Row Filter

```sql
-- Cell 12: remove the row filter
ALTER TABLE eeg_lakehouse.silver.eeg_silver_advanced DROP ROW FILTER;

SELECT COUNT(*) AS visible_rows FROM eeg_lakehouse.silver.eeg_silver_advanced;
```

---

## Section 7: Part 4 — Data Lineage via System Tables

### Step 12 — Enable and Query the Lineage System Tables

```python
# Cell 13: query Unity Catalog system tables for table lineage
# System tables are available under the 'system' catalog in Unity Catalog
lineage_df = spark.sql("""
    SELECT
        source_table_full_name,
        target_table_full_name,
        entity_type,
        created_by
    FROM system.access.table_lineage
    WHERE target_table_full_name LIKE 'eeg_lakehouse%'
    ORDER BY target_table_full_name
""")

display(lineage_df)
```

### Step 13 — Query Column-Level Lineage

```python
# Cell 14: column-level lineage — trace where each output column originated
column_lineage_df = spark.sql("""
    SELECT
        source_table_full_name,
        source_column_name,
        target_table_full_name,
        target_column_name
    FROM system.access.column_lineage
    WHERE target_table_full_name LIKE 'eeg_lakehouse.gold%'
    ORDER BY target_table_full_name, target_column_name
""")

display(column_lineage_df)
```

### Step 14 — Audit Access Logs

```python
# Cell 15: audit who has accessed the Gold feature table in the past 7 days
access_df = spark.sql("""
    SELECT
        user_identity.email   AS user_email,
        request_params.table  AS table_name,
        action_name,
        event_time
    FROM system.access.audit
    WHERE action_name IN ('commandSubmit', 'getTable', 'runCommand')
      AND request_params.table LIKE 'eeg_lakehouse.gold%'
      AND event_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    ORDER BY event_time DESC
    LIMIT 50
""")

display(access_df)
```

---

## Section 8: Part 5 — External Locations and Storage Credentials

### Step 15 — Create a Storage Credential

A storage credential delegates cloud storage access to Unity Catalog.

```sql
-- Cell 16: create a storage credential using an Azure managed identity
-- Run this as a metastore admin in a SQL warehouse or admin notebook
CREATE STORAGE CREDENTIAL eeg_adls_credential
    WITH AZURE_MANAGED_IDENTITY
    (CREDENTIAL 'eeg-databricks-connector')
    COMMENT 'Credential for accessing the EEG lakehouse ADLS Gen2 storage account.';

SHOW STORAGE CREDENTIALS;
```

### Step 16 — Create an External Location

```sql
-- Cell 17: create an external location pointing to the EEG ADLS container
CREATE EXTERNAL LOCATION eeg_lakehouse_location
    URL 'abfss://eeg-lakehouse@<storage-account>.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL eeg_adls_credential)
    COMMENT 'External location for all EEG lakehouse data on ADLS Gen2.';

-- Validate the external location
VALIDATE STORAGE CREDENTIAL eeg_adls_credential
    ON LOCATION 'abfss://eeg-lakehouse@<storage-account>.dfs.core.windows.net/';
```

> Replace `<storage-account>` with the actual ADLS Gen2 account name. On AWS, use `s3://` URIs and an IAM role credential.

---

## Section 9: Exam Reference Tables

### Unity Catalog Governance Operations

| Operation | SQL syntax | Notes |
|---|---|---|
| Grant privilege | `GRANT <privilege> ON <object_type> <name> TO <principal>` | Principal can be a user, group, or service principal |
| Revoke privilege | `REVOKE <privilege> ON <object_type> <name> FROM <principal>` | Does not cascade to downstream objects |
| Show grants | `SHOW GRANTS ON <object_type> <name>` | Lists all principals with access |
| Column mask | `ALTER TABLE ... ALTER COLUMN ... SET MASK <function>` | Function receives the column value; returns masked value |
| Drop mask | `ALTER TABLE ... ALTER COLUMN ... DROP MASK` | Removes masking; unmasked values become visible |
| Row filter | `ALTER TABLE ... SET ROW FILTER <function> ON (<cols>)` | Filter applied transparently at query time |
| Drop row filter | `ALTER TABLE ... DROP ROW FILTER` | Removes filter; all rows become visible |

### System Table Reference

| System table | Purpose | Key columns |
|---|---|---|
| `system.access.table_lineage` | Track table-level read/write lineage | `source_table_full_name`, `target_table_full_name` |
| `system.access.column_lineage` | Track column-level transformation lineage | `source_column_name`, `target_column_name` |
| `system.access.audit` | Access log for all workspace actions | `user_identity`, `action_name`, `event_time` |
| `system.billing.usage` | DBU consumption per cluster and job | `usage_quantity`, `sku_name`, `usage_date` |

### Certified Professional Exam Domain Mapping — Day 13 Topics

| Topic | Professional exam domain |
|---|---|
| Three-level Unity Catalog namespace | Domain 1 — Lakehouse Platform |
| `GRANT` / `REVOKE` privilege model | Domain 5 — Security and Governance |
| Column masking functions | Domain 5 — Column-level security |
| Row filter functions | Domain 5 — Row-level security |
| Table and column lineage system tables | Domain 1 — Data lineage |
| External locations and storage credentials | Domain 1 — Storage access |

---

## Section 10: Self-Check Questions

Answer each question before proceeding to Day 14.

1. What three privilege grants are required for a user to query a table in Unity Catalog?
2. How does a column masking function receive access to the caller’s identity?
3. What is the difference between `REVOKE` and `DROP MASK`?
4. Where are Unity Catalog table lineage records stored, and what system table exposes them?
5. What is the purpose of a storage credential in Unity Catalog?
6. Why must the access mode be `Single user` or `Shared` (not `No isolation shared`) to enforce column masks?

**Reference answers:**

1. The user requires `USE CATALOG` on the catalog, `USE SCHEMA` on the schema, and `SELECT` on the table (or a parent object that inherits SELECT). All three must be granted for the query to succeed.
2. Inside the masking function, `is_member('group_name')` evaluates the group membership of the user executing the query at runtime. Unity Catalog injects the calling user’s identity into the function context automatically.
3. `REVOKE` removes a privilege grant from a principal (e.g., removes the ability to read a table). `DROP MASK` removes the masking function from a column, exposing the raw value to all users who have `SELECT` privilege.
4. Lineage is recorded by Unity Catalog automatically and exposed through the `system.access.table_lineage` and `system.access.column_lineage` system tables in the `system` catalog.
5. A storage credential is a Unity Catalog object that holds cloud identity credentials (e.g., an Azure managed identity or an AWS IAM role). It is referenced by external locations to delegate access to cloud storage without embedding credentials in notebooks or jobs.
6. The `No isolation shared` access mode does not enforce Unity Catalog security policies at the cluster level. Column masks and row filters are security constructs that require the Unity Catalog access control engine, which is only active in `Single user` and `Shared` modes.

---

## Section 11: Day 13 Summary

| Artifact | Tool | Layer | Exam domain |
|---|---|---|---|
| Schema privilege grants | `GRANT` / `REVOKE` SQL | All schemas | Domain 5 |
| `patient_id` PII column tag | `ALTER TABLE ... SET TAGS` | Silver | Domain 5 |
| `mask_patient_id` masking function | Unity Catalog SQL function | Silver | Domain 5 |
| `filter_by_quality` row filter | Unity Catalog SQL function | Silver | Domain 5 |
| Table-level lineage query | `system.access.table_lineage` | Monitoring | Domain 1 |
| Column-level lineage query | `system.access.column_lineage` | Monitoring | Domain 1 |
| Access audit query | `system.access.audit` | Monitoring | Domain 1 |
| Storage credential + external location | Unity Catalog Admin SQL | Storage | Domain 1 |

**Next**: Day 14 covers Delta Lake performance optimisation — Z-Ordering, file compaction, data skipping, predictive I/O, and liquid clustering.
