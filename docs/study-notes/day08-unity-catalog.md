# Day 08 – Unity Catalog: Governance, Access Control & Data Lineage

## Overview
Unity Catalog (UC) is Databricks' unified data governance layer. It provides
centralised access control, data lineage, auditing, and fine-grained permissions
across all workspaces in an account. Critical exam topic for the Databricks DE exam.

---

## Core Concepts

### 1. Three-Level Namespace
```
catalog.schema.table

Examples:
  eeg_catalog.bronze.raw_signals
  eeg_catalog.silver.epochs
  eeg_catalog.gold.subject_features
  main.default.my_table
```
- **Catalog** – top-level container; maps to an organisation or project.
- **Schema (Database)** – groups related tables.
- **Table/View** – the data asset.

### 2. Securable Objects Hierarchy
```
Account
  └─ Metastore (one per region)
       ├─ Catalog
       │    ├─ Schema
       │    │    ├─ Table / View / Volume / Function
       │    │    └─ ...
       │    └─ ...
       ├─ Storage Credential
       └─ External Location
```

### 3. Privilege Model
```sql
-- Grant read on a table
GRANT SELECT ON TABLE eeg_catalog.silver.epochs TO `data-scientist@company.com`;

-- Grant schema-level create
GRANT CREATE TABLE ON SCHEMA eeg_catalog.gold TO `de-team`;

-- Grant all privileges on a catalog
GRANT ALL PRIVILEGES ON CATALOG eeg_catalog TO `admin-group`;

-- Revoke
REVOKE SELECT ON TABLE eeg_catalog.silver.epochs FROM `intern@company.com`;

-- Show grants
SHOW GRANTS ON TABLE eeg_catalog.silver.epochs;
```

**Key privileges:**
| Privilege | Applies to |
|---|---|
| `SELECT` | Tables, Views |
| `MODIFY` | Tables (INSERT, UPDATE, DELETE) |
| `CREATE TABLE` | Schemas |
| `CREATE SCHEMA` | Catalogs |
| `READ VOLUME` | Volumes |
| `WRITE VOLUME` | Volumes |
| `ALL PRIVILEGES` | Any securable |

### 4. Row-Level and Column-Level Security
```sql
-- Column masking: hide PII for non-privileged users
CREATE OR REPLACE FUNCTION eeg_catalog.masks.mask_subject_id(subject_id STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('clinicians') THEN subject_id
  ELSE 'REDACTED'
END;

ALTER TABLE eeg_catalog.silver.epochs
  ALTER COLUMN subject_id
  SET MASK eeg_catalog.masks.mask_subject_id;

-- Row filter: restrict to own subjects
CREATE OR REPLACE FUNCTION eeg_catalog.filters.my_subjects(subject_id STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('admins') OR subject_id = current_user();

ALTER TABLE eeg_catalog.silver.epochs
  SET ROW FILTER eeg_catalog.filters.my_subjects ON (subject_id);
```

### 5. Data Lineage
- UC automatically tracks column-level lineage across tables, notebooks, and jobs.
- Accessible via the Catalog Explorer UI or the REST API.
- No configuration needed; works automatically for Delta tables.

### 6. External Locations & Storage Credentials
```sql
-- Create a storage credential (admin only)
CREATE STORAGE CREDENTIAL eeg_adls
  WITH AZURE_MANAGED_IDENTITY (CONNECTOR_ID = '/subscriptions/.../connectors/eeg-connector');

-- Create an external location
CREATE EXTERNAL LOCATION eeg_raw_data
  URL 'abfss://raw@eegadls.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL eeg_adls);

-- Grant access to external location
GRANT READ FILES ON EXTERNAL LOCATION eeg_raw_data TO `de-team`;
```

---

## EEG / Neuroscience Context

### Why UC for Clinical EEG Data?
- **Subject privacy (GDPR):** Column masking on `subject_id`, `date_of_birth`.
- **Research access tiers:** Row filters to restrict researchers to consented datasets.
- **Audit trail:** Every query logged for IRB (ethics board) compliance.
- **Cross-team collaboration:** Clinicians see patient IDs; data scientists see anonymised data.

### EEG Catalog Design
```
eeg_catalog
  ├─ bronze
  │    └─ raw_signals, raw_annotations
  ├─ silver
  │    └─ epochs, events, subjects
  └─ gold
       └─ subject_features, nightly_summary, cohort_stats
```

---

## Exam-Focused Summary

| Topic | Key Fact |
|---|---|
| Namespace | `catalog.schema.table` (3 levels) |
| Metastore | One per region; shared across workspaces |
| GRANT syntax | `GRANT <privilege> ON <object> TO <principal>` |
| Column masking | `SET MASK <function>` on a column |
| Row filter | `SET ROW FILTER <function> ON (<col>)` |
| Lineage | Automatic; no config required |
| External location | Points to cloud storage path via storage credential |

---

## Key Files Created Today
| File | Purpose |
|---|---|
| `docs/exam/uc-governance.md` | Deep-dive UC reference doc |
| `notebooks/day08_unity_catalog.py` | Hands-on UC SQL notebook |
| `tests/test_uc_permissions.py` | Permission logic validation |

---

## Self-Check Questions
1. What are the three levels of the UC namespace?
2. What is the difference between `MODIFY` and `ALL PRIVILEGES`?
3. How does column masking differ from row-level security?
4. Why is automatic lineage valuable for a clinical research pipeline?
5. What is a storage credential and when do you need one?
6. How would you grant a data scientist read access to `silver.epochs` without exposing subject IDs?

---

## Further Reading
- [Unity Catalog Overview](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Column Masking](https://docs.databricks.com/en/tables/column-mask.html)
- [Row Filters](https://docs.databricks.com/en/tables/row-filter.html)
