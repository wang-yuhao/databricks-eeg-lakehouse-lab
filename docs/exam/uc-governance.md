# Unity Catalog — Data Governance Exam Cheatsheet

> **Day 8 · Exam Domain: Data Governance & Security (~9% of exam)**
> Companion to `notebooks/day08_unity_catalog.py` and `src/` UC patterns.

---

## 1. Unity Catalog Object Hierarchy

```
Metastore
└── Catalog          (e.g. eeg_catalog)
    └── Schema       (e.g. silver, gold)
        ├── Table    (managed or external)
        ├── View
        └── Function (Python/SQL UDF)
```

**Key rule:** `catalog.schema.table` — always three-part names in UC.

---

## 2. GRANT / REVOKE — Privilege Model

### Object-level privileges

| Privilege | Applies to | Description |
|-----------|-----------|-------------|
| `USE CATALOG` | Catalog | Required to use any object in the catalog |
| `USE SCHEMA` | Schema | Required to access tables/views in schema |
| `SELECT` | Table / View | Read access |
| `MODIFY` | Table | INSERT / UPDATE / DELETE |
| `CREATE TABLE` | Schema | Create new tables in the schema |
| `ALL PRIVILEGES` | Any | Grants all applicable privileges |

### Common exam patterns

```sql
-- Grant analyst read-only access to Silver EEG data
GRANT USE CATALOG ON CATALOG eeg_catalog TO analysts;
GRANT USE SCHEMA ON SCHEMA eeg_catalog.silver TO analysts;
GRANT SELECT ON TABLE eeg_catalog.silver.eeg_epochs TO analysts;

-- Grant data engineer write access
GRANT MODIFY ON TABLE eeg_catalog.silver.eeg_epochs TO data_engineers;

-- Revoke SELECT from a group
REVOKE SELECT ON TABLE eeg_catalog.gold.subject_features FROM interns;

-- Show all grants on a table
SHOW GRANTS ON TABLE eeg_catalog.silver.eeg_epochs;
```

**Exam trap:** You must grant `USE CATALOG` AND `USE SCHEMA` before table-level grants
work — forgetting either causes access denied even if table SELECT is granted.

---

## 3. Row-Level Security with Row Filters

Row filters let you restrict which rows different users/groups can see.
Implemented as a SQL function returning a boolean condition.

```sql
-- Create row filter function: each analyst only sees their own site group
CREATE OR REPLACE FUNCTION eeg_catalog.silver.site_row_filter(site_group STRING)
RETURNS BOOLEAN
RETURN IS_ACCOUNT_GROUP_MEMBER(site_group);

-- Attach filter to table
ALTER TABLE eeg_catalog.silver.eeg_epochs
SET ROW FILTER eeg_catalog.silver.site_row_filter ON (site_group);

-- Remove row filter
ALTER TABLE eeg_catalog.silver.eeg_epochs DROP ROW FILTER;
```

**EEG research use case:** Each EEG recording site (LMU Munich, Charité Berlin)
only sees their subjects — zero-code access control with full audit trail.

---

## 4. Column Masking

Column masks dynamically replace column values for unauthorised users.

```sql
-- Mask subject PII: show only first 4 chars of subject_id to non-admins
CREATE OR REPLACE FUNCTION eeg_catalog.silver.mask_subject_id(subject_id STRING)
RETURNS STRING
RETURN CASE
  WHEN IS_ACCOUNT_GROUP_MEMBER('eeg_admins') THEN subject_id
  ELSE CONCAT(LEFT(subject_id, 4), '****')
END;

-- Apply mask
ALTER TABLE eeg_catalog.silver.eeg_epochs
ALTER COLUMN subject_id
SET MASK eeg_catalog.silver.mask_subject_id;

-- Drop mask
ALTER TABLE eeg_catalog.silver.eeg_epochs
ALTER COLUMN subject_id DROP MASK;
```

**GDPR compliance note:** Column masks are enforced at query time and logged
in the Unity Catalog audit log — satisfying GDPR Article 25 (data minimisation).

---

## 5. Audit Logging

Unity Catalog automatically logs ALL SELECT/MODIFY operations to:
```
system.access.audit_log
```

No custom code required. Fields include:
- `event_time` — when the query ran
- `user_name` — who ran it
- `request_params.table_full_name` — which table was accessed
- `response.status_code` — success or failure

```sql
-- Find all access to EEG Silver table in last 24 hours
SELECT event_time, user_name, action_name, request_params
FROM system.access.audit_log
WHERE request_params.table_full_name = 'eeg_catalog.silver.eeg_epochs'
  AND event_time >= NOW() - INTERVAL 1 DAY
ORDER BY event_time DESC;
```

---

## 6. External Locations & Credentials

```sql
-- Create storage credential (points to an Azure Managed Identity or AWS IAM role)
CREATE STORAGE CREDENTIAL eeg_adls_credential
WITH AZURE_MANAGED_IDENTITY (CREDENTIAL_NAME = 'eeg-mi');

-- Create external location backed by the credential
CREATE EXTERNAL LOCATION eeg_landing
URL 'abfss://landing@eegdatalake.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL eeg_adls_credential);

-- Validate access
VALIDATE STORAGE CREDENTIAL eeg_adls_credential;
```

---

## 7. Volumes (UC-native file storage)

Volumes are UC-managed file-system paths — no Delta table required.

```sql
-- Create a managed volume (storage managed by UC)
CREATE VOLUME eeg_catalog.bronze.raw_edf_files;

-- Write files to volume path
-- /Volumes/eeg_catalog/bronze/raw_edf_files/subject_001.edf

-- Create external volume (you own the storage path)
CREATE EXTERNAL VOLUME eeg_catalog.bronze.edf_archive
URL 'abfss://archive@eegdatalake.dfs.core.windows.net/edf/';
```

---

## 8. Common Exam Q&A

| Question | Answer |
|----------|--------|
| What is the minimum privilege to read a UC table? | `USE CATALOG` + `USE SCHEMA` + `SELECT` |
| Where are UC audit logs stored? | `system.access.audit_log` |
| How do you enforce GDPR column masking? | `ALTER TABLE ... ALTER COLUMN ... SET MASK <function>` |
| What replaces `SHOW GRANTS` for all objects? | `SHOW GRANTS ON <object_type> <object_name>` |
| Can DLT pipelines use UC? | Yes — set `catalog` in pipeline config |
| What is the difference between managed and external tables? | Managed: UC owns data; drop table = drop data. External: you own data; drop table = metadata only |
| How do row filters interact with aggregations? | Filter is applied BEFORE aggregation — users can only aggregate rows they can see |
| What privilege is needed for INSERT? | `MODIFY` |

---

## 9. EEG Lab Implementation Reference

| File | What it demonstrates |
|------|---------------------|
| `notebooks/day08_unity_catalog.py` | Live GRANT/REVOKE, row filter, column mask execution |
| `src/dlt/eeg_pipeline.py` | DLT tables registered in UC catalog |
| `docs/exam/uc-governance.md` | This cheatsheet |
| `docs/interview-star-stories.md` | STAR story: GDPR-compliant multi-site EEG access |

---

*Exam weight: ~9% · Study time: 2–3 hours · Practice: run `notebooks/day08_unity_catalog.py` end-to-end*
