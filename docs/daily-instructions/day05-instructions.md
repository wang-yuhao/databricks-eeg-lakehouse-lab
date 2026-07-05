# Day 5: Delta Lake Internals — Time Travel, OPTIMIZE, ZORDER, VACUUM

**Time estimate**: 2–3 hours  
**Prerequisite**: Day 4 completed; Silver table `eeg_lakehouse.silver.cleaned_epochs` exists

---

## Environment Setup

Complete every sub-section below before executing any notebook cell. A reader starting from a blank Databricks workspace must follow these steps in order. The lab assumes the Silver dataset already exists as the Unity Catalog table `eeg_lakehouse.silver.cleaned_epochs`; it does **not** assume a manually created folder such as `SILVER_PATH` exists. [page:1][web:8]

### 1. Create a GitHub Personal Access Token (PAT)

1. Navigate to [https://github.com/settings/tokens](https://github.com/settings/tokens) and sign in.
2. Click **Generate new token (classic)**.
3. Set **Note** to `databricks-eeg-lab`.
4. Set **Expiration** to `90 days`.
5. Select the following scopes: `repo` (full), `workflow`.
6. Click **Generate token** and copy the token value immediately — it will not be shown again.

### 2. Configure Databricks Git Integration

1. In your Databricks workspace, click your username in the top-right corner and select **User Settings**.
2. Click the **Git Integration** tab.
3. Set **Git provider** to `GitHub`.
4. Paste your GitHub PAT into the **Token** field.
5. Enter your GitHub username in the **Username** field.
6. Click **Save**.

### 3. Clone the Repository into Databricks Repos

1. In the left sidebar, click **Repos**.
2. Click **Add Repo**.
3. Enter the repository URL: `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab.git`.
4. Leave **Branch** as `main`.
5. Click **Create Repo**. The repository tree will appear under your user folder.

### 4. Create a Unity Catalog–Enabled Cluster

1. In the left sidebar, click **Compute**.
2. Click **Create compute**.
3. Configure the cluster using the reference table below.

| Parameter | Value |
|---|---|
| Cluster name | `eeg-lab-cluster` |
| Cluster mode | Single node |
| Databricks Runtime | **14.3 LTS** (Scala 2.12, Spark 3.5) |
| Node type | `Standard_DS3_v2` (Azure) or equivalent |
| Terminate after | 60 minutes of inactivity |
| Unity Catalog | Enabled (set **Access mode** to **Single user**) |

4. Expand **Advanced options** > **Spark** and add the following configuration:

```text
spark.databricks.delta.retentionDurationCheck.enabled false
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
```

5. Click **Create compute** and wait until the cluster status shows **Running**.

### 5. Install Required Libraries

1. Select your cluster from the Compute list and click the **Libraries** tab.
2. Click **Install new**.
3. Install the following libraries in order.

| Library source | Coordinates / Package name |
|---|---|
| PyPI | `mne==1.7.0` |
| PyPI | `scipy==1.13.0` |
| PyPI | `numpy==1.26.4` |

4. Wait for each library to show the status **Installed** before proceeding.

### 6. Open and Attach the Notebook

1. In the left sidebar, click **Repos** and navigate to `wang-yuhao/databricks-eeg-lakehouse-lab/notebooks/`.
2. Open the Day 5 Delta Lake notebook for this lab. If the filename in the repo differs from this document, use the notebook whose content covers time travel, OPTIMIZE, ZORDER, and VACUUM.
3. In the notebook toolbar, click the cluster dropdown and select `eeg-lab-cluster`.
4. Confirm the cluster indicator turns green before running any cell.

---

## Objectives

- Understand how Delta Lake tracks table versions through the transaction log.
- Perform time travel queries by version number and by timestamp.
- Use `OPTIMIZE` to compact small Parquet files.
- Apply `ZORDER` for column-level data co-location.
- Execute `VACUUM` to remove obsolete file versions.
- Inspect and interpret Delta table commit history.

---

## Background

Delta Lake stores table data in Parquet files and tracks changes through a transaction log that records file additions, removals, and metadata operations. The engine can reconstruct the state of a table at an earlier version by replaying the log, which is why time travel works. [web:11]

In this lab, you should treat the Unity Catalog table name as the source of truth:

```python
SILVER_TABLE = "eeg_lakehouse.silver.cleaned_epochs"
```

Use the catalog table directly throughout the notebook. Do **not** assume a `SILVER_PATH` constant exists or that previous project days created a manually named folder. [page:1][web:8]

---

## Step 1 — Define the Delta table handle

Create the canonical table reference and a Delta Lake handle:

```python
from delta.tables import DeltaTable

SILVER_TABLE = "eeg_lakehouse.silver.cleaned_epochs"
delta_table = DeltaTable.forName(spark, SILVER_TABLE)
```

`DeltaTable.forName()` binds directly to the registered Unity Catalog table, which is a better fit for this project than a hard-coded storage path. [web:11]

---

## Step 2 — Inspect table metadata with DESCRIBE DETAIL

Retrieve the current table metadata:

```sql
DESCRIBE DETAIL eeg_lakehouse.silver.cleaned_epochs;
```

Review the following fields carefully:

- `format`
- `location`
- `numFiles`
- `sizeInBytes`
- `partitionColumns`

`DESCRIBE DETAIL` is the supported Databricks command for inspecting table metadata such as location, file count, and total size. [web:8]

### Why this replaces `dbutils.fs.ls(SILVER_PATH)`

Earlier versions of this lab listed files directly from a storage path. That approach breaks when the table exists in Unity Catalog but no explicit project-level path constant was created. `DESCRIBE DETAIL` is more robust because it works from the table definition itself. [page:1][web:8]

---

## Step 3 — Inspect commit history with DESCRIBE HISTORY

Display the full commit history of the Silver table:

```sql
DESCRIBE HISTORY eeg_lakehouse.silver.cleaned_epochs;
```

This shows version numbers, timestamps, operations, users, and operation parameters for each commit. Delta Lake also exposes history programmatically through `DeltaTable.history()`. [web:11]

### Why this replaces raw `_delta_log` file reads

Path-based `_delta_log` exploration is useful for internals study, but it should not be the primary teaching path in a Unity Catalog lab. `DESCRIBE HISTORY` gives a stable, portable view of the same versioned behavior without depending on a manually known storage root. [web:11][web:8]

---

## Step 4 — Discover the physical storage location

Even though this lab is table-first, it is still helpful to see that the table has a physical backing location. Save the `location` value returned by `DESCRIBE DETAIL`.

Example pattern:

```python
detail_df = spark.sql(f"DESCRIBE DETAIL {SILVER_TABLE}")
detail = detail_df.collect()

table_location = detail["location"]
num_files_before = detail["numFiles"]
size_before = detail["sizeInBytes"]

print(f"Location: {table_location}")
print(f"Active files: {num_files_before}")
print(f"Size in bytes: {size_before}")
```

The key idea is that the physical location is discovered from metadata rather than assumed in advance. [web:8]

---

## Step 5 — Read the current version of the table

Load the latest state of the Silver table:

```python
current_df = spark.table(SILVER_TABLE)

display(current_df.limit(10))
print(f"Current row count: {current_df.count()}")
```

For the latest version, `spark.table(SILVER_TABLE)` is simpler and clearer than reading by path. [web:11]

---

## Step 6 — Read a historical version by version number

Use Delta Lake time travel to query an earlier snapshot:

```python
version_0_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .table(SILVER_TABLE)
)

print(f"Version 0 row count: {version_0_df.count()}")
display(version_0_df.limit(10))
```

You can replace `0` with any historical version shown by `DESCRIBE HISTORY`. Delta Lake supports version-based time travel directly from the registered table name. [web:11]

---

## Step 7 — Read a historical version by timestamp

You can also query the table as it existed at a specific timestamp:

```python
timestamp_df = (
    spark.read
    .format("delta")
    .option("timestampAsOf", "2026-06-21 23:30:00")
    .table(SILVER_TABLE)
)

print(f"Timestamp snapshot row count: {timestamp_df.count()}")
display(timestamp_df.limit(10))
```

Choose a timestamp that falls within the table history shown by `DESCRIBE HISTORY`. Timestamp-based time travel is useful when you know when a change happened but do not know the exact version number. [web:11]

---

## Step 8 — Compare versions

Compare the current table to an earlier version:

```python
current_count = spark.table(SILVER_TABLE).count()

version_0_count = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .table(SILVER_TABLE)
    .count()
)

print(f"Current count:   {current_count}")
print(f"Version 0 count: {version_0_count}")
```

If the counts are different, the table changed between those versions. Even if the counts match, the content or physical file layout may still differ because Delta versions track all committed changes, not only row-count changes. [web:11]

---

## Step 9 — Measure current file layout before OPTIMIZE

Capture file count and size before compaction:

```python
detail_before = spark.sql(f"DESCRIBE DETAIL {SILVER_TABLE}").collect()

files_before = detail_before["numFiles"]
size_before = detail_before["sizeInBytes"]

print(f"Files before OPTIMIZE: {files_before}")
print(f"Size before OPTIMIZE: {size_before}")
```

This replaces any helper that counted Parquet files from `SILVER_PATH`. Using `DESCRIBE DETAIL` keeps the notebook compatible with managed tables. [web:8]

---

## Step 10 — Run OPTIMIZE

Compact small files into fewer larger files:

```sql
OPTIMIZE eeg_lakehouse.silver.cleaned_epochs;
```

`OPTIMIZE` rewrites data files into a more efficient layout and creates a new Delta table version. It does not immediately delete the old superseded files from storage. [web:11]

---

## Step 11 — Verify file layout after OPTIMIZE

Check whether the active file count changed:

```python
detail_after_optimize = spark.sql(f"DESCRIBE DETAIL {SILVER_TABLE}").collect()

files_after_optimize = detail_after_optimize["numFiles"]
size_after_optimize = detail_after_optimize["sizeInBytes"]

print(f"Files after OPTIMIZE: {files_after_optimize}")
print(f"Size after OPTIMIZE: {size_after_optimize}")
```

The active file count often decreases after compaction, while the table remains logically equivalent. The old files still exist until vacuum removes obsolete versions. [web:8][web:11]

---

## Step 12 — Apply ZORDER

Run `OPTIMIZE` with `ZORDER BY` on a useful query column:

```sql
OPTIMIZE eeg_lakehouse.silver.cleaned_epochs
ZORDER BY (subject_id);
```

`ZORDER` is most effective for columns frequently used in selective filters, especially when those columns have enough distinct values to improve file skipping. It is usually not helpful for very low-cardinality fields such as a Boolean flag. [page:1][web:11]

### Example reasoning

- Effective candidate: `subject_id`
- Weak candidate: `is_artifact`

A high-cardinality field lets related rows cluster more effectively across files, improving data skipping. A near-binary field tends to appear in most files anyway, so clustering provides little benefit. [page:1]

---

## Step 13 — Review history after OPTIMIZE and ZORDER

Inspect the updated transaction history:

```python
(
    delta_table.history()
    .select("version", "timestamp", "operation", "operationParameters")
    .show(20, truncate=False)
)
```

You should see new history entries for the optimization operations. Delta history records the operation even though the table still appears as one logical dataset to readers. [web:11]

---

## Step 14 — Dry-run VACUUM

Preview which obsolete files would be removed:

```python
dry_run_df = spark.sql(f"VACUUM {SILVER_TABLE} RETAIN 0 HOURS DRY RUN")
display(dry_run_df)
print("Dry run complete — no files were deleted.")
```

A dry run is the safest way to preview the impact of vacuum. It reports the candidate files without removing them. [page:1]

> **Warning**: This lab disables the retention safety check to make the behavior visible for learning, but production workloads should normally keep the default retention window rather than using zero-hour retention. Databricks history and Delta internals guidance both emphasize that aggressive vacuum settings can permanently remove files needed for time travel. [web:11][page:1]

---

## Step 15 — Execute VACUUM

Run the actual cleanup:

```python
delta_table.vacuum(retentionHours=0)
print("VACUUM complete. Obsolete files for superseded versions have been removed.")
```

`VACUUM` removes obsolete data files that are no longer required by the current table state. The transaction history remains, but historical reads may fail if their underlying files were deleted. [web:11]

> **Production guidance**: Do not use `retentionHours=0` in production. Use at least the standard 7-day retention window unless you have a controlled exception and understand the recovery implications. [page:1][web:11]

---

## Step 16 — Verify time travel failure after VACUUM

Try to read an older version again:

```python
try:
    df_check = (
        spark.read
        .format("delta")
        .option("versionAsOf", 0)
        .table(SILVER_TABLE)
    )
    print(f"Version 0 accessible — record count: {df_check.count()}")
except Exception as exc:
    print("Expected failure: VACUUM removed the files required for version 0.")
    print(str(exc)[:300])
```

If vacuum removed the obsolete files for version 0, the historical query should fail even though the history metadata still exists. That distinction is central to understanding Delta Lake retention behavior. [web:11][page:1]

---

## Step 17 — Inspect final commit history

Review the final history again:

```python
(
    delta_table.history()
    .select("version", "timestamp", "operation")
    .show(20, truncate=False)
)
```

You should still see the older versions listed in the history output. `VACUUM` removes obsolete data files, not the logical history entries that describe past commits. [web:11][page:1]

---

## Optional internals extension

If your workspace permissions allow it, use the `location` field from `DESCRIBE DETAIL` to inspect the underlying storage directory and `_delta_log` manually. Treat this as an advanced exploration step, not a required dependency for the lab.

Example optional exploration:

```python
detail = spark.sql(f"DESCRIBE DETAIL {SILVER_TABLE}").collect()
table_location = detail["location"]

print(table_location)
```

Only continue to direct file inspection if that location is accessible in your workspace. This keeps the main lab portable across managed-table environments. [web:8]

---

## Exam Reflection Questions

Answer the following questions without referring to the notebook. These topics appear directly in Delta Lake and Databricks workflow discussions covered by this lab. [web:11]

1. Why is a Unity Catalog table name a safer primary reference than a manually hard-coded storage path in this project?
2. What information does `DESCRIBE DETAIL` return, and why is it useful before and after `OPTIMIZE`?
3. Write the PySpark expression to read version 3 of a Delta table by table name.
4. Why can `DESCRIBE HISTORY` still show old versions after `VACUUM` has made time travel fail for those versions?
5. Give one example of a good `ZORDER` column and one bad one. Explain why.
6. What is the risk of running `VACUUM retentionHours=0`?

### Reference answers

1. A table name is registered in the metastore and remains valid even if the physical storage location is abstracted or managed. A hard-coded path can be missing, renamed, or unknown in a managed Unity Catalog workflow. [web:8]
2. `DESCRIBE DETAIL` returns metadata such as storage location, file count, table size, format, and partition columns. It is useful for measuring how physical layout changes after maintenance operations. [web:8]
3. `spark.read.format("delta").option("versionAsOf", 3).table(SILVER_TABLE)` [web:11]
4. Because history is stored as Delta transaction metadata, while `VACUUM` removes obsolete underlying data files. The version metadata can remain visible even when the files needed to reconstruct that version are gone. [web:11]
5. Good: `subject_id` if analysts frequently filter by it and it has enough distinct values. Bad: `is_artifact` if it is only Boolean, because most files will still contain both values. [page:1]
6. It can permanently delete the obsolete files needed for historical reads, breaking time travel and limiting rollback options. [web:11][page:1]

---

## Day 5 Operation Reference

| Operation | Command | Side effect |
|---|---|---|
| Define Delta handle | `DeltaTable.forName(spark, SILVER_TABLE)` | Read-only [web:11] |
| Inspect table metadata | `DESCRIBE DETAIL eeg_lakehouse.silver.cleaned_epochs` | Read-only [web:8] |
| Inspect commit history | `DESCRIBE HISTORY eeg_lakehouse.silver.cleaned_epochs` | Read-only [web:11] |
| Read current table | `spark.table(SILVER_TABLE)` | Read-only [web:11] |
| Time travel by version | `.option("versionAsOf", N).table(SILVER_TABLE)` | Read-only [web:11] |
| Time travel by timestamp | `.option("timestampAsOf", "YYYY-MM-DD HH:MM:SS").table(SILVER_TABLE)` | Read-only [web:11] |
| Compact files | `OPTIMIZE eeg_lakehouse.silver.cleaned_epochs` | Creates a new version [web:11] |
| Co-locate data | `OPTIMIZE ... ZORDER BY (...)` | Creates a new version [web:11] |
| Preview cleanup | `VACUUM ... DRY RUN` | Read-only preview [page:1] |
| Remove obsolete files | `VACUUM ...` | Deletes obsolete files and can break time travel [web:11][page:1] |

---

## Improvement notes

This revised version intentionally changes the teaching model from **path-first** to **table-first**. That matches the prerequisite in the current file, which guarantees the Silver table exists but does not guarantee a project-created `SILVER_PATH` folder exists. [page:1]

It also separates three concepts more cleanly:
- **Table identity**: `SILVER_TABLE`
- **Metadata inspection**: `DESCRIBE DETAIL`
- **History inspection**: `DESCRIBE HISTORY` / `DeltaTable.history()`

That structure is more robust for Databricks and clearer for learners working with Unity Catalog managed tables. [web:8][web:11]
