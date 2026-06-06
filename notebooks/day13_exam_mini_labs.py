# Databricks notebook source
# notebooks/day13_exam_mini_labs.py
# =============================================================================
# DAY 13 - Exam Pattern Mini-Labs: CTAS, MERGE INTO, Time Travel, COPY INTO
# =============================================================================
# EXAM DOMAINS: SQL patterns, Delta operations, incremental data management
# These patterns appear DIRECTLY in exam questions - practice each one!
# =============================================================================

# COMMAND ----------
# %md
# ## Day 13: Exam Pattern Mini-Labs
# Run each section independently. All patterns use EEG Gold tables.
#
# | Pattern | Exam frequency | EEG use case |
# |---------|---------------|---------------|
# | CTAS | Very high | Create Gold from Silver |
# | INSERT INTO | High | Append new nightly data |
# | MERGE INTO | Very high | Upsert spindle features |
# | Time travel | High | Reproduce ML training set |
# | COPY INTO | Medium | One-time bulk load |
# | RESTORE | Medium | Rollback bad write |

GOLD_TABLE = "eeg_lakehouse.gold.eeg_features"
SILVER_EVENTS = "eeg_lakehouse.silver.eeg_events"

# COMMAND ----------
# %md ### Lab 1: CREATE TABLE AS SELECT (CTAS)
# EXAM NOTE:
# - CTAS creates a NEW Delta table from a query result
# - Schema is INFERRED from the query (no explicit schema needed)
# - Always creates a MANAGED Delta table (unless LOCATION specified)
# - Equivalent to: CREATE TABLE t AS SELECT ...

# %sql
# -- CTAS: create Silver summary from events table
# CREATE OR REPLACE TABLE eeg_lakehouse.gold.spindle_summary AS
# SELECT
#     subject_id,
#     session,
#     recording_date,
#     COUNT(CASE WHEN event_type = 'spindle' THEN 1 END)           AS spindle_count,
#     AVG(CASE WHEN event_type = 'spindle' THEN duration_s END)    AS avg_spindle_dur,
#     AVG(CASE WHEN event_type = 'spindle' THEN amplitude_uv END)  AS avg_spindle_amp,
#     COUNT(CASE WHEN event_type = 'slow_oscillation' THEN 1 END)  AS so_count
# FROM eeg_lakehouse.silver.eeg_events
# GROUP BY subject_id, session, recording_date;
#
# -- Verify
# DESCRIBE HISTORY eeg_lakehouse.gold.spindle_summary;

print("Lab 1: CTAS pattern - run SQL above in a %sql cell")

# COMMAND ----------
# %md ### Lab 2: INSERT INTO
# EXAM NOTE:
# - INSERT INTO appends rows to existing table
# - Schema must match (unlike overwrite which can change schema with option)
# - Use for daily incremental loads into Bronze/Silver

# %sql
# -- Simulate a new night's data arriving for subject SC4003
# INSERT INTO eeg_lakehouse.gold.eeg_features
# VALUES
#   ('SC4003', 'night1', '2024-01-12', 4.2, 0.85, 55.0, 3.1, -90.0, 1.35,
#    0.42, 0.55, 0.12, 0.20, NULL, NULL, NULL, NULL, 1);
#
# -- INSERT INTO from SELECT (more realistic)
# INSERT INTO eeg_lakehouse.gold.eeg_features
# SELECT * FROM eeg_lakehouse.gold.spindle_summary
# WHERE recording_date = '2024-01-13';

print("Lab 2: INSERT INTO pattern")

# COMMAND ----------
# %md ### Lab 3: MERGE INTO (Upsert)
# EXAM NOTE:
# - MERGE INTO is the CANONICAL upsert pattern in Delta Lake
# - Matches source rows to target rows using ON condition
# - WHEN MATCHED THEN UPDATE: update existing records
# - WHEN NOT MATCHED THEN INSERT: insert new records
# - WHEN NOT MATCHED BY SOURCE THEN DELETE: delete rows absent from source
# - This is the most important SQL pattern on the exam!

# %sql
# MERGE INTO eeg_lakehouse.gold.eeg_features AS target
# USING (
#   -- New/updated spindle data for today's run
#   SELECT
#     subject_id, session, recording_date,
#     spindle_count, spindle_density, spindle_amplitude_mean,
#     so_count, so_density, pac_mi,
#     sigma_power_mean, delta_power_mean, beta_power_mean, theta_power_mean,
#     spindle_duration_mean, spindle_amplitude_std, so_neg_peak_mean, so_duration_mean,
#     memory_score
#   FROM eeg_lakehouse.gold.eeg_features_staging  -- staging table from pipeline run
# ) AS source
# ON target.subject_id = source.subject_id
#    AND target.session = source.session
#    AND target.recording_date = source.recording_date
# WHEN MATCHED THEN
#   UPDATE SET
#     target.spindle_count          = source.spindle_count,
#     target.spindle_density        = source.spindle_density,
#     target.spindle_amplitude_mean = source.spindle_amplitude_mean,
#     target.memory_score           = source.memory_score
# WHEN NOT MATCHED THEN
#   INSERT *;   -- insert all columns from source

print("Lab 3: MERGE INTO pattern - the most important exam SQL pattern")

# COMMAND ----------
# %md ### Lab 4: Time Travel
# EXAM NOTE:
# - Delta keeps full history (limited by VACUUM retention)
# - VERSION AS OF <n>: query by version number
# - TIMESTAMP AS OF <ts>: query by timestamp string
# - Useful for: ML reproducibility, auditing, rollback

# %sql
# -- Query version 0 (initial write)
# SELECT * FROM eeg_lakehouse.gold.eeg_features VERSION AS OF 0;
#
# -- Query by timestamp
# SELECT * FROM eeg_lakehouse.gold.eeg_features
# TIMESTAMP AS OF '2024-01-10 00:00:00';
#
# -- Python API: time travel with DataFrameReader
# df_v0 = spark.read.format("delta") \
#     .option("versionAsOf", 0) \
#     .table("eeg_lakehouse.gold.eeg_features")
#
# # Using @v0 syntax in SQL (Databricks shorthand)
# spark.sql("SELECT * FROM eeg_lakehouse.gold.eeg_features@v0")

print("Lab 4: Time Travel")

# COMMAND ----------
# %md ### Lab 5: RESTORE TABLE
# EXAM NOTE:
# - RESTORE reverts a table to a previous version
# - Creates a NEW version that matches the target version's data
# - Does NOT delete history (unlike VACUUM)
# - Use when: bad data written, wrong DELETE ran, etc.

# %sql
# -- Restore to version 1 (undo the MERGE from Lab 3)
# RESTORE TABLE eeg_lakehouse.gold.eeg_features TO VERSION AS OF 1;
#
# -- Verify the restore created a new version
# DESCRIBE HISTORY eeg_lakehouse.gold.eeg_features;

print("Lab 5: RESTORE TABLE")

# COMMAND ----------
# %md ### Lab 6: COPY INTO
# EXAM NOTE:
# - COPY INTO loads files from cloud storage into Delta table
# - IDEMPOTENT: tracks which files were loaded, won't re-load them
# - Alternative to Auto Loader for one-time or less-frequent bulk loads
# - Use Auto Loader (cloudFiles) for continuous/incremental; COPY INTO for batch

# %sql
# COPY INTO eeg_lakehouse.bronze.raw_eeg_files
# FROM '/Volumes/eeg_lakehouse/bronze/raw_edf/'
# FILEFORMAT = BINARYFILE
# -- For CSV:
# -- FILEFORMAT = CSV
# -- FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# -- COPY_OPTIONS ('mergeSchema' = 'true');

print("Lab 6: COPY INTO")

# COMMAND ----------
# %md ### Lab 7: Generated Columns
# EXAM NOTE:
# - Generated columns auto-compute from other columns
# - Stored like regular columns (not computed at query time)
# - Delta checks constraint at write time

# %sql
# CREATE TABLE eeg_lakehouse.gold.eeg_features_gen (
#   subject_id     STRING NOT NULL,
#   session        STRING NOT NULL,
#   recording_date DATE,
#   recording_year INT    GENERATED ALWAYS AS (YEAR(recording_date)),
#   spindle_density DOUBLE,
#   sigma_power_mean DOUBLE
# )
# USING DELTA
# PARTITIONED BY (recording_year);  -- partition by generated column!

print("Lab 7: Generated Columns")

# COMMAND ----------
# %md ### Lab 8: Table Constraints
# EXAM NOTE:
# - Delta supports NOT NULL and CHECK constraints
# - Enforced at write time (INSERT/UPDATE/MERGE fail if violated)
# - Good for data quality without DLT

# %sql
# -- Add NOT NULL constraint
# ALTER TABLE eeg_lakehouse.gold.eeg_features
#   ALTER COLUMN subject_id SET NOT NULL;
#
# -- Add CHECK constraint
# ALTER TABLE eeg_lakehouse.gold.eeg_features
#   ADD CONSTRAINT valid_spindle_density
#   CHECK (spindle_density >= 0 AND spindle_density <= 50);
#
# -- View constraints
# DESCRIBE DETAIL eeg_lakehouse.gold.eeg_features;

print("Lab 8: Table Constraints")

# COMMAND ----------
# %md
# ## Day 13 Summary: Must-Know SQL Patterns
#
# | Pattern | Key syntax | When to use |
# |---------|-----------|-------------|
# | CTAS | `CREATE TABLE t AS SELECT ...` | Initial table creation |
# | INSERT INTO | `INSERT INTO t SELECT ...` | Append new rows |
# | MERGE INTO | `MERGE INTO t USING s ON ... WHEN MATCHED ...` | Upsert (most important!) |
# | Time travel | `SELECT * FROM t VERSION AS OF 0` | Audit, ML reproducibility |
# | RESTORE | `RESTORE TABLE t TO VERSION AS OF n` | Rollback bad writes |
# | COPY INTO | `COPY INTO t FROM path FILEFORMAT=...` | Idempotent bulk load |
# | Generated col | `col INT GENERATED ALWAYS AS (expr)` | Auto-computed columns |
# | CHECK constraint | `ADD CONSTRAINT name CHECK (expr)` | Data quality |

print("Day 13 complete! Proceed to Day 14: Portfolio polish.")
