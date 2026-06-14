# Professional Exam Upgrade Plan: From Associate to Professional (60% → 90%+)

> 🎯 **Goal:** Upgrade this repo from 60% Professional exam alignment to 90%+ in 7 days  
> 📊 **Current:** Strong Associate foundation; gaps in 5 Professional-only domains  
> 🗓️ **Timeline:** 7 focused days targeting critical gaps

---

## 🚨 Critical Issues (Must Fix Immediately)

### Issue #1: Domain 4 (Data Sharing & Federation) — COMPLETELY MISSING (5%)
This domain is **entirely absent** from the repo. It's a Professional-specific topic.

**Impact:** Automatic -5% on exam (3 questions lost)  
**Severity:** 🔴 CRITICAL

### Issue #2: Domain 5 (Monitoring & Alerting) — Very Weak (10%)
Basic Spark UI coverage; no DLT event logs, CDF, or alerting.

**Impact:** Lose ~6-8% (4-5 questions)  
**Severity:** 🔴 CRITICAL

### Issue #3: Domain 7 (Security & Compliance) — Very Weak (10%)
GRANT/REVOKE basics only; no row filters, column masks, or PII handling.

**Impact:** Lose ~6-7% (4 questions)  
**Severity:** 🟠 HIGH

**Total Gap:** These 3 domains alone = **17-20%** of exam (10-12 questions)

---

## 📅 7-Day Enhancement Plan

### Day 15: Data Sharing & Federation (🔴 Priority 1)
**Goal:** Add complete Domain 4 coverage (5% of exam)

#### Morning: Theory & Setup (3 hours)
1. **Study Delta Sharing:**
   - Read official docs: https://docs.databricks.com/en/delta-sharing/index.html
   - Understand shares, recipients, and providers
   - Review Unity Catalog integration

2. **Study Lakehouse Federation:**
   - Read: https://docs.databricks.com/en/query-federation/index.html
   - PostgreSQL, MySQL connector setup
   - Performance considerations

#### Afternoon: Code & Documentation (4 hours)
**Create:**
1. **`docs/study-notes/day16-delta-sharing.md`**
   ```markdown
   ## Delta Sharing Core Concepts
   - Share = collection of tables shared externally
   - Recipient = entity receiving data
   - Provider = organization sharing data
   
   ## Key Commands
   CREATE SHARE eeg_research_share;
   ALTER SHARE eeg_research_share ADD TABLE eeg_catalog.gold.subject_features;
   CREATE RECIPIENT external_lab;
   GRANT SELECT ON SHARE eeg_research_share TO RECIPIENT external_lab;
   
   ## Lakehouse Federation
   CREATE CONNECTION postgres_lab
   TYPE postgresql
   OPTIONS (host 'db.example.com', port '5432', user 'readonly');
   
   SELECT * FROM postgres_lab.public.external_subjects;
   ```

2. **`notebooks/day16_delta_sharing_federation.py`**
   - Delta Sharing: Create share for gold.subject_features
   - Federation: Query external PostgreSQL metadata table
   - Join federated data with internal Delta tables

3. **`docs/exam/delta-sharing-cheatsheet.md`**
   - All Delta Sharing SQL commands
   - Recipient types and permissions
   - Federation connector examples

**Evening: Practice (2 hours)**
- Run through 5 Delta Sharing scenarios
- Practice Federation queries
- Add unit tests

---

### Day 16: Monitoring & Observability (🔴 Priority 2)
**Goal:** Cover Spark UI, DLT event logs, CDF, alerting (10% of exam)

#### Morning: Spark UI Deep Dive (3 hours)
**Create: `docs/study-notes/day17-monitoring-observability.md`**

Topics:
- **Spark UI Stages Tab:** Understanding shuffle, skew, spill
- **Task Metrics:** Duration, GC time, shuffle read/write
- **DAG Visualization:** Identifying bottlenecks
- **Event Timeline:** Task scheduling insights

**Hands-on:**
- Run EEG preprocessing job
- Analyze Spark UI for shuffle metrics
- Identify data skew in subject_id partitions

#### Afternoon: DLT & Delta Observability (3 hours)
**Create: `notebooks/day17_spark_ui_analysis.py`**

```python
# Query DLT event logs
SELECT 
  timestamp, 
  event_type,
  details:flow_definition.output_dataset,
  details:flow_progress.data_quality.dropped_records
FROM event_log(TABLE(eeg_dlt_pipeline))
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;

# Enable Change Data Feed
ALTER TABLE eeg_catalog.gold.subject_features
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# Query CDF
SELECT * FROM table_changes('eeg_catalog.gold.subject_features', 1)
WHERE _change_type IN ('update_postimage', 'insert');
```

#### Evening: Alerting & Logging (2 hours)
**Create: `src/utils/monitoring.py`**

```python
import logging
import json
from pyspark.sql import SparkSession

class PipelineMonitor:
    def __init__(self, pipeline_name: str):
        self.logger = logging.getLogger(pipeline_name)
        
    def log_metric(self, metric_name: str, value: float):
        self.logger.info(json.dumps({
            "metric": metric_name,
            "value": value,
            "timestamp": datetime.now().isoformat()
        }))
    
    def send_alert(self, message: str, severity: str):
        # Email/webhook integration
        pass

# Usage in DLT
@dlt.table
def silver_epochs_with_monitoring():
    monitor = PipelineMonitor("silver_epochs")
    df = dlt.read_stream("bronze_eeg_raw")
    
    # Log metrics
    monitor.log_metric("input_rows", df.count())
    
    return df.filter("quality_flag = true")
```

---

### Day 17: Security & Compliance (🟠 Priority 3)
**Goal:** Row filters, column masks, PII protection (10% of exam)

#### Morning: Unity Catalog Security (3 hours)
**Expand: `docs/exam/uc-governance.md`**

Add sections:
1. **Row-Level Security:**
   ```sql
   CREATE FUNCTION filter_by_institution()
   RETURN current_user() IN ('researcher_a', 'researcher_b');
   
   ALTER TABLE eeg_catalog.gold.subject_features
   SET ROW FILTER filter_by_institution ON (institution_id);
   ```

2. **Column-Level Security:**
   ```sql
   CREATE FUNCTION mask_subject_name(name STRING)
   RETURN CASE 
     WHEN is_account_group_member('pii_access') THEN name
     ELSE '***REDACTED***'
   END;
   
   ALTER TABLE eeg_catalog.gold.subject_features
   ALTER COLUMN subject_name SET MASK mask_subject_name;
   ```

3. **PII Protection:**
   - Anonymization techniques
   - Hashing vs encryption
   - Tokenization patterns

#### Afternoon: Implementation (4 hours)
**Create: `src/utils/security.py`**

```python
import hashlib
from pyspark.sql import functions as F

class PIIMasking:
    @staticmethod
    def hash_identifier(col: str) -> Column:
        """SHA-256 hash for irreversible anonymization"""
        return F.sha2(F.col(col), 256)
    
    @staticmethod
    def mask_partial(col: str, visible_chars: int = 3) -> Column:
        """Show first N chars, mask rest"""
        return F.concat(
            F.substring(F.col(col), 1, visible_chars),
            F.lit("***")
        )
    
    @staticmethod
    def age_bucket(col: str) -> Column:
        """Generalize age to 5-year buckets"""
        return F.floor(F.col(col) / 5) * 5

# Apply in Gold layer
gold_features_anonymized = (
    silver_subjects
    .withColumn("subject_id_hash", PIIMasking.hash_identifier("subject_id"))
    .withColumn("age_bucket", PIIMasking.age_bucket("age"))
    .drop("subject_id", "age", "name", "birthdate")
)
```

**Create: `docs/study-notes/day19-security-compliance.md`**
- Audit logging queries
- Secrets management (Azure Key Vault)
- Compliance frameworks (GDPR, HIPAA for EEG data)

---

### Day 18: Advanced Spark & Cost Optimization (🟡 Priority 4)
**Goal:** Fill Domain 1 & 6 gaps (35% combined)

#### Morning: Broadcast Joins & Skew (3 hours)
**Create: `notebooks/day15_advanced_spark_sql.py`**

```python
# Broadcast join for small dimension table
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

subject_metadata = spark.table("silver.subjects")  # Small table
epoch_facts = spark.table("silver.epochs")  # Large table

# Force broadcast
result = epoch_facts.join(
    broadcast(subject_metadata),
    "subject_id"
)

# Skew handling with salting
skewed_df = epoch_facts.withColumn(
    "salted_key",
    F.concat(F.col("subject_id"), F.lit("_"), (F.rand() * 10).cast("int"))
)

# Replicate small table
replicated_metadata = subject_metadata.withColumn(
    "replica_id",
    F.explode(F.array(*[F.lit(i) for i in range(10)]))
).withColumn(
    "salted_key",
    F.concat(F.col("subject_id"), F.lit("_"), F.col("replica_id"))
)

skew_optimized = skewed_df.join(replicated_metadata, "salted_key")
```

**Add:**
- Complex CTEs with window functions
- Pivot/unpivot for EEG channel analysis
- SCD Type 2 implementation
- Higher-order functions (transform, filter, aggregate)

#### Afternoon: Cost & Performance (4 hours)
**Expand: `docs/study-notes/day12-performance-tuning.md`**

Add:
1. **Photon Engine:**
   ```sql
   ALTER TABLE eeg_catalog.silver.epochs
   SET TBLPROPERTIES ('delta.enablePhoton' = 'true');
   ```
   - Run benchmark with/without Photon
   - Document 2-3x speedup for aggregations

2. **Serverless vs Classic Compute:**
   - Cost comparison table
   - Use cases for each
   - Startup time vs sustained performance

3. **Cost Monitoring:**
   ```sql
   -- Query system tables for DBU usage
   SELECT 
     usage_date,
     sku_name,
     usage_quantity,
     usage_quantity * list_price AS cost_usd
   FROM system.billing.usage
   WHERE usage_date >= current_date() - 30
   GROUP BY usage_date, sku_name
   ORDER BY cost_usd DESC;
   ```

**Create: `docs/exam/cost-optimization-checklist.md`**
- OPTIMIZE ZORDER best practices
- Autoscaling configuration
- Spot instance strategies
- File size optimization (128 MB target)

---

### Day 19: Databricks CLI & REST API (🟡 Priority 5)
**Goal:** Debugging & deployment tools (10% of exam)

**Create: `docs/study-notes/day20-databricks-cli-api.md`**

#### CLI Essentials
```bash
# Authentication
databricks configure --token

# Workspace operations
databricks workspace ls /Repos/production
databricks workspace export /notebooks/pipeline.py pipeline_backup.py

# Job management
databricks jobs list
databricks jobs run-now --job-id 123
databricks jobs get-run --run-id 456

# DBFS
databricks fs ls dbfs:/mnt/raw/eeg/
databricks fs cp local_file.csv dbfs:/tmp/

# Secrets
databricks secrets create-scope --scope prod_keys
databricks secrets put --scope prod_keys --key db_password
```

#### REST API Examples
```python
import requests

BASE_URL = "https://adb-xxxx.azuredatabricks.net"
TOKEN = dbutils.secrets.get("prod_keys", "api_token")

# Jobs API
response = requests.post(
    f"{BASE_URL}/api/2.1/jobs/run-now",
    headers={"Authorization": f"Bearer {TOKEN}"},
    json={"job_id": 123, "notebook_params": {"date": "2026-06-15"}}
)

# Workspace API
response = requests.get(
    f"{BASE_URL}/api/2.0/workspace/list",
    headers={"Authorization": f"Bearer {TOKEN}"},
    params={"path": "/Repos/production"}
)
```

**Create: `docs/exam/cli-api-cheatsheet.md`**
- Top 20 CLI commands
- REST API endpoint reference
- Error handling patterns

---

### Day 20: Data Quality & Deduplication (🟢 Priority 6)
**Goal:** Domain 3 enhancements

**Create: `src/silver/data_quality.py`**

```python
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

class DataQualityManager:
    def __init__(self, quarantine_table: str):
        self.quarantine_table = quarantine_table
    
    def deduplicate_with_window(self, df: DataFrame, keys: list, order_col: str) -> DataFrame:
        """Advanced deduplication using window functions"""
        window = Window.partitionBy(*keys).orderBy(F.desc(order_col))
        return (
            df.withColumn("row_num", F.row_number().over(window))
            .filter("row_num = 1")
            .drop("row_num")
        )
    
    def quarantine_violations(self, df: DataFrame, rules: dict) -> tuple:
        """Separate valid and invalid records"""
        condition = " AND ".join([f"({rule})" for rule in rules.values()])
        
        valid_df = df.filter(condition)
        quarantine_df = df.filter(f"NOT ({condition})")
        
        # Log to quarantine table
        quarantine_df.write.mode("append").saveAsTable(self.quarantine_table)
        
        return valid_df, quarantine_df

# Usage
quality_mgr = DataQualityManager("eeg_catalog.quarantine.failed_epochs")

rules = {
    "amplitude_range": "max_amplitude_uv BETWEEN -500 AND 500",
    "duration": "duration_s BETWEEN 25 AND 35",
    "sample_rate": "sample_rate_hz = 256"
}

valid_epochs, quarantined = quality_mgr.quarantine_violations(bronze_epochs, rules)
```

---

### Day 21: Data Modeling & SCD Type 2 (🟢 Priority 7)
**Goal:** Domain 10 enhancements

**Create: `src/gold/dimensional_model.py`**

```python
# SCD Type 2 for subject metadata
from delta.tables import DeltaTable

def upsert_scd_type2(target_table: str, updates: DataFrame, keys: list):
    """Implement SCD Type 2 with effective dates"""
    target = DeltaTable.forName(spark, target_table)
    
    # Add metadata columns
    updates_with_meta = (
        updates
        .withColumn("effective_date", F.current_date())
        .withColumn("end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )
    
    # Merge logic
    target.alias("target").merge(
        updates_with_meta.alias("updates"),
        " AND ".join([f"target.{k} = updates.{k}" for k in keys])
        + " AND target.is_current = true"
    ).whenMatchedUpdate(
        condition="target.diagnosis != updates.diagnosis",  # Attribute changed
        set={
            "end_date": "current_date()",
            "is_current": "false"
        }
    ).whenNotMatchedInsertAll().execute()
    
    # Insert new versions for changed records
    changed_records = updates_with_meta  # Logic to identify changes
    changed_records.write.mode("append").saveAsTable(target_table)

# Star schema example
subject_dim = gold_subjects.select(
    "subject_id",  # Surrogate key
    "age", "gender", "diagnosis",
    "effective_date", "end_date", "is_current"
)

epoch_fact = gold_epochs.select(
    "epoch_id",  # Fact key
    "subject_id",  # Foreign key to dim
    "sleep_stage", "duration_s", "quality_score"
)
```

**Create: `docs/study-notes/day21-data-modeling.md`**
- SCD Type 1 vs Type 2 comparison
- Star schema vs snowflake schema
- Partitioning strategy for fact tables

---

## 📋 Summary: Files to Create

### Study Notes (7 new files)
1. `docs/study-notes/day15-data-source-connectors.md`
2. `docs/study-notes/day16-delta-sharing.md`
3. `docs/study-notes/day17-monitoring-observability.md`
4. `docs/study-notes/day19-security-compliance.md`
5. `docs/study-notes/day20-databricks-cli-api.md`
6. `docs/study-notes/day21-data-modeling.md`

### Notebooks (5 new)
1. `notebooks/day15_advanced_spark_sql.py`
2. `notebooks/day16_delta_sharing_federation.py`
3. `notebooks/day17_spark_ui_analysis.py`
4. `notebooks/day18_cost_performance.py`

### Source Code (4 new)
1. `src/utils/spark_optimization.py`
2. `src/utils/monitoring.py`
3. `src/utils/security.py`
4. `src/silver/data_quality.py`
5. `src/gold/dimensional_model.py`

### Exam Reference (3 new)
1. `docs/exam/delta-sharing-cheatsheet.md`
2. `docs/exam/cost-optimization-checklist.md`
3. `docs/exam/cli-api-cheatsheet.md`

### Updates (3 files)
1. **Expand:** `docs/study-notes/day12-performance-tuning.md` (Photon, serverless, cost)
2. **Expand:** `docs/exam/uc-governance.md` (row filters, column masks)
3. **Expand:** `src/bronze/ingest_eeg_files.py` (schema evolution, rescue columns)

---

## 🎯 Expected Outcome

| Domain | Before | After | Improvement |
|--------|--------|-------|-------------|
| 1. Code Development (22%) | 🟡 60% | 🟢 90% | +30% |
| 2. Ingestion (7%) | 🟢 80% | 🟢 95% | +15% |
| 3. Transformation (10%) | 🟡 70% | 🟢 90% | +20% |
| 4. Data Sharing (5%) | 🔴 0% | 🟢 90% | +90% |
| 5. Monitoring (10%) | 🟠 30% | 🟢 85% | +55% |
| 6. Performance (13%) | 🟡 70% | 🟢 90% | +20% |
| 7. Security (10%) | 🟠 40% | 🟢 85% | +45% |
| 8. Governance (7%) | 🟡 70% | 🟢 90% | +20% |
| 9. Debugging (10%) | 🟢 75% | 🟢 95% | +20% |
| 10. Modeling (6%) | 🟢 80% | 🟢 95% | +15% |

**Overall:** 60% → 90% (+30%)

---

## ✅ Daily Checklist

### Day 15:
- [ ] Study Delta Sharing docs (3h)
- [ ] Study Lakehouse Federation docs (2h)
- [ ] Create day16 study note
- [ ] Create day16 notebook
- [ ] Create delta-sharing-cheatsheet.md
- [ ] Run 5 practice scenarios

### Day 16:
- [ ] Analyze Spark UI for EEG job (2h)
- [ ] Create day17 study note
- [ ] Create day17 notebook with DLT event log queries
- [ ] Implement CDF on gold tables
- [ ] Create monitoring.py module
- [ ] Test alerting functionality

### Day 17:
- [ ] Study row-level security (2h)
- [ ] Study column-level security (2h)
- [ ] Expand uc-governance.md
- [ ] Create security.py module
- [ ] Create day19 study note
- [ ] Test PII masking functions

### Day 18:
- [ ] Create day15 notebook (broadcast, skew, CTEs)
- [ ] Add Photon benchmarks to day12 note
- [ ] Create cost-optimization-checklist.md
- [ ] Run serverless vs classic comparison
- [ ] Add cost monitoring queries

### Day 19:
- [ ] Practice 20 CLI commands
- [ ] Test REST API workflows
- [ ] Create day20 study note
- [ ] Create cli-api-cheatsheet.md

### Day 20:
- [ ] Create data_quality.py module
- [ ] Test quarantine pattern
- [ ] Test advanced deduplication

### Day 21:
- [ ] Create dimensional_model.py
- [ ] Implement SCD Type 2
- [ ] Create star schema example
- [ ] Create day21 study note

---

## 📖 Study Tips

1. **Focus on Gaps First:** Days 15-17 target the 3 critical missing domains
2. **Hands-On Practice:** Every concept should have runnable code
3. **Exam Questions:** After each day, do 10 practice questions on that domain
4. **Flashcards:** Create Anki cards for SQL syntax (GRANT, CREATE SHARE, etc.)
5. **Mock Exams:** Take 2 full mock exams after Day 21

---

## 🔗 Official Resources

- [Data Engineer Professional Exam Guide](https://www.databricks.com/learn/certification/data-engineer-professional)
- [Delta Sharing](https://docs.databricks.com/en/delta-sharing/index.html)
- [Lakehouse Federation](https://docs.databricks.com/en/query-federation/index.html)
- [Unity Catalog Security](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html)
- [DLT Event Logs](https://docs.databricks.com/en/delta-live-tables/observability.html)
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html)
