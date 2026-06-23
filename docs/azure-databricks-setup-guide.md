# Azure Databricks Setup Guide

## Overview

This guide provides complete, step-by-step instructions for provisioning and configuring an Azure Databricks workspace for the **EEG Lakehouse Lab** project. The pipeline ingests raw EEG data from PhysioNet/WFDB sources into a Medallion Architecture (Bronze → Silver → Gold) on Azure Data Lake Storage Gen2, governed by Unity Catalog.

> **2026 Update Notes:**
> - `dbutils.fs.mount()` is **deprecated** — this guide uses the modern **ABFS direct access** pattern with Unity Catalog external locations
> - Azure Active Directory (AAD) is now **Microsoft Entra ID**
> - Databricks Runtime **16.x LTS** is the current long-term support release
> - Databricks CLI **v0.2x** replaces the legacy v0.1x Python CLI
> - **Serverless compute** is now generally available and recommended for interactive notebooks
> - Unity Catalog is enabled **by default** on all new Premium workspaces

---

## Prerequisites

### Required Azure Resources
- Active Azure subscription (Pay-as-you-go or EA)
- **Contributor** or **Owner** role on the target resource group, plus **User Access Administrator** for role assignments
- Microsoft Entra ID tenant with permission to register applications

### Required Tools (install before starting)

| Tool | Version | Install Command |
|------|---------|----------------|
| Azure CLI | ≥ 2.60 | `winget install Microsoft.AzureCLI` / `brew install azure-cli` |
| Databricks CLI | ≥ 0.230 | `pip install databricks-cli` or `brew tap databricks/tap && brew install databricks` |
| Python | ≥ 3.11 | [python.org](https://python.org) |

```bash
# Verify versions
az --version
databricks --version
python --version
```

---

## Architecture Overview

```
PhysioNet / WFDB API
        │
        ▼
  [Azure Data Lake Storage Gen2]
  ├── bronze/   ← Raw EEG (.edf, .hea, .dat) + metadata JSON
  ├── silver/   ← Validated, segmented, normalized Delta tables
  ├── gold/     ← Feature-aggregated Delta tables for ML/analysis
  └── checkpoints/  ← DLT pipeline state & streaming checkpoints

        │
        ▼
  [Azure Databricks Workspace]  (Premium, DBR 16.x LTS)
  ├── Unity Catalog (Metastore → eeg_lakehouse catalog)
  ├── Delta Live Tables pipelines
  ├── Serverless SQL Warehouse
  └── Notebooks (Python + SQL)
```

---

## Step 1: Create Resource Group

### 1.1 Via Azure Portal
1. Open [Azure Portal](https://portal.azure.com) → sign in
2. Search **"Resource groups"** → click **"+ Create"**
3. Fill in:
   - **Subscription**: select your subscription
   - **Resource group name**: `rg-eeg-lakehouse-prod`
   - **Region**: `West Europe` (or your preferred region — keep all resources in the same region)
4. Click **"Review + create"** → **"Create"**

### 1.2 Via Azure CLI (faster)
```bash
az login
az group create \
  --name rg-eeg-lakehouse-prod \
  --location westeurope
```

**Expected result**: `"provisioningState": "Succeeded"`

---

## Step 2: Create Azure Data Lake Storage Gen2

### 2.1 Create Storage Account

```bash
az storage account create \
  --name steeglakehouse \
  --resource-group rg-eeg-lakehouse-prod \
  --location westeurope \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false
```

> **Critical**: `--enable-hierarchical-namespace true` enables ADLS Gen2 semantics (required for Delta Lake and Unity Catalog).

### 2.2 Create Medallion Containers

```bash
STORAGE_ACCOUNT="steeglakehouse"
RESOURCE_GROUP="rg-eeg-lakehouse-prod"

for container in bronze silver gold checkpoints unity-catalog; do
  az storage fs create \
    --name $container \
    --account-name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --auth-mode login
done
```

| Container | Purpose |
|-----------|---------|
| `bronze` | Raw EEG files (.edf, .hea, .dat) and metadata JSON |
| `silver` | Cleaned, validated, segmented Delta tables |
| `gold` | Feature-aggregated Delta tables for ML and reporting |
| `checkpoints` | DLT pipeline checkpoints and streaming state |
| `unity-catalog` | Unity Catalog metastore root storage |

**Expected result**: 5 containers created and visible under **"Storage browser"** in the portal.

---

## Step 3: Create Managed Identity & Assign Permissions

In 2026, the **recommended authentication method** for Databricks→Storage access is via **Azure Databricks Access Connector** (a managed identity), replacing service principal OAuth flows for most scenarios.

### 3.1 Create Databricks Access Connector

```bash
az databricks access-connector create \
  --name ac-eeg-lakehouse \
  --resource-group rg-eeg-lakehouse-prod \
  --location westeurope \
  --identity-type SystemAssigned
```

Get the principal ID:
```bash
CONNECTOR_PRINCIPAL=$(az databricks access-connector show \
  --name ac-eeg-lakehouse \
  --resource-group rg-eeg-lakehouse-prod \
  --query identity.principalId -o tsv)

echo "Access Connector Principal ID: $CONNECTOR_PRINCIPAL"
```

### 3.2 Assign Storage Role to Access Connector

```bash
STORAGE_ID=$(az storage account show \
  --name steeglakehouse \
  --resource-group rg-eeg-lakehouse-prod \
  --query id -o tsv)

az role assignment create \
  --assignee $CONNECTOR_PRINCIPAL \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID
```

> The `Storage Blob Data Contributor` role gives the Databricks workspace managed identity full read/write access to all containers. For production, scope it per-container.

### 3.3 (Optional) Create Service Principal for CI/CD

Only required for GitHub Actions / Azure DevOps deployments — skip for interactive setup.

```bash
# Create service principal
az ad sp create-for-rbac \
  --name sp-databricks-eeg-cicd \
  --role Contributor \
  --scopes /subscriptions/<SUBSCRIPTION_ID>/resourceGroups/rg-eeg-lakehouse-prod \
  --json-auth > sp-credentials.json

# ⚠️ Never commit sp-credentials.json to git!
echo "sp-credentials.json" >> .gitignore
```

---

## Step 4: Create Azure Databricks Workspace

### 4.1 Create Workspace

```bash
az databricks workspace create \
  --name dbw-eeg-lakehouse-prod \
  --resource-group rg-eeg-lakehouse-prod \
  --location westeurope \
  --sku premium \
  --enable-no-public-ip true
```

> **Premium tier is required** for Unity Catalog, Delta Live Tables, and serverless compute.  
> `--enable-no-public-ip true` is a security best practice (Secure Cluster Connectivity / No Public IP).

**Deployment time**: 5–10 minutes.

### 4.2 Retrieve Workspace URL

```bash
WORKSPACE_URL=$(az databricks workspace show \
  --name dbw-eeg-lakehouse-prod \
  --resource-group rg-eeg-lakehouse-prod \
  --query workspaceUrl -o tsv)

echo "Workspace URL: https://$WORKSPACE_URL"
```

---

## Step 5: Configure Azure Key Vault & Secrets

### 5.1 Create Key Vault

```bash
az keyvault create \
  --name kv-eeg-lakehouse \
  --resource-group rg-eeg-lakehouse-prod \
  --location westeurope \
  --enable-rbac-authorization true
```

### 5.2 Grant Your User Access to Key Vault

```bash
MY_OBJECT_ID=$(az ad signed-in-user show --query id -o tsv)

az role assignment create \
  --assignee $MY_OBJECT_ID \
  --role "Key Vault Secrets Officer" \
  --scope $(az keyvault show --name kv-eeg-lakehouse --query id -o tsv)
```

### 5.3 Add Secrets

```bash
# Storage account key (fallback — prefer managed identity)
STORAGE_KEY=$(az storage account keys list \
  --account-name steeglakehouse \
  --resource-group rg-eeg-lakehouse-prod \
  --query "[0].value" -o tsv)

az keyvault secret set \
  --vault-name kv-eeg-lakehouse \
  --name storage-account-key \
  --value "$STORAGE_KEY"

# Access connector resource ID (for Unity Catalog)
CONNECTOR_ID=$(az databricks access-connector show \
  --name ac-eeg-lakehouse \
  --resource-group rg-eeg-lakehouse-prod \
  --query id -o tsv)

az keyvault secret set \
  --vault-name kv-eeg-lakehouse \
  --name access-connector-id \
  --value "$CONNECTOR_ID"
```

### 5.4 Link Key Vault to Databricks Secret Scope

1. Navigate to: `https://<WORKSPACE_URL>#secrets/createScope`
2. Fill in:
   - **Scope Name**: `eeg-lakehouse-scope`
   - **Manage Principal**: `All Users`
   - **DNS Name**: `https://kv-eeg-lakehouse.vault.azure.net/`
   - **Resource ID**: output of `az keyvault show --name kv-eeg-lakehouse --query id -o tsv`
3. Click **"Create"**

**Alternative — Databricks CLI:**
```bash
databricks secrets create-scope \
  --scope eeg-lakehouse-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id $(az keyvault show --name kv-eeg-lakehouse --query id -o tsv) \
  --dns-name "https://kv-eeg-lakehouse.vault.azure.net/"
```

---

## Step 6: Set Up Unity Catalog

Unity Catalog is **enabled by default** on new Premium workspaces in 2026. This section covers creating the metastore, external location, and catalog structure.

### 6.1 Access the Databricks Account Console

Navigate to [https://accounts.azuredatabricks.net](https://accounts.azuredatabricks.net) → sign in as account admin.

### 6.2 Create Metastore (One-Time, Per Region)

> If a metastore already exists for your region, skip to 6.4 (assign workspace).

1. In Account Console → **"Data"** → **"Unity Catalog"** → **"Create metastore"**
2. Fill in:
   - **Metastore name**: `eeg-lakehouse-metastore-westeurope`
   - **Region**: `westeurope`
   - **ADLS Gen2 path**: `abfss://unity-catalog@steeglakehouse.dfs.core.windows.net/`
   - **Access connector ID**: paste the Resource ID of `ac-eeg-lakehouse`
3. Click **"Create"**

### 6.3 Assign Metastore to Workspace

1. Select the metastore → **"Workspaces"** tab
2. Click **"Assign to workspace"**
3. Select `dbw-eeg-lakehouse-prod` → **"Assign"**

### 6.4 Create External Location for ADLS Gen2

In the Databricks workspace (not account console), open a SQL notebook:

```sql
-- Create storage credential backed by the Access Connector
CREATE STORAGE CREDENTIAL `eeg-lakehouse-credential`
WITH AZURE_MANAGED_IDENTITY (
  CONNECTOR = '/subscriptions/<SUBSCRIPTION_ID>/resourceGroups/rg-eeg-lakehouse-prod/providers/Microsoft.Databricks/accessConnectors/ac-eeg-lakehouse'
);

-- Create external locations for each layer
CREATE EXTERNAL LOCATION `bronze-location`
URL 'abfss://bronze@steeglakehouse.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `eeg-lakehouse-credential`);

CREATE EXTERNAL LOCATION `silver-location`
URL 'abfss://silver@steeglakehouse.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `eeg-lakehouse-credential`);

CREATE EXTERNAL LOCATION `gold-location`
URL 'abfss://gold@steeglakehouse.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `eeg-lakehouse-credential`);

CREATE EXTERNAL LOCATION `checkpoints-location`
URL 'abfss://checkpoints@steeglakehouse.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `eeg-lakehouse-credential`);

-- Validate
SHOW EXTERNAL LOCATIONS;
```

### 6.5 Create Catalog and Schemas

```sql
-- Create project catalog
CREATE CATALOG IF NOT EXISTS eeg_lakehouse
COMMENT 'EEG Lakehouse Lab — Medallion Architecture for EEG signal processing';

USE CATALOG eeg_lakehouse;

-- Create Medallion schemas backed by external locations
CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION 'abfss://bronze@steeglakehouse.dfs.core.windows.net/managed'
COMMENT 'Raw EEG data layer: .edf files, WFDB records, metadata JSON';

CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION 'abfss://silver@steeglakehouse.dfs.core.windows.net/managed'
COMMENT 'Cleaned and validated layer: segmented EEG epochs, quality-filtered signals';

CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION 'abfss://gold@steeglakehouse.dfs.core.windows.net/managed'
COMMENT 'Feature-aggregated layer: band power, connectivity metrics, ML-ready features';

-- Verify
SHOW CATALOGS;
SHOW SCHEMAS IN eeg_lakehouse;
```

**Expected result**: Catalog `eeg_lakehouse` with three schemas visible in the Databricks catalog explorer.

---

## Step 7: Configure Databricks CLI (v0.2x)

### 7.1 Authenticate with OAuth

```bash
# Configure profile for this workspace
databricks configure --host https://<WORKSPACE_URL> --profile eeg-lakehouse

# Authenticate (opens browser for OAuth token)
databricks auth login --host https://<WORKSPACE_URL> --profile eeg-lakehouse

# Verify
databricks clusters list --profile eeg-lakehouse
```

### 7.2 Verify Workspace Access

```bash
# List catalogs
databricks unity-catalog catalogs list --profile eeg-lakehouse

# List secrets
databricks secrets list-scopes --profile eeg-lakehouse
```

---

## Step 8: Create Compute

### 8.1 Serverless Compute (Recommended for Interactive Notebooks)

Serverless compute is **automatically available** on Premium workspaces — no cluster creation required. When creating a notebook, select **"Serverless"** from the compute dropdown.

Benefits over classic clusters:
- Starts in ~5 seconds (no cluster provisioning)
- Auto-scales transparently
- Billed per-second, no idle cost
- Always on the latest Databricks Runtime

### 8.2 Classic Cluster (Required for Delta Live Tables and Long-Running Jobs)

Create via UI:
1. Sidebar → **"Compute"** → **"+ Create compute"**
2. Configure:

| Setting | Value |
|---------|-------|
| Cluster name | `eeg-processing-cluster` |
| Policy | `Unrestricted` (or custom) |
| Cluster mode | `Single node` (dev) / `Standard` (multi-worker) |
| Databricks Runtime | **`16.4 LTS`** (Spark 3.5, Scala 2.12) |
| Worker type | `Standard_DS3_v2` (4 cores, 14 GB) |
| Min workers | 1 |
| Max workers | 4 |
| Auto-scaling | ✅ Enabled |
| Auto-termination | 30 minutes |

**Spark configuration** (paste into Advanced → Spark config):
```
spark.databricks.delta.preview.enabled true
spark.sql.catalog.spark_catalog com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog
spark.databricks.delta.schema.autoMerge.enabled true
spark.databricks.repl.allowedLanguages python,sql
```

3. Click **"Create compute"** → wait 3–5 minutes.

### 8.3 Create via Databricks CLI

```bash
databricks clusters create --profile eeg-lakehouse --json '{
  "cluster_name": "eeg-processing-cluster",
  "spark_version": "16.4.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "autoscale": {"min_workers": 1, "max_workers": 4},
  "autotermination_minutes": 30,
  "data_security_mode": "SINGLE_USER",
  "runtime_engine": "PHOTON"
}'
```

> **`data_security_mode: SINGLE_USER`** is required for Unity Catalog access from classic clusters. **`PHOTON`** engine gives 2–4× faster Delta operations at no extra cost on DS3_v2.

---

## Step 9: Storage Access — Modern ABFS Pattern

> ⚠️ **`dbutils.fs.mount()` is deprecated as of DBR 15.0.** Do not use mount points in new code. Use **ABFS direct paths** or **Unity Catalog volume paths** instead.

### 9.1 Direct ABFS Path Access (Notebooks)

```python
# Modern pattern — no mount required
storage_account = "steeglakehouse"

# Read raw EEG metadata from bronze
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/"
gold_path   = f"abfss://gold@{storage_account}.dfs.core.windows.net/"

# List files
display(dbutils.fs.ls(bronze_path))

# Read Delta table
df = spark.read.format("delta").load(f"{silver_path}eeg_epochs/")
display(df.limit(10))
```

Authentication is handled automatically via the Access Connector managed identity — **no keys or secrets needed in the notebook code**.

### 9.2 Unity Catalog Volume Paths (Preferred for Managed Tables)

```python
# After Unity Catalog schemas are created (Step 6.5):
# Tables are accessed via three-part name: catalog.schema.table

# Read a managed table
df = spark.read.table("eeg_lakehouse.silver.eeg_epochs")

# Write a managed table
df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("eeg_lakehouse.gold.band_power_features")
```

### 9.3 Verify Access

```python
# Test storage connectivity
containers = ["bronze", "silver", "gold", "checkpoints"]
for c in containers:
    path = f"abfss://{c}@steeglakehouse.dfs.core.windows.net/"
    try:
        files = dbutils.fs.ls(path)
        print(f"✅ {c}: accessible ({len(files)} items)")
    except Exception as e:
        print(f"❌ {c}: FAILED — {e}")

# Test Unity Catalog
spark.sql("SHOW SCHEMAS IN eeg_lakehouse").show()
```

---

## Step 10: Install Required Libraries

### 10.1 Cluster Libraries (for Classic Clusters)

Navigate to cluster → **"Libraries"** tab → **"Install new"**

**PyPI packages:**
```
mne==1.8.0
wfdb==4.1.2
pyEDFlib==0.1.37
scipy==1.14.0
numpy==2.0.1
matplotlib==3.9.2
pandas==2.2.3
```

**Install via init script** (recommended for reproducibility):

```bash
# File: /Workspace/Repos/wang-yuhao/databricks-eeg-lakehouse-lab/scripts/install-libs.sh
#!/bin/bash
/databricks/python/bin/pip install \
  mne==1.8.0 \
  wfdb==4.1.2 \
  pyEDFlib==0.1.37 \
  scipy==1.14.0 \
  numpy==2.0.1 \
  matplotlib==3.9.2 \
  pandas==2.2.3
```

Set this as an **init script** in the cluster's Advanced → Init Scripts section.

### 10.2 For Serverless Notebooks

Serverless environments require `%pip` magic in each notebook session:

```python
%pip install mne==1.8.0 wfdb==4.1.2 pyEDFlib==0.1.37
dbutils.library.restartPython()
```

### 10.3 Verify Installation

```python
import mne
import wfdb
import pyspark
import delta

print(f"MNE:     {mne.__version__}")        # expected: 1.8.0
print(f"WFDB:    {wfdb.__version__}")       # expected: 4.1.2
print(f"PySpark: {pyspark.__version__}")    # expected: 3.5.x
print(f"DBR:     {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion')}")
```

---

## Step 11: Connect GitHub Repository (Databricks Git Folders)

### 11.1 Generate GitHub PAT

1. GitHub → **Settings** → **Developer settings** → **Personal access tokens** → **Fine-grained tokens**
2. Permissions needed: `Contents: Read and write`, `Metadata: Read`
3. Generate and copy the token

### 11.2 Add Token to Databricks

1. Databricks workspace → **Settings** (gear icon) → **Linked accounts**
2. Click **"Add a Git credential"**
3. **Git provider**: GitHub
4. **Git username**: your GitHub username
5. **Token**: paste PAT
6. Click **"Save"**

### 11.3 Clone Repository

1. Sidebar → **"Workspace"** → **"+ New"** → **"Git folder"**
2. **Git repository URL**: `https://github.com/wang-yuhao/databricks-eeg-lakehouse-lab`
3. **Git provider**: GitHub
4. **Folder name**: `databricks-eeg-lakehouse-lab`
5. Click **"Create Git folder"**

### 11.4 Verify

```bash
# From Databricks CLI
databricks repos list --profile eeg-lakehouse
```

---

## Step 12: Deploy Delta Live Tables Pipeline

### 12.1 Create DLT Pipeline via UI

1. Sidebar → **"Delta Live Tables"** → **"+ Create pipeline"**
2. Configure:

| Setting | Value |
|---------|-------|
| Pipeline name | `eeg-medallion-pipeline` |
| Product edition | **Advanced** (for expectations & enhanced monitoring) |
| Pipeline mode | **Triggered** (batch) or **Continuous** (streaming) |
| Source code | `/Repos/wang-yuhao/databricks-eeg-lakehouse-lab/notebooks/` |
| Target catalog | `eeg_lakehouse` |
| Target schema | `silver` |
| Storage location | `abfss://checkpoints@steeglakehouse.dfs.core.windows.net/dlt/` |
| Cluster policy | `Delta Live Tables` |

3. Click **"Create"**

### 12.2 Create Pipeline via databricks.yml (Already in Repo)

The project `databricks.yml` at the repo root defines the pipeline configuration. Deploy with:

```bash
databricks bundle deploy --target dev --profile eeg-lakehouse
databricks bundle run eeg_medallion_pipeline --profile eeg-lakehouse
```

---

## Step 13: Verification Checklist

### Infrastructure
- [ ] Resource group `rg-eeg-lakehouse-prod` created in `westeurope`
- [ ] Storage account `steeglakehouse` with hierarchical namespace enabled
- [ ] Five containers: `bronze`, `silver`, `gold`, `checkpoints`, `unity-catalog`
- [ ] Databricks Access Connector `ac-eeg-lakehouse` with `Storage Blob Data Contributor` role
- [ ] Databricks workspace `dbw-eeg-lakehouse-prod` (Premium tier)
- [ ] Key Vault `kv-eeg-lakehouse` with required secrets

### Unity Catalog
- [ ] Metastore created and assigned to workspace
- [ ] Storage credential backed by Access Connector
- [ ] External locations for all four data containers
- [ ] Catalog `eeg_lakehouse` with schemas `bronze`, `silver`, `gold`

### Compute & Access
- [ ] Serverless compute available (no action needed)
- [ ] Classic cluster `eeg-processing-cluster` (DBR 16.4 LTS, Photon enabled)
- [ ] Libraries installed and verified
- [ ] GitHub repo cloned via Git Folders
- [ ] ABFS direct paths working (all containers accessible)
- [ ] Unity Catalog table read/write verified

### Pipeline
- [ ] DLT pipeline `eeg-medallion-pipeline` created
- [ ] `databricks bundle deploy` succeeds from CLI

---

## Troubleshooting

### ❌ ABFS Access Denied
```
Operation failed: "This request is not authorized to perform this operation using this permission."
```
**Cause**: Access Connector missing `Storage Blob Data Contributor` role, or role assignment propagation delay (up to 5 minutes).  
**Fix**:
```bash
az role assignment list \
  --assignee $CONNECTOR_PRINCIPAL \
  --scope $STORAGE_ID \
  --output table
```
If missing, re-run the role assignment command from Step 3.2.

### ❌ Unity Catalog: "User does not have CREATE privilege"
**Cause**: Your user lacks `CREATE CATALOG` or `CREATE SCHEMA` privileges.  
**Fix**: In the Account Console → Unity Catalog → Permissions, grant your user `Account Admin` or `Metastore Admin`, then re-run the SQL.

### ❌ Cluster Fails to Start: `QUOTA_EXCEEDED`
**Cause**: Azure subscription vCPU quota insufficient for the region.  
**Fix**: Go to Portal → Subscriptions → Usage + Quotas → request increase for `Standard DSv2 Family vCPUs`. Alternatively, switch to `Standard_D4s_v5` (same price, often more available).

### ❌ `databricks bundle deploy` Authentication Error
**Cause**: CLI profile not set up correctly.  
**Fix**:
```bash
databricks auth login --host https://<WORKSPACE_URL> --profile eeg-lakehouse
databricks bundle validate --profile eeg-lakehouse
```

### ❌ MNE/WFDB Import Error on Serverless
**Cause**: Serverless sessions reset on detach.  
**Fix**: Always place `%pip install` at the top of notebooks that use these libraries, followed by `dbutils.library.restartPython()`.

### ❌ DLT Pipeline: Storage Location Not Found
**Cause**: External location not created or credentials missing.  
**Fix**: Run `SHOW EXTERNAL LOCATIONS;` in a SQL notebook and verify `checkpoints-location` is listed.

---

## Next Steps

1. **Run the ingestion notebook**: `notebooks/01_bronze_ingestion.py` — downloads EEG data from PhysioNet to `bronze`
2. **Execute DLT pipeline**: `notebooks/02_silver_dlt_pipeline.py` — applies quality filters and creates Silver Delta tables
3. **Generate Gold features**: `notebooks/03_gold_features.py` — extracts band power and connectivity metrics
4. **Explore with SQL Warehouse**: Create a Serverless SQL Warehouse and query `eeg_lakehouse.gold.*` from the SQL editor
5. **Set up CI/CD**: See `.github/workflows/` for the pre-configured GitHub Actions pipeline
6. **Monitor with Databricks Observability**: Enable cluster logs → Azure Log Analytics for production monitoring

---

## Additional Resources

- [Azure Databricks Documentation (2026)](https://learn.microsoft.com/en-us/azure/databricks/)
- [Unity Catalog Setup Guide](https://docs.databricks.com/en/data-governance/unity-catalog/get-started.html)
- [Databricks Asset Bundles (DAB)](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Delta Live Tables Documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
- [ABFS Direct Access Pattern](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver)
- [Databricks CLI v0.2x Reference](https://docs.databricks.com/en/dev-tools/cli/index.html)
- [DBR 16.x Release Notes](https://docs.databricks.com/en/release-notes/runtime/index.html)
- [PhysioNet / WFDB Python Docs](https://wfdb.readthedocs.io/)
- [MNE-Python Documentation](https://mne.tools/stable/)

---

**Document Version**: 2.0  
**Last Updated**: June 2026  
**Databricks Runtime**: 16.4 LTS (Spark 3.5)  
**Maintainer**: EEG Lakehouse Project Team
