# Azure Databricks Setup Guide

## Overview
This guide provides step-by-step instructions for creating an Azure Databricks workspace and connecting it to essential Azure resources for the EEG Lakehouse project.

---

## Prerequisites

### Required Azure Resources
- Active Azure subscription
- Appropriate permissions (Contributor or Owner role on resource group)
- Azure CLI installed (optional, for command-line setup)

### Required Tools
- Web browser (Chrome, Edge, or Firefox recommended)
- Azure account credentials

---

## Step 1: Create Resource Group

### 1.1 Navigate to Azure Portal
1. Open [Azure Portal](https://portal.azure.com)
2. Sign in with your Azure credentials
3. Click on **"Resource groups"** in the left menu
4. Click **"+ Create"**

### 1.2 Configure Resource Group
1. **Subscription**: Select your subscription
2. **Resource group name**: `rg-eeg-lakehouse-prod`
3. **Region**: Select region (e.g., `West Europe` or `East US`)
4. Click **"Review + create"**
5. Click **"Create"**

**Expected Result**: Resource group created successfully

---

## Step 2: Create Azure Data Lake Storage Gen2

### 2.1 Create Storage Account
1. In Azure Portal, click **"+ Create a resource"**
2. Search for **"Storage account"**
3. Click **"Create"**

### 2.2 Configure Basics
1. **Subscription**: Select your subscription
2. **Resource group**: `rg-eeg-lakehouse-prod`
3. **Storage account name**: `steeglakehouse` (must be globally unique)
4. **Region**: Same as resource group
5. **Performance**: Standard
6. **Redundancy**: LRS (Locally-redundant storage) for dev, ZRS/GRS for production

### 2.3 Configure Advanced Settings
1. **Enable hierarchical namespace**: ✅ **YES** (Critical for Data Lake Gen2)
2. **Enable blob public access**: ❌ No
3. **Minimum TLS version**: Version 1.2
4. Click **"Review + create"**
5. Click **"Create"**

### 2.4 Create Containers
1. Navigate to the storage account
2. Click **"Containers"** under Data storage
3. Create the following containers:
   - `bronze` - Raw EEG data
   - `silver` - Cleaned and validated data
   - `gold` - Business-level aggregated data
   - `checkpoints` - Delta Live Tables checkpoints

**Expected Result**: Storage account with hierarchical namespace enabled and 4 containers created

---

## Step 3: Create Azure Databricks Workspace

### 3.1 Create Databricks Resource
1. In Azure Portal, click **"+ Create a resource"**
2. Search for **"Azure Databricks"**
3. Click **"Create"**

### 3.2 Configure Workspace Basics
1. **Subscription**: Select your subscription
2. **Resource group**: `rg-eeg-lakehouse-prod`
3. **Workspace name**: `dbw-eeg-lakehouse-prod`
4. **Region**: Same as storage account
5. **Pricing tier**: 
   - **Standard** - Basic features, lower cost
   - **Premium** - Unity Catalog, RBAC, audit logs (recommended)
   - **Trial (Premium)** - 14-day premium trial

### 3.3 Configure Networking (Optional)
For basic setup, use default settings:
- **Deploy in your Virtual Network (VNet)**: No

For production:
- Enable VNet injection for network isolation
- Configure private endpoints

### 3.4 Configure Security
1. **Managed identity**: System-assigned (enable for easier authentication)
2. Click **"Review + create"**
3. Click **"Create"**

**Deployment Time**: 5-10 minutes

**Expected Result**: Databricks workspace deployed successfully

---

## Step 4: Configure Storage Access

### 4.1 Retrieve Storage Account Key
1. Navigate to your storage account (`steeglakehouse`)
2. Click **"Access keys"** under Security + networking
3. Copy **"Key1"** value

### 4.2 Set Up Service Principal (Recommended for Production)

#### Create Service Principal
1. Navigate to **"Azure Active Directory"** → **"App registrations"**
2. Click **"+ New registration"**
3. **Name**: `sp-databricks-eeg-lakehouse`
4. Click **"Register"**
5. Copy **"Application (client) ID"** and **"Directory (tenant) ID"**

#### Create Client Secret
1. In the app registration, click **"Certificates & secrets"**
2. Click **"+ New client secret"**
3. **Description**: `databricks-access-secret`
4. **Expires**: 12 months (or custom)
5. Click **"Add"**
6. **Copy the secret value immediately** (you can't view it again)

#### Assign Storage Permissions
1. Navigate to storage account → **"Access Control (IAM)"**
2. Click **"+ Add"** → **"Add role assignment"**
3. **Role**: `Storage Blob Data Contributor`
4. **Assign access to**: User, group, or service principal
5. **Select**: `sp-databricks-eeg-lakehouse`
6. Click **"Save"**

**Expected Result**: Service principal with access to storage account

---

## Step 5: Configure Databricks Workspace

### 5.1 Launch Workspace
1. Navigate to your Databricks workspace in Azure Portal
2. Click **"Launch Workspace"**
3. Sign in with your Azure credentials

### 5.2 Create Secret Scope

#### Using Azure Key Vault (Recommended)
1. Create Azure Key Vault:
   - Portal → Create resource → Key Vault
   - Name: `kv-eeg-lakehouse`
   - Same resource group and region

2. Add secrets to Key Vault:
   - `storage-account-key`
   - `sp-client-id`
   - `sp-client-secret`
   - `sp-tenant-id`

3. Link to Databricks:
   - In Databricks workspace URL, navigate to: `https://<workspace-url>#secrets/createScope`
   - **Scope Name**: `eeg-lakehouse-scope`
   - **Manage Principal**: All Users (or restrict)
   - **DNS Name**: Key Vault DNS (from KV properties)
   - **Resource ID**: Key Vault Resource ID (from KV properties)
   - Click **"Create"**

#### Using Databricks-Managed Scope (Quick Setup)
```python
# Create secret scope using Databricks CLI
databricks secrets create-scope --scope eeg-lakehouse-scope

# Add storage account key
databricks secrets put --scope eeg-lakehouse-scope --key storage-account-key
```

### 5.3 Mount Storage Account

Create a notebook and run:

```python
# Configuration
storage_account_name = "steeglakehouse"
container_name = "bronze"
mount_point = "/mnt/bronze"

# Method 1: Using Account Key
storage_account_key = dbutils.secrets.get(scope="eeg-lakehouse-scope", key="storage-account-key")

dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = mount_point,
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
)

# Repeat for silver, gold, and checkpoints containers
for container in ["silver", "gold", "checkpoints"]:
    dbutils.fs.mount(
        source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net",
        mount_point = f"/mnt/{container}",
        extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
    )
```

**Alternative: Using Service Principal**

```python
# Configuration
application_id = dbutils.secrets.get(scope="eeg-lakehouse-scope", key="sp-client-id")
tenant_id = dbutils.secrets.get(scope="eeg-lakehouse-scope", key="sp-tenant-id")
secret = dbutils.secrets.get(scope="eeg-lakehouse-scope", key="sp-client-secret")

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = mount_point,
    extra_configs = configs
)
```

### 5.4 Verify Mount

```python
# List mounted locations
display(dbutils.fs.mounts())

# Test read/write access
dbutils.fs.ls("/mnt/bronze")

# Create test file
dbutils.fs.put("/mnt/bronze/test.txt", "Test connection successful")

# Read test file
print(dbutils.fs.head("/mnt/bronze/test.txt"))

# Clean up
dbutils.fs.rm("/mnt/bronze/test.txt")
```

**Expected Result**: All storage containers mounted and accessible

---

## Step 6: Create Compute Cluster

### 6.1 Navigate to Compute
1. In Databricks workspace, click **"Compute"** in left sidebar
2. Click **"+ Create compute"**

### 6.2 Configure Cluster

#### Basic Configuration
- **Cluster name**: `eeg-processing-cluster`
- **Cluster mode**: 
  - **Standard** - For general workloads
  - **High Concurrency** - For multiple users (Premium only)
  - **Single Node** - For development/testing

#### Databricks Runtime
- **Runtime version**: `14.3 LTS` or latest LTS
- **ML Runtime**: Not required unless using ML features

#### Worker Configuration
- **Worker type**: `Standard_DS3_v2` (4 cores, 14 GB) for development
- **Min workers**: 1
- **Max workers**: 3
- **Enable autoscaling**: ✅ Yes

#### Driver Configuration
- **Driver type**: Same as worker

#### Advanced Options
```python
# Spark Config (for optimizations)
spark.databricks.delta.preview.enabled true
spark.databricks.delta.retentionDurationCheck.enabled false
```

#### Auto Termination
- **Terminate after**: 30 minutes of inactivity

### 6.3 Create Cluster
1. Click **"Create compute"**
2. Wait for cluster to start (3-5 minutes)

**Expected Result**: Cluster running and ready for notebooks

---

## Step 7: Set Up Unity Catalog (Premium Only)

### 7.1 Enable Unity Catalog
1. Navigate to account console: `https://accounts.azuredatabricks.net`
2. Click **"Data"** → **"Unity Catalog"**
3. Click **"Create metastore"**

### 7.2 Configure Metastore
1. **Metastore name**: `eeg-lakehouse-metastore`
2. **Region**: Same as workspace
3. **ADLS Gen2 path**: `abfss://unity-catalog@steeglakehouse.dfs.core.windows.net/`
4. **Access connector**: Create new managed identity
5. Click **"Create"**

### 7.3 Assign to Workspace
1. Select the metastore
2. Click **"Assign to workspace"**
3. Select `dbw-eeg-lakehouse-prod`
4. Click **"Assign"**

### 7.4 Create Catalog and Schema

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS eeg_lakehouse;

-- Use catalog
USE CATALOG eeg_lakehouse;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw EEG data layer';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleaned and validated data layer';
CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Business-level aggregated data layer';

-- Verify
SHOW CATALOGS;
SHOW SCHEMAS IN eeg_lakehouse;
```

**Expected Result**: Unity Catalog enabled with three-level namespace

---

## Step 8: Install Required Libraries

### 8.1 Cluster Libraries
1. Navigate to cluster → **"Libraries"** tab
2. Click **"Install new"**
3. Install the following:

#### PyPI Packages
```
mne
wfdb
pandas
numpy
scipy
matplotlib
```

#### Maven Packages (if needed)
```
io.delta:delta-core_2.12:2.4.0
```

### 8.2 Verify Installation

```python
import mne
import wfdb
import delta
import pyspark

print(f"MNE version: {mne.__version__}")
print(f"WFDB version: {wfdb.__version__}")
print(f"PySpark version: {pyspark.__version__}")
```

**Expected Result**: All libraries imported successfully

---

## Step 9: Verification Checklist

### 9.1 Resource Verification
- [ ] Resource group created
- [ ] Storage account with hierarchical namespace enabled
- [ ] Four containers created (bronze, silver, gold, checkpoints)
- [ ] Databricks workspace deployed
- [ ] Service principal configured with storage access
- [ ] Secret scope created and secrets stored

### 9.2 Connectivity Verification
- [ ] All storage containers mounted in Databricks
- [ ] Read/write access verified
- [ ] Compute cluster running
- [ ] Required libraries installed

### 9.3 Unity Catalog (Premium)
- [ ] Metastore created and assigned
- [ ] Catalog and schemas created
- [ ] Permissions configured

---

## Troubleshooting

### Issue: Mount Fails with Authentication Error
**Solution**: 
- Verify storage account key or service principal credentials
- Check secret scope name and key names
- Ensure service principal has Storage Blob Data Contributor role

### Issue: Cluster Won't Start
**Solution**:
- Check subscription quota for VM cores
- Verify region has capacity for selected VM type
- Review cluster event logs for specific error

### Issue: Can't Access Unity Catalog
**Solution**:
- Verify Premium tier workspace
- Check metastore assignment to workspace
- Ensure workspace identity has access to metastore storage

### Issue: Libraries Won't Install
**Solution**:
- Check cluster has internet access
- Verify PyPI package names and versions
- Review driver logs for installation errors

---

## Next Steps

1. **Configure Git integration**: Connect GitHub repository to Databricks Repos
2. **Set up CI/CD**: Configure Azure DevOps or GitHub Actions for deployment
3. **Implement monitoring**: Enable diagnostic logs and Azure Monitor integration
4. **Configure backup**: Set up automated backups for critical data
5. **Review security**: Implement network security groups and private endpoints

---

## Additional Resources

- [Azure Databricks Documentation](https://docs.microsoft.com/azure/databricks/)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Azure Storage Documentation](https://docs.microsoft.com/azure/storage/)

---

**Document Version**: 1.0  
**Last Updated**: 2025  
**Maintainer**: EEG Lakehouse Project Team
