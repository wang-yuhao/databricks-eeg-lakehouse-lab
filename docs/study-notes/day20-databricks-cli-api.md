# Day 20: Databricks CLI and API

## Overview
Study notes on Databricks CLI, REST APIs, and automation for Professional certification.

## Key Concepts

### 1. Databricks CLI
- Command-line interface for Databricks operations
- Built on top of Databricks REST API
- Authentication via personal access tokens
- Configuration profiles for multiple workspaces

### 2. REST API
- RESTful HTTP API for programmatic access
- JSON request and response format
- Authentication: Bearer token
- Rate limiting considerations

### 3. SDK Support
- Python SDK (databricks-sdk)
- Java SDK
- Go SDK
- Third-party libraries

## CLI Installation and Setup

### Installation
```bash
# Using pip
pip install databricks-cli

# Verify installation
databricks --version
```

### Configuration
```bash
# Configure authentication
databricks configure --token

# Configure with profile
databricks configure --token --profile production

# Configuration file location: ~/.databrickscfg
```

### Configuration File Format
```ini
[DEFAULT]
host = https://adb-123456789.azuredatabricks.net
token = dapi1234567890abcdef

[production]
host = https://prod.cloud.databricks.com
token = dapi9876543210fedcba
```

## Common CLI Commands

### Workspace Operations
```bash
# List workspace items
databricks workspace ls /Users/user@company.com

# Export notebook
databricks workspace export /path/to/notebook notebook.py

# Import notebook
databricks workspace import notebook.py /path/to/notebook

# Delete workspace item
databricks workspace rm /path/to/item
```

### Cluster Operations
```bash
# List clusters
databricks clusters list

# Get cluster info
databricks clusters get --cluster-id 1234-567890-abcd123

# Create cluster
databricks clusters create --json-file cluster-config.json

# Start cluster
databricks clusters start --cluster-id 1234-567890-abcd123

# Terminate cluster
databricks clusters delete --cluster-id 1234-567890-abcd123
```

### Jobs Operations
```bash
# List jobs
databricks jobs list

# Create job
databricks jobs create --json-file job-config.json

# Run job now
databricks jobs run-now --job-id 123

# Get run output
databricks runs get-output --run-id 456
```

### DBFS Operations
```bash
# List DBFS files
databricks fs ls dbfs:/path/to/directory

# Upload file to DBFS
databricks fs cp local-file.csv dbfs:/data/file.csv

# Download file from DBFS
databricks fs cp dbfs:/data/file.csv local-file.csv

# Remove file from DBFS
databricks fs rm dbfs:/data/file.csv
```

### Secrets Management
```bash
# Create secret scope
databricks secrets create-scope --scope my-scope

# Put secret
databricks secrets put --scope my-scope --key my-key

# List secrets in scope
databricks secrets list --scope my-scope

# Delete secret
databricks secrets delete --scope my-scope --key my-key
```

## REST API Endpoints

### Clusters API
```python
import requests

headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# List clusters
response = requests.get(
    f'{workspace_url}/api/2.0/clusters/list',
    headers=headers
)

# Create cluster
cluster_config = {
    'cluster_name': 'my-cluster',
    'spark_version': '11.3.x-scala2.12',
    'node_type_id': 'Standard_DS3_v2',
    'num_workers': 2
}

response = requests.post(
    f'{workspace_url}/api/2.0/clusters/create',
    headers=headers,
    json=cluster_config
)
```

### Jobs API
```python
# Create job
job_config = {
    'name': 'my-job',
    'tasks': [{
        'task_key': 'main_task',
        'notebook_task': {
            'notebook_path': '/Users/user/notebook',
            'source': 'WORKSPACE'
        },
        'new_cluster': {
            'spark_version': '11.3.x-scala2.12',
            'node_type_id': 'Standard_DS3_v2',
            'num_workers': 2
        }
    }]
}

response = requests.post(
    f'{workspace_url}/api/2.1/jobs/create',
    headers=headers,
    json=job_config
)
```

### DBFS API
```python
import base64

# Upload file to DBFS
with open('local_file.txt', 'rb') as f:
    content = base64.b64encode(f.read()).decode('utf-8')

data = {
    'path': '/tmp/remote_file.txt',
    'contents': content,
    'overwrite': True
}

response = requests.post(
    f'{workspace_url}/api/2.0/dbfs/put',
    headers=headers,
    json=data
)
```

## Python SDK

### Installation and Setup
```python
pip install databricks-sdk
```

### Using the SDK
```python
from databricks.sdk import WorkspaceClient

# Initialize client
w = WorkspaceClient(
    host='https://workspace.cloud.databricks.com',
    token='dapi123...'
)

# List clusters
for cluster in w.clusters.list():
    print(f'{cluster.cluster_name}: {cluster.state}')

# Create job
job = w.jobs.create(
    name='my-job',
    tasks=[...]
)

# Run job
run = w.jobs.run_now(job_id=job.job_id)
```

## Automation Best Practices

### CI/CD Integration
- Store credentials in secure vaults
- Use service principals for automation
- Implement error handling and retries
- Log API calls for auditing
- Version control infrastructure code

### Error Handling
```python
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure retries
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount('https://', adapter)
```

### Rate Limiting
- Respect API rate limits
- Implement exponential backoff
- Use batch operations when available
- Monitor API usage patterns

## Common Use Cases

### 1. Automated Deployments
- Deploy notebooks from version control
- Update job configurations
- Manage cluster policies
- Sync workspace resources

### 2. Monitoring and Alerting
- Query job run status
- Monitor cluster utilization
- Track failed runs
- Generate usage reports

### 3. Data Pipeline Orchestration
- Trigger jobs programmatically
- Chain job executions
- Handle dependencies
- Implement retry logic

### 4. Workspace Management
- User and group provisioning
- Permissions management
- Resource cleanup
- Audit logging

## Practice Questions
1. How do you configure multiple workspace profiles in Databricks CLI?
2. What authentication methods are supported by the REST API?
3. How do you implement retry logic for API calls?
4. What are the differences between CLI and Python SDK?
5. How do you securely manage API tokens in CI/CD pipelines?
6. What API version should be used for Jobs operations?

## References
- Databricks CLI Documentation
- REST API Reference
- Python SDK Documentation
- Authentication Guide
