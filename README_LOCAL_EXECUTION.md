# Running Fabric Scanner API Locally

This script can now run outside of Microsoft Fabric notebooks, on your local machine.

## Setup

### 1. Python Version

Requires **Python 3.8 or higher**.

Verify your Python version:
```bash
python --version
```

Recommended versions: Python 3.8, 3.9, 3.10, 3.11, or 3.12.

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Authentication

Create a `.env` file (copy from `.env.template`):

```bash
FABRIC_SP_TENANT_ID=your-tenant-id
FABRIC_SP_CLIENT_ID=your-app-client-id
FABRIC_SP_CLIENT_SECRET=your-app-secret
```

Or set environment variables directly:

```powershell
# PowerShell
$env:FABRIC_SP_TENANT_ID = "your-tenant-id"
$env:FABRIC_SP_CLIENT_ID = "your-client-id"
$env:FABRIC_SP_CLIENT_SECRET = "your-secret"
```

### 3. Service Principal Setup

Your service principal (App Registration) needs:

- **API Permissions**: Power BI Service
  - `Tenant.Read.All` (for Scanner API)
  - `Workspace.Read.All`

- **Power BI Admin**: Must be added to a security group that has:
  - Fabric Admin Portal → Tenant Settings → Developer Settings → "Allow service principals to use Fabric APIs" (Enabled)
  - Service principal added to the security group

## Usage

### Run Incremental Scan (Default)

```bash
python fabric_scanner_cloud_connections.py
```

This will:
- Scan workspaces modified in last 24 hours
- Save results to `./scanner_output/curated/tenant_cloud_connections.parquet`
- Also save as CSV: `./scanner_output/curated/tenant_cloud_connections.csv`

### Run Full Tenant Scan

Edit the `if __name__ == "__main__":` section at the bottom of the script:

```python
if __name__ == "__main__":
    run_cloud_connection_scan(
        enable_full_scan=True,
        enable_incremental_scan=False
    )
```

### Custom Time Window

```python
run_cloud_connection_scan(
    enable_incremental_scan=True,
    incremental_hours_back=6  # Last 6 hours
)
```

## Output

When running locally, data is saved to:

```
scanner_output/
├── raw/               # Raw JSON responses from Scanner API
│   ├── full/          # Full scan results
│   └── incremental/   # Incremental scan results
└── curated/           # Processed data
    ├── tenant_cloud_connections.parquet
    └── tenant_cloud_connections.csv
```

## Differences from Fabric Execution

| Feature | Fabric Notebook | Local Execution |
|---------|----------------|-----------------|
| Data Storage | Lakehouse Tables (Spark SQL) | Parquet + CSV files |
| DataFrame Engine | PySpark | pandas |
| Authentication | Delegated or SPN | Service Principal only |
| Raw Data Storage | Lakehouse Files/ | ./scanner_output/raw/ |

## Environment Detection

The script automatically detects whether it's running in Fabric or locally:

- **In Fabric**: Uses Spark, mssparkutils, Lakehouse paths
- **Locally**: Uses pandas, local filesystem, creates ./scanner_output/

## Troubleshooting

### Import Errors (PySpark/notebookutils)

These are expected when running locally. The script handles them automatically.

### Authentication Errors

- Verify your service principal has correct API permissions
- Check that the security group is enabled in Power BI Admin Portal
- Ensure environment variables are set correctly

### Rate Limiting (429 errors)

For large tenants (10K+ workspaces), use chunked scanning:

```python
run_cloud_connection_scan(
    enable_full_scan_chunked=True,
    max_batches_per_hour=450  # Stays under 500/hour limit
)
```

## Viewing Results

### Using pandas (Python)

```python
import pandas as pd

df = pd.read_parquet("scanner_output/curated/tenant_cloud_connections.parquet")
print(df.head())
print(df.info())

# Filter cloud connections only
cloud_only = df[df['cloud'] == True]
print(cloud_only.groupby('connector')['workspace_name'].count())
```

### Using Excel/Power BI

Open the CSV file directly:
```
scanner_output/curated/tenant_cloud_connections.csv
```

## Advanced: Using with .env files

Install python-dotenv:

```bash
pip install python-dotenv
```

Add to top of script:

```python
from dotenv import load_dotenv
load_dotenv()  # Loads .env file automatically
```

## Next Steps

- Schedule with Windows Task Scheduler or cron
- Integrate with Power BI for visualization
- Export to Azure SQL Database for centralized reporting
- Set up alerting for new cloud connections
