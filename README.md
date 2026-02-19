# Fabric Scanner API — Cloud Connections Inventory

[![CI Tests](https://github.com/jpmicrosoft/fabric_scanner_cloud_connections/actions/workflows/ci-tests.yml/badge.svg)](https://github.com/jpmicrosoft/fabric_scanner_cloud_connections/actions/workflows/ci-tests.yml)

This package contains a Python script that can run **both in Microsoft Fabric notebooks and locally on your machine**:

- Performs a **full tenant scan** (including **Personal workspaces**) using the **Scanner Admin REST APIs**
- Supports **incremental scans** (workspaces modified since a timestamp) with **flexible time windows** (hours or days)
- **Retrieves scan results by scan ID** for previously completed scans
- **Scans all JSON files** in a lakehouse directory to identify cloud connections
- **Single file mode** for debugging and testing individual JSON files
- Allows you to **enable/disable any combination** of scanning features
- Flattens results into a unified cloud‑connections schema
- **In Fabric**: Persists results to **Parquet** in your Lakehouse and exposes a **SQL table** `tenant_cloud_connections`
- **Locally**: Saves results to **Parquet** and **CSV** files in `./scanner_output/` (optionally uploads to lakehouse)

## Files
- `fabric_scanner_cloud_connections.py` — the Python script (works in Fabric or locally)
- `requirements.txt` — Python dependencies for local execution
- `.env.template` — Template for environment variables (local execution)
- `scanner_config.yaml.example` — Example YAML configuration file
- `scanner_config.json.example` — Example JSON configuration file
- `.gitignore` — Pre-configured to protect credentials and secrets
- `README.md` — this guide (Fabric notebook usage)
- `README_LOCAL_EXECUTION.md` — guide for running locally outside Fabric

**✅ Security:** The included [.gitignore](.gitignore) is pre-configured to prevent committing:
- Credential files (`*credential*`, `*credentials*`)
- Secret files (`*secret*`, `*secrets*`)
- Config files with real IDs (`scanner_config.yaml`, `scanner_config.json`)
- Environment files (`.env.*`) except `.env.template`

## Execution Modes

### 1. Fabric Notebook (Default)
- Uses **PySpark** for data processing
- Saves to **Lakehouse Tables** (SQL accessible)
- Supports **delegated** or **service principal** authentication
- Raw data stored in Lakehouse `Files/`

### 2. Local Execution (NEW)
- Uses **pandas** for data processing
- Saves to **local files** in `./scanner_output/` (ALWAYS)
- **Optionally** uploads to Fabric lakehouse (requires lakehouse configuration)
- Requires **service principal** authentication
- See [README_LOCAL_EXECUTION.md](README_LOCAL_EXECUTION.md) for setup

The script automatically detects its environment and adapts accordingly.

## Features

### 1. Full Tenant Scan
Scans all workspaces in your Fabric tenant using the Scanner API to create a baseline inventory.

### 2. Automatic Token Refresh (NEW)
**For long-running scans**: Automatically manages authentication tokens to prevent failures during extended scans.
- **Token caching**: Caches tokens with 5-minute expiry buffer
- **Auto-refresh**: Proactively refreshes tokens before expiration
- **401 error handling**: Automatically recovers from expired tokens
- **Multi-hour support**: Enables scans lasting 165+ hours (full tenant with MAX_PARALLEL_SCANS=1)
- **Zero downtime**: Seamless token rotation without interrupting scans
- **All auth modes**: Works with Service Principal, interactive, and delegated authentication

**Benefits:**
- Run unattended overnight/weekend scans without manual intervention
- Supports very large tenants (247K+ workspaces = ~7 days continuous scanning)
- No authentication failures during long-running incremental scans

### 3. Full Tenant Scan (Large Shared Tenants Mode)
**For large shared tenants (10K+ workspaces)**: Automatically manages rate limits by processing workspaces in hourly chunks.
- **Rate limit safe**: Respects 500 API calls/hour limit
- **Automatic pausing**: Waits between chunks to avoid 429 errors
- **Progress tracking**: Shows completion status and estimated time
- **Incremental saving**: Saves results after each chunk (no data loss if interrupted)
- **Configurable speed**: Adjust `max_batches_per_hour` to balance speed vs. other users
- **Shared tenant friendly**: Leaves room for other Scanner API users in your organization

**CLI Usage:**
```powershell
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants
```

### 4. Parallel Capacity Scanning (Phase 3 - NEW)
**Speed up full tenant scans** by scanning multiple capacities concurrently with thread-safe rate limiting:
- **2-3x faster**: Reduce scan time from hours to minutes depending on capacity distribution
- **Thread-safe**: Distributed quota management ensures no API limit violations
- **Conservative options**: Start with sequential mode (safest) or conservative parallel settings
- **Flexible filtering**: Include/exclude specific capacities or prioritize critical ones
- **Smart quota distribution**: API calls distributed evenly across parallel workers
- **Backward compatible**: Defaults to sequential mode (Phase 2 behavior)

**Performance Examples:**
- **Sequential (most conservative)**: 1 capacity at a time = baseline speed (safest for heavily shared tenants)
- **Conservative parallel**: 2 capacities with reduced quota (300 calls/hour) = ~1.5x faster
- **Balanced parallel**: 2 capacities with standard quota (450 calls/hour) = ~2x faster
- **Faster parallel**: 3 capacities with full quota (450 calls/hour) = ~2.8x faster

**CLI Usage Examples:**

```powershell
# Very Conservative - Sequential (safest for heavily shared tenants)
python fabric_scanner_cloud_connections.py --full-scan --parallel-capacities 1

# Conservative Parallel - 2 workers with reduced quota
python fabric_scanner_cloud_connections.py --full-scan \
  --parallel-capacities 2 \
  --max-calls-per-hour 300

# Balanced Parallel - 2 workers with standard quota
python fabric_scanner_cloud_connections.py --full-scan \
  --parallel-capacities 2 \
  --max-calls-per-hour 450

# Faster Parallel - 3 workers with full quota
python fabric_scanner_cloud_connections.py --full-scan \
  --parallel-capacities 3 \
  --max-calls-per-hour 450

# With Capacity Filtering - Only scan production capacities
python fabric_scanner_cloud_connections.py --full-scan \
  --parallel-capacities 2 \
  --max-calls-per-hour 450 \
  --capacity-filter "prod-capacity-1,prod-capacity-2"

# With Capacity Exclusion - Skip test/dev environments
python fabric_scanner_cloud_connections.py --full-scan \
  --parallel-capacities 2 \
  --max-calls-per-hour 450 \
  --exclude-capacities "test-capacity,dev-capacity"

# With Priority - Scan critical capacities first
python fabric_scanner_cloud_connections.py --full-scan \
  --parallel-capacities 3 \
  --max-calls-per-hour 450 \
  --capacity-priority "production,critical"
```

**When to Use:**
- ✅ **Use Sequential** (`--parallel-capacities 1`): Heavily shared tenants, many concurrent Scanner API users
- ✅ **Use Conservative Parallel** (`--parallel-capacities 2 --max-calls-per-hour 300`): Shared tenants with moderate API usage
- ✅ **Use Balanced Parallel** (`--parallel-capacities 2 --max-calls-per-hour 450`): Most tenants, good speed/safety balance
- ✅ **Use Faster Parallel** (`--parallel-capacities 3 --max-calls-per-hour 450`): Dedicated tenants or off-hours scanning

### 5. Incremental Scan
Scans only workspaces modified since a specific timestamp for efficient updates.
- **Flexible time windows**: Specify lookback period in hours or days
- **Sub-hour precision**: Support for fractional hours (e.g., 0.5 = 30 minutes)

### 6. Scan ID Retrieval
Retrieves results from a previous scan using the WorkspaceInfo GetScanResult API.
- **Use scan IDs** from previous scans without re-scanning
- **24-hour window**: Works with scans completed within the last 24 hours
- **Includes personal workspaces**: Gets all workspaces from the original scan

### 7. Workspace Table Source (NEW)
**Optimize performance and reduce API calls** by reading the workspace list from a pre-existing lakehouse table instead of calling the Scanner API.

**Use Cases:**
- **Pre-filtered scans**: Only scan production workspaces by maintaining a curated workspace list
- **Governance integration**: Use workspace inventories from external governance or CMDB systems
- **Scheduled scans**: Ensure consistent workspace scope across scheduled scan jobs
- **API quota management**: Save API calls for actual scanning (workspace discovery can be 1 API call)
- **Performance**: Faster scan startup with no workspace discovery delay

**Expected Table Schema:**
- `workspace_id` (required): Workspace GUID - **This column is mandatory**
- `workspace_name` (optional): Workspace display name
- `workspace_type` (optional): Workspace type (e.g., "PersonalGroup", "Workspace")
- `capacity_id` (optional): Capacity ID for capacity-based filtering

**How It Works:**
1. Scanner reads workspace list from your table/file instead of calling `GetModifiedWorkspaces` API
2. Table name is validated (alphanumeric, underscores, dots only) to prevent SQL injection
3. If table read fails or returns no workspaces, automatically falls back to API call
4. Applies all normal filters (`--no-personal`, `--capacity-filter`, etc.) to the workspace list
5. Proceeds with normal scanning workflow

**Security Note:**
Table names are validated to allow only alphanumeric characters, underscores, and dots. Invalid table names will trigger an error and fallback to API discovery.

**CLI Usage:**
```powershell
# Use workspace list from lakehouse table
python fabric_scanner_cloud_connections.py --full-scan --workspace-table-source workspace_inventory

# Combine with no-personal filter (filters after reading from table)
python fabric_scanner_cloud_connections.py --full-scan --workspace-table-source workspace_inventory --no-personal

# Works with chunked mode for large tenants
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --workspace-table-source my_workspaces

# Works with capacity grouping
python fabric_scanner_cloud_connections.py --full-scan --group-by-capacity --workspace-table-source workspace_catalog

# Works with capacity filtering
python fabric_scanner_cloud_connections.py --full-scan --workspace-table-source workspace_inventory --capacity-filter cap-prod-1
```

**Fabric Notebook Example:**
```python
# Option 1: Create workspace inventory table from previous scan results
spark.sql("""
CREATE TABLE workspace_inventory AS
SELECT DISTINCT
    workspace_id,
    workspace_name,
    workspace_kind as workspace_type,
    -- Extract capacity_id if available from your data
    NULL as capacity_id
FROM tenant_cloud_connections
WHERE workspace_kind != 'PersonalGroup'  -- Pre-filter out personal workspaces
""")

# Option 2: Create from Power BI Admin API workspace list
# (You would run a separate script to populate this table)
spark.sql("""
CREATE TABLE workspace_inventory (
    workspace_id STRING,
    workspace_name STRING,
    workspace_type STRING,
    capacity_id STRING
)
""")

# Use the table for subsequent scans
%run Files/scripts/fabric_scanner_cloud_connections
full_tenant_scan(workspace_table_source="workspace_inventory")
```

**Local Execution Example:**
```powershell
# Create parquet file from your workspace data source
# (Example: export from Power BI Admin API, governance tool, etc.)

# Then use it for scanning
python fabric_scanner_cloud_connections.py --full-scan --workspace-table-source ./my_workspaces.parquet

# Or place in curated directory
python fabric_scanner_cloud_connections.py --full-scan --workspace-table-source my_workspaces
# This will look for: scanner_output/curated/my_workspaces.parquet
```

**Creating Sample Parquet File (Python):**
```python
import pandas as pd

# Your workspace data from any source
workspaces_df = pd.DataFrame({
    "workspace_id": ["guid1", "guid2", "guid3"],
    "workspace_name": ["Sales", "Marketing", "Finance"],
    "workspace_type": ["Workspace", "Workspace", "Workspace"],
    "capacity_id": ["cap-prod-1", "cap-prod-1", "cap-prod-2"]
})

workspaces_df.to_parquet("my_workspaces.parquet")
```

**Benefits:**
- **Faster scans**: Eliminates workspace discovery API call (saves 1+ minutes on large tenants)
- **Pre-filtered lists**: Only scan workspaces you care about (e.g., exclude test/dev)
- **External integration**: Use workspace lists from Governance tools, CMDB, or custom catalogs
- **Consistent scope**: Ensure all scans use the same workspace set across runs
- **API quota preservation**: Save API calls for actual scanning operations
- **Automatic fallback**: If table read fails, automatically uses API discovery (no manual intervention needed)

### 8. JSON Directory Scan
Scans all JSON files in a lakehouse directory (e.g., previously saved scanner API responses) and extracts cloud connection information.
- **Single file mode**: Process one specific JSON file for debugging/testing
- **Batch mode**: Process all JSON files in a directory
- Useful for analyzing archived scan results and historical data

## Configuration

### General Configuration

```python
# Authentication mode
USE_DELEGATED = True  # True -> Delegated (Fabric Admin); False -> Service Principal

# Debug logging
DEBUG_MODE = False  # Set to True for detailed logging (JSON structure, workspace table reads, API calls)

# JSON single file mode (for testing/debugging)
JSON_SINGLE_FILE_MODE = False  # Set to True to process only one specific JSON file
JSON_TARGET_FILE = "Files/scanner/raw/scan_result_20241208.json"  # Target file path

# Local execution: Upload to Lakehouse (OPTIONAL - only needed if you want to upload to Fabric)
# When running locally, results ALWAYS save to ./scanner_output/ regardless of these settings
UPLOAD_TO_LAKEHOUSE = False  # Set to True to ALSO upload results to Fabric Lakehouse
LAKEHOUSE_WORKSPACE_ID = ""  # Required only if UPLOAD_TO_LAKEHOUSE = True
LAKEHOUSE_ID = ""  # Required only if UPLOAD_TO_LAKEHOUSE = True
LAKEHOUSE_UPLOAD_PATH = "Files/scanner"  # Path within lakehouse (optional, defaults to Files/scanner)
```
## Prerequisites

### For Fabric Notebook Execution
1. In the Fabric **Admin Portal** enable Admin API settings for metadata scanning (and optionally DAX/Mashup) so the Scanner API returns rich datasource details.
2. Choose authentication:
   - **Service Principal (SPN)** (recommended for automation): set `AUTH_MODE = "spn"` (default) and provide `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`.
   - **Delegated Fabric Admin**: set `AUTH_MODE = "delegated"`. Run inside Fabric notebooks.
   - **Interactive User**: set `AUTH_MODE = "interactive"` for ad-hoc testing with personal credentials.
3. Ensure you have **Fabric Administrator** or **Power BI Administrator** role for Scanner API access.

### For Local Execution
1. **Python version:** Requires **Python 3.8 or higher** (tested with Python 3.8, 3.9, 3.10, 3.11, 3.12)

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Service Principal:**
   - Create App Registration in Azure AD
   - Add API permissions: Power BI Service → `Tenant.Read.All`, `Workspace.Read.All`
   - Enable in Power BI Admin Portal → Developer Settings → "Allow service principals to use Fabric APIs"
   - Set environment variables:
     ```powershell
     $env:FABRIC_SP_TENANT_ID = "your-tenant-id"
     $env:FABRIC_SP_CLIENT_ID = "your-client-id"
     $env:FABRIC_SP_CLIENT_SECRET = "your-secret"
     ```

4. **Run the script:**
   ```bash
   python fabric_scanner_cloud_connections_notebook.py
   ```

See [`README_LOCAL_EXECUTION.md`](README_LOCAL_EXECUTION.md) for detailed local execution instructions.

## How to Use

### Quick Start - Command Line Interface (CLI)

The easiest way to use the script is via the **command-line interface**:

```powershell
# Full scan (baseline - all workspaces)
python fabric_scanner_cloud_connections.py --full-scan

# Full scan with rate limiting (safe for large shared tenants)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants

# Full scan using workspace list from table (reduces API calls)
python fabric_scanner_cloud_connections.py --full-scan --workspace-table-source workspace_inventory

# Incremental scan (last 24 hours - default, with hash optimization)
python fabric_scanner_cloud_connections.py --incremental

# Incremental scan (last 7 days)
python fabric_scanner_cloud_connections.py --incremental --days 7

# Incremental scan (last 6 hours)
python fabric_scanner_cloud_connections.py --incremental --hours 6

# Incremental without hash optimization
python fabric_scanner_cloud_connections.py --incremental --no-hash-optimization

# Health check before scanning
python fabric_scanner_cloud_connections.py --health-check

# Get results from a previous scan
python fabric_scanner_cloud_connections.py --scan-id e7d03602-4873-4760-b37e-1563ef5358e3

# Analyze connection directionality
python fabric_scanner_cloud_connections.py --analyze-direction --with-activity --activity-days 30

# Process JSON directory
python fabric_scanner_cloud_connections.py --json-dir Files/scanner/raw/full

# Exclude personal workspaces
python fabric_scanner_cloud_connections.py --full-scan --no-personal

# Upload to lakehouse when running locally (OPTIONAL - local files always saved to ./scanner_output/)
python fabric_scanner_cloud_connections.py --full-scan \
  --upload-to-lakehouse \
  --lakehouse-workspace-id "abc-def-ghi" \
  --lakehouse-id "123-456-789" \
  --lakehouse-upload-path "Files/scanner"

# Use configuration file for all settings
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --config scanner_config.yaml

# Enable debug output to see detailed processing information
python fabric_scanner_cloud_connections.py --incremental --debug

# Get help and see all options
python fabric_scanner_cloud_connections.py --help
```

**CLI Options:**

**Scan Modes** (mutually exclusive):
- `--full-scan` - Run full tenant scan (all workspaces)
- `--incremental` - Run incremental scan (modified workspaces only)
- `--scan-id SCAN_ID` - Retrieve results from a specific scan ID (UUID)
- `--health-check` - Check Scanner API health and quota availability
- `--analyze-direction` - Analyze connection directionality (inbound vs outbound)
- `--json-dir PATH` - Process JSON files from directory

**Full Scan Options:**
- `--large-shared-tenants` - Use rate-limited chunked mode for large shared tenants (processes in hourly chunks)
- `--max-batches-per-hour N` - Max API calls per hour in chunked mode (default: 450)
- `--group-by-capacity` - Group workspaces by capacity for organized scanning (Phase 2)

**Parallel Capacity Scanning Options (Phase 3):**
- `--parallel-capacities N` - Number of capacities to scan in parallel (1=sequential/most conservative, 2=balanced, 3=faster; default: 1)
- `--max-calls-per-hour N` - Total API quota distributed across parallel workers (300=conservative, 450=standard; default: 450)
- `--capacity-filter IDS` - Only scan these capacity IDs (comma-separated)
- `--exclude-capacities IDS` - Skip these capacity IDs (comma-separated)
- `--capacity-priority IDS` - Process these capacity IDs first (comma-separated)

**Incremental Scan Options:**
- `--days N` - Days to look back for modified workspaces (default: 1)
- `--hours N` - Hours to look back (overrides --days if specified)
- `--no-hash-optimization` - Disable hash optimization (scans all modified workspaces)

**Direction Analysis Options:**
- `--with-activity` - Include Activity Event API analysis for inbound connections
- `--activity-days N` - Days of activity history to analyze (default: 30)
- `--output-dir PATH` - Output directory for analysis results

**Configuration & Debug Options:**
- `--config PATH` - Path to configuration file (YAML or JSON)
- `--debug` - Enable detailed debug output (shows workspace processing, item counts, datasources, connections extracted)

**General Options:**
- `--no-personal` - Exclude personal workspaces from scan
- `--workspace-table-source TABLE_NAME` - Read workspace list from lakehouse table instead of API (e.g., workspace_inventory)
- `--table-name NAME` - SQL table name for results (default: tenant_cloud_connections)
- `--curated-dir PATH` - Output directory for curated data
- `--no-merge` - Overwrite existing data instead of merging

**Configuration & Checkpoint Options:**
- `--config PATH` - Path to configuration file (YAML or JSON). See `scanner_config.yaml.example`
- `--enable-checkpoints` - Enable checkpoint/resume for long-running scans (overrides config file)
- `--disable-checkpoints` - Disable checkpoint/resume (overrides config file)
- `--checkpoint-storage TYPE` - Checkpoint storage type: `json` (local files) or `lakehouse` (Fabric storage)
- `--clear-checkpoint ID` - Clear a specific checkpoint file and exit (utility command)

**Lakehouse Upload Options (Local Execution):**

**Note:** When running locally, results are **always saved to `./scanner_output/`** first. These parameters enable **additional upload** to Fabric lakehouse.

- `--upload-to-lakehouse` - Upload results to Fabric lakehouse (in addition to local files)
- `--lakehouse-workspace-id WORKSPACE_ID` - Workspace ID containing the target lakehouse (required if uploading)
- `--lakehouse-id LAKEHOUSE_ID` - Lakehouse ID to upload results to (required if uploading)
- `--lakehouse-upload-path PATH` - Path within lakehouse to upload files (default: Files/scanner)

### Running from Fabric Notebook

To run this scanner from a Microsoft Fabric notebook, follow these steps:

#### 1. **Upload the Script to Your Lakehouse**
   - Open your Fabric workspace
   - Navigate to your Lakehouse
   - Upload `fabric_scanner_cloud_connections.py` to the **Files** section (e.g., `Files/scripts/`)

#### 2. **Configure Authentication Mode**

   Open the script and set the authentication mode to **delegated** (recommended for Fabric):

   ```python
   # Near line 95 in the script
   AUTH_MODE = "delegated"  # Change from "spn" to "delegated"
   ```

   **Authentication Options:**
   - `"delegated"` - Uses your Fabric Admin credentials (recommended for notebooks)
   - `"spn"` - Service Principal (requires TENANT_ID, CLIENT_ID, CLIENT_SECRET)
   - `"interactive"` - Interactive browser login (not recommended for notebooks)

#### 3. **Create a New Notebook Cell**

   In your Fabric notebook, create a cell with the following code:

   ```python
   # Load the scanner script
   %run Files/scripts/fabric_scanner_cloud_connections
   ```

#### 4. **Run a Scan**

   In a new cell, choose your scan mode:

   **Full Tenant Scan:**
   ```python
   # Scan all workspaces
   run_full_scan_v1()
   ```

   **Incremental Scan (Last 24 Hours):**
   ```python
   # Scan workspaces modified in last 24 hours
   run_incremental_scan(hours_back=24)
   ```

   **Incremental Scan (Last 7 Days):**
   ```python
   # Scan workspaces modified in last 7 days
   run_incremental_scan(days_back=7)
   ```

   **Get Results from Previous Scan:**
   ```python
   # Retrieve results using a scan ID
   get_scan_result_by_id(scan_id="your-scan-id-here")
   ```

#### 5. **Access Results**

   Results are automatically saved to your Lakehouse:

   **SQL Table:**
   ```sql
   SELECT * FROM tenant_cloud_connections
   WHERE connector IN ('azuresqldatabase', 'synapse', 'snowflake')
   ORDER BY workspace_name
   ```

   **Raw Files:**
   - Location: `Files/scanner/raw/`
   - Format: JSON files with scan results
   - Naming: `scan_result_YYYYMMDD_HHMMSS.json`

   **Curated Data:**
   - Location: `Tables/dbo/tenant_cloud_connections`
   - Format: Delta table (Parquet)
   - Access: Via SQL endpoint or Spark

#### Key Differences from Local Execution

| Aspect | Fabric Notebook | Local Execution |
|--------|----------------|-----------------|
| **Data Engine** | PySpark (Spark DataFrames) | pandas (DataFrames) |
| **Storage** | Lakehouse Tables (`Tables/dbo/`) | Local files (`./scanner_output/`) |
| **Raw Files** | `Files/scanner/raw/` in Lakehouse | `./scanner_output/raw/` locally |
| **Authentication** | Delegated (default) or SPN | SPN (required) or Interactive |
| **Dependencies** | Auto-available (PySpark, mssparkutils) | Manual install (`requirements.txt`) |
| **SQL Access** | ✅ Yes (via SQL endpoint) | ❌ No (file-based only) |

#### Example: Complete Notebook Workflow

```python
# Cell 1: Load the script
%run Files/scripts/fabric_scanner_cloud_connections

# Cell 2: Run incremental scan (last 24 hours)
run_incremental_scan(hours_back=24)

# Cell 3: Query results
%%sql
SELECT 
    workspace_name,
    connector,
    server,
    database,
    COUNT(*) as connection_count
FROM tenant_cloud_connections
GROUP BY workspace_name, connector, server, database
ORDER BY connection_count DESC
LIMIT 20
```

#### Troubleshooting

**Issue: "Module not found" errors**
- **Solution**: Fabric notebooks include PySpark, mssparkutils, and common libraries by default. No installation needed.

**Issue: "Authentication failed" errors**
- **Solution**: Ensure you have **Fabric Administrator** or **Power BI Administrator** role
- Verify `AUTH_MODE = "delegated"` is set in the script

**Issue: "Table not found" errors**
- **Solution**: First run a scan (full or incremental) to create the table
- Check that the lakehouse is attached to your notebook

**Issue: Rate limit (429) errors**
- **Solution**: Scanner automatically handles rate limits with exponential backoff
- For large tenants, use `--large-shared-tenants` mode (if using CLI) or reduce `MAX_PARALLEL_SCANS`

### Configuration File (NEW)

For production deployments and team collaboration, use a configuration file to manage settings:

1. **Copy the example configuration:**
   ```powershell
   Copy-Item scanner_config.yaml.example scanner_config.yaml
   ```

2. **Edit `scanner_config.yaml`:**
   ```yaml
   # API Settings
   api:
     max_parallel_scans: 1  # Conservative for large shared tenants
     poll_interval_seconds: 20
     scan_timeout_minutes: 30
   
   # Checkpoint Settings
   checkpoint:
     enabled: true
     storage: json  # or lakehouse
     interval: 100  # Save every 100 batches
   
   # Phase 3: Parallel Capacity Scanning (NEW)
   phase3:
     parallel_capacities: 1  # CONSERVATIVE: 1=sequential, 2=balanced, 3=faster
     max_calls_per_hour: 450  # CONSERVATIVE: 300, STANDARD: 450
     capacity_filter: []  # Optional: ["capacity-id-1", "capacity-id-2"]
     exclude_capacities: []  # Optional: ["test-capacity"]
     capacity_priority: []  # Optional: ["prod-capacity"]
   
   # Lakehouse Upload (Local Execution)
   lakehouse:
     upload_enabled: true
     workspace_id: "your-workspace-id"
     lakehouse_id: "your-lakehouse-id"
     upload_path: "Files/scanner"
   
   # Scan Behavior
   scan:
     include_personal: true
     incremental_days_back: 1
     enable_hash_optimization: true
   ```

3. **Use the configuration:**
   ```powershell
   python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --config scanner_config.yaml
   ```

**Complete Configuration Reference:**

See [`scanner_config.yaml.example`](scanner_config.yaml.example) or [`scanner_config.json.example`](scanner_config.json.example) for all available settings:

| Section | Setting | Description | Default |
|---------|---------|-------------|----------|
| **api** | `max_parallel_scans` | Concurrent Scanner API calls | 1 |
| | `poll_interval_seconds` | Scan status check interval | 20 |
| | `scan_timeout_minutes` | Scan timeout duration | 30 |
| **checkpoint** | `enabled` | Enable checkpoint/resume | true |
| | `storage` | Storage type (json/lakehouse) | json |
| | `interval` | Save every N batches | 100 |
| | `directory` | Checkpoint directory | checkpoints |
| **phase3** | `parallel_capacities` | Capacities to scan in parallel (1=sequential, 2=balanced, 3=faster) | 1 |
| | `max_calls_per_hour` | API quota across all workers (300=conservative, 450=standard) | 450 |
| | `capacity_filter` | Only scan these capacity IDs (optional) | [] |
| | `exclude_capacities` | Skip these capacity IDs (optional) | [] |
| | `capacity_priority` | Process these IDs first (optional) | [] |
| **lakehouse** | `upload_enabled` | Upload to lakehouse (local mode) | false |
| | `workspace_id` | Target workspace GUID | "" |
| | `lakehouse_id` | Target lakehouse GUID | "" |
| | `upload_path` | Path within lakehouse | Files/scanner |
| **auth** | `mode` | Auth mode (interactive/spn/delegated) | interactive |
| **output** | `curated_dir` | Output directory | Files/curated/tenant_cloud_connections |
| | `table_name` | SQL table name | tenant_cloud_connections |
| **scan** | `include_personal` | Include personal workspaces | true |
| | `incremental_days_back` | Default incremental lookback | 1 |
| | `enable_hash_optimization` | Smart workspace filtering | true |
| **performance** | `batch_size_workspaces` | Workspaces per batch | 100 |
| | `max_batches_per_hour` | Rate limit (chunked mode) | 450 |
| **debug** | | Enable debug logging | false |

**Benefits of Configuration Files:**
- **Version control**: Track settings changes in Git
- **Team collaboration**: Share standardized settings
- **Environment management**: Different configs for dev/test/prod
- **Override flexibility**: CLI parameters override config file settings
- **Lakehouse integration**: Configure local-to-lakehouse uploads
- **Format flexibility**: Supports both YAML and JSON formats

### Checkpoint/Resume for Long-Running Scans (NEW)

For large tenants (>50k workspaces), scans can take hours or even days. The checkpoint/resume feature prevents data loss from interruptions:

**How It Works:**
- Automatically saves progress every 100 batches (configurable)
- Stores completed batch indices in checkpoint files
- Resumes from last checkpoint if scan is interrupted
- Clears checkpoint automatically on successful completion

**Storage Options:**
1. **JSON (Local Files)** - Default, stores in `checkpoints/` directory
2. **Lakehouse (Fabric Storage)** - More reliable for long runs, survives notebook restarts

**Example: 247k Workspace Tenant**
```powershell
# Enable checkpoints with lakehouse storage
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --enable-checkpoints --checkpoint-storage lakehouse

# If scan is interrupted, simply re-run the same command to resume:
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --enable-checkpoints --checkpoint-storage lakehouse

# Output shows:
# 🔄 Resuming from checkpoint: 1200 batches already completed
# 📦 Total batches: 2470 | Remaining: 1270
```

**Checkpoint Math for Large Tenants:**
- 247k workspaces = ~2470 batches (100 workspaces/batch)
- Checkpoint every 100 batches = ~25 checkpoints during full scan
- At MAX_PARALLEL_SCANS=1: ~1 hour between checkpoints
- **Benefit:** If scan fails at hour 160 (day 6.5), resume from last checkpoint instead of restarting from zero

**Clear Old Checkpoints:**
```powershell
# List checkpoints (look in checkpoints/ directory)
Get-ChildItem checkpoints/

# Clear specific checkpoint
python fabric_scanner_cloud_connections.py --clear-checkpoint full_scan_20250116_103000

# Or manually delete: checkpoints/full_scan_20250116_103000_checkpoint.json
```

### Progress Bars (NEW)

When running locally or in Fabric notebooks with `tqdm` installed, you'll see real-time progress bars:

```
Overall Progress: |████████████████████                | 1200/2470 [48.5%] (5.2 hours)
Chunk 13:         |████████████████████████████████████| 450/450 [100%] (0:58:32)
```

**Install tqdm (optional):**
```powershell
pip install tqdm
```

The script works with or without `tqdm` - it's purely for visual feedback during long scans.
- `--curated-dir PATH` - Output directory for curated data
- `--no-merge` - Overwrite existing data instead of merging

**Examples for Large Shared Tenants (247k workspaces):**

```powershell
# Recommended: Chunked full scan (ultra-safe, respects rate limits)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants

# Daily incremental updates (with hash optimization - super fast!)
python fabric_scanner_cloud_connections.py --incremental --hours 24
```

### Alternative - Using the Orchestrator Function (Python API)

The `run_cloud_connection_scan()` function allows you to choose any combination of features:

```python
# Example 1: Run incremental scan (default - last 24 hours)
run_cloud_connection_scan(
    enable_incremental_scan=True,
    incremental_hours_back=24
)

# Example 2: Run incremental scan for last 6 hours
run_cloud_connection_scan(
    enable_incremental_scan=True,
    incremental_hours_back=6
)

# Example 3: Run incremental scan for last 30 minutes
run_cloud_connection_scan(
    enable_incremental_scan=True,
    incremental_hours_back=0.5
)

# Example 4: Run full baseline scan only (standard - may hit rate limits on large tenants)
run_cloud_connection_scan(
    enable_full_scan=True,
    enable_incremental_scan=False
)

# Example 4b: Run full baseline scan (large shared tenants mode - for tenants with 10K+ workspaces)
# Recommended for large shared tenants to avoid rate limit issues
run_cloud_connection_scan(
    enable_full_scan_chunked=True,
    enable_incremental_scan=False,
    max_batches_per_hour=250  # Conservative: 50% of 500/hour limit (leaves room for others)
)

# Example 4c: Run chunked scan during off-hours (faster completion)
run_cloud_connection_scan(
    enable_full_scan_chunked=True,
    enable_incremental_scan=False,
    max_batches_per_hour=450  # Aggressive: 90% of limit (run when others aren't using API)
)

# Example 5: Retrieve results from a previous scan using scan ID
run_cloud_connection_scan(
    enable_scan_id_retrieval=True,
    scan_id="e7d03602-4873-4760-b37e-1563ef5358e3",
    scan_id_merge_with_existing=True
)

# Example 6: Scan JSON files in a directory
run_cloud_connection_scan(
    enable_json_directory_scan=True,
    json_directory_path="Files/scanner/raw/full",
    json_merge_with_existing=True
)

# Example 7: Combine full scan + JSON directory scan
run_cloud_connection_scan(
    enable_full_scan=True,
    enable_json_directory_scan=True,
    json_directory_path="Files/scanner/raw",
    json_merge_with_existing=False
)

# Example 8: Enable all features
run_cloud_connection_scan(
    enable_full_scan=True,
    enable_incremental_scan=True,
    enable_json_directory_scan=True,
    enable_scan_id_retrieval=True,
    json_directory_path="Files/scanner/archived",
    scan_id="previous-scan-id",
    incremental_hours_back=12,
    include_personal=True
)
```

### Parameters

#### Feature Toggles
- **`enable_full_scan`** (bool): Run full tenant scan (standard - may hit rate limits on large tenants)
- **`enable_full_scan_chunked`** (bool): Run full tenant scan with automatic rate limit management (recommended for 10K+ workspaces)
- **`enable_incremental_scan`** (bool): Run incremental scan for modified workspaces
- **`enable_json_directory_scan`** (bool): Scan JSON files in a directory
- **`enable_scan_id_retrieval`** (bool): Retrieve results from a previous scan using scan ID

#### Time Windows
- **`incremental_hours_back`** (float): Hours to look back for incremental scan (takes precedence, supports fractions)
- **`incremental_days_back`** (float): Days to look back for incremental scan (can be fractional, e.g., 0.5 = 12 hours)

#### Scan Configuration
- **`include_personal`** (bool): Include personal workspaces in API scans
- **`max_batches_per_hour`** (int): Max API calls per hour for chunked scans (default: 250, provides 50% buffer for other users)
- **`json_directory_path`** (str): Path to directory with JSON files (required if JSON scan enabled)
- **`json_merge_with_existing`** (bool): Merge JSON results with existing data or overwrite
- **`scan_id`** (str): Scan ID to retrieve (required if scan ID retrieval enabled)
- **`scan_id_merge_with_existing`** (bool): Merge scan ID results with existing data or overwrite

#### Optimization Settings
- **`enable_hash_optimization`** (bool): Enable smart filtering to skip workspaces scanned within last 24 hours (default: True, saves 80-90% API calls)

#### Output Configuration
- **`curated_dir`** (str): Output directory for curated data (default: "Tables/dbo")
- **`table_name`** (str): SQL table name for results (default: "tenant_cloud_connections")

### Hash Optimization (API Call Reduction)

**Default: Enabled** - Reduces Scanner API calls by **80-90%** on subsequent scans.

The hash optimization tracks when workspaces were last scanned and automatically skips workspaces scanned within the last 24 hours. This dramatically reduces API usage without losing data freshness.

**How it works:**
- Calculates SHA256 hashes of workspace connections to detect changes
- First scan of the day: Processes all modified workspaces (~35 API calls for 500 workspaces)
- Subsequent scans: Skips ~85% already scanned, only processes genuinely new/changed (~7 API calls)
- Saves hashes + scan timestamps to storage (no extra API calls required)
- Storage: `workspace_connection_hashes` table (Fabric) or parquet file (local)

> **See [HASH_OPTIMIZATION_GUIDE.md](HASH_OPTIMIZATION_GUIDE.md) for detailed architecture, troubleshooting, and advanced configuration.**

**When to disable (`enable_hash_optimization=False`):**

1. **Critical/Emergency scans** - Need to scan every workspace regardless of last scan time
2. **Monthly/quarterly audits** - Comprehensive full refresh to ensure nothing was missed
3. **After major system changes** - Migrations, major deployment, or infrastructure changes
4. **Debugging data issues** - Troubleshooting missing connections or suspected stale data
5. **First-time setup** - Although it works fine when enabled, no benefit on very first run

**Example - Monthly full refresh:**
```python
# Daily scans (use optimization - 80-90% API reduction)
run_cloud_connection_scan(
    enable_incremental_scan=True,
    enable_hash_optimization=True  # Default
)

# Monthly comprehensive audit (disable optimization)
run_cloud_connection_scan(
    enable_incremental_scan=True,
    enable_hash_optimization=False  # Force scan everything
)
```

**Monitoring optimization:**
Watch for this output showing API savings:
```
🔍 Using hash-based optimization to reduce API calls...
   Loaded 425 stored hashes from previous scans
✅ Hash optimization complete:
   Skipping 425 workspaces scanned within last 24 hours
   Processing 75 workspaces (85.0% reduction)
```

> **Recommendation:** Keep enabled 95% of the time. Only disable for scheduled full-refresh audits or troubleshooting.

### Alternative - Direct Function Calls (Advanced)

You can also call individual functions directly from Python code:

```python
# Full tenant scan (standard)
full_tenant_scan(include_personal=True)

# Full tenant scan (large shared tenants mode)
full_tenant_scan_chunked(
    include_personal=True,
    max_batches_per_hour=250
)

# Incremental scan (last 6 hours)
since_iso = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat(timespec="seconds").replace("+00:00","Z")
incremental_update(since_iso, include_personal=True)

# Incremental scan (last 30 days - alternative to full baseline scan)
since_iso = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(timespec="seconds").replace("+00:00","Z")
incremental_update(since_iso, include_personal=True)

# Retrieve scan by ID
get_scan_result_by_id(
    scan_id="e7d03602-4873-4760-b37e-1563ef5358e3",
    merge_with_existing=True
)

# JSON directory scan
scan_json_directory_for_connections(
    json_dir_path="Files/scanner/raw",
    merge_with_existing=True
)
```

### Alternative Approach: Skip Full Baseline Scan (Large Tenants)

For very large tenants (>100k workspaces), you can **skip the 7-day baseline scan** entirely and use **longer incremental scans** instead:

**Why This Works:**
- Most workspaces are modified within 30-60 days
- Incremental scan with 30-60 day lookback captures 90-95% of active workspaces
- Much faster than full baseline (hours vs. days)
- Can repeat monthly to catch remaining dormant workspaces

**Example: 30-Day Incremental as Baseline Alternative**
```powershell
# CLI: 30-day incremental scan
python fabric_scanner_cloud_connections.py --incremental --days 30

# CLI: 60-day incremental scan (even more comprehensive)
python fabric_scanner_cloud_connections.py --incremental --days 60

# Python: Same approach
run_cloud_connection_scan(
    enable_incremental_scan=True,
    incremental_days_back=30,  # or 60
    enable_hash_optimization=True
)
```

**Comparison: 247k Workspace Tenant**
| Approach | Duration | Coverage | API Calls | Best For |
|----------|----------|----------|-----------|----------|
| Full baseline (chunked) | 7 days | 100% | ~20,000 | Complete audit |
| 60-day incremental | 2-3 hours | ~95% | ~1,500 | Fast initial setup |
| 30-day incremental | 1-2 hours | ~90% | ~800 | Monthly updates |
| 7-day incremental | 30-45 min | ~75% | ~300 | Weekly updates |
| Daily incremental | 5-10 min | ~10% | ~40 | Daily monitoring |

**Recommended Strategy for Large Tenants:**
1. **Initial setup**: 60-day incremental scan (captures most active workspaces)
2. **Fill gaps**: Repeat 60-day scan after 1 month (catches dormant workspaces)
3. **Ongoing**: Daily or weekly incremental scans with hash optimization
4. **Quarterly**: Optional full baseline for complete audit

**Pros of This Approach:**
- ✅ Avoid 7-day baseline scan entirely
- ✅ Get actionable data in hours, not days
- ✅ Lower risk of interruption (2 hours vs. 7 days)
- ✅ Less checkpoint/resume complexity
- ✅ Easier to schedule in maintenance windows

**Cons:**
- ⚠️ May miss 5-10% of dormant workspaces (inactive >60 days)
- ⚠️ Not suitable for compliance audits requiring 100% coverage
- ⚠️ Need to repeat monthly for several months to achieve full coverage

### Query Results

After running any scan, query the results table:

```sql
SELECT * FROM tenant_cloud_connections;

-- Filter for cloud connections only
SELECT * FROM tenant_cloud_connections WHERE cloud = true;

-- Group by connector type
SELECT connector, COUNT(*) as connection_count 
FROM tenant_cloud_connections 
GROUP BY connector 
ORDER BY connection_count DESC;
```

## Path Configuration

### In Fabric Notebook
The script uses **Spark-relative paths** for simplicity. You can customize the storage locations:

```python
# Spark-relative paths (recommended format)
RAW_DIR     = "Files/scanner/raw"           # Raw JSON responses
CURATED_DIR = "Tables/dbo"                  # Curated Parquet output

# Or use your own folder structure:
RAW_DIR     = "Files/myfolder/folder2/folder3"
CURATED_DIR = "Tables/myoutput"
```

**Path formats supported:**
- ✅ **Spark-relative**: `"Files/myfolder/subfolder"` or `"Tables/mytable"` (recommended)
- ✅ **Lakehouse URI**: `"lakehouse:/Default/Files/myfolder"` (also works)
- ✅ **Absolute paths**: `"/lakehouse/default/Files/myfolder"` (for advanced use)

The script automatically converts paths for `mssparkutils.fs` operations when needed.

### In Local Execution
Paths are automatically set to local filesystem:

```python
# Automatically configured when running locally
RAW_DIR     = "./scanner_output/raw"           # Raw JSON responses
CURATED_DIR = "./scanner_output/curated"       # Parquet + CSV output
```

Output files:
- `scanner_output/curated/tenant_cloud_connections.parquet`
- `scanner_output/curated/tenant_cloud_connections.csv`

## Understanding API Rate Limits

### What Gets Tracked

The script **tracks API usage locally** (not from Microsoft APIs) to help you avoid hitting limits:

- **Counter**: Increments each time an API call is made to Power BI Admin endpoints
- **Calculation**: `(total_calls / elapsed_seconds) * 3600 = projected calls/hour`
- **Warning Thresholds**:
  - **>450 calls/hour (90%)**: High risk - longer scans may be throttled
  - **>350 calls/hour (70%)**: Moderate - consider off-peak hours
  - **<350 calls/hour (70%)**: Healthy - safe to continue

### How Incremental Scans Scale

**Example from real usage:**
```
3-hour lookback:  245 workspaces → 13 API calls → 462 calls/hour (92%)
6-hour lookback:  490 workspaces → 26 API calls → 920 calls/hour (184%) ❌ EXCEEDS LIMIT
```

**API calls per scan:**
- `modified_workspace_ids()` - 1 call (same regardless of lookback period)
- `post_workspace_info()` - 1 call per 100 workspaces
- `get_scan_status()` - 1 call per batch
- `get_scan_result()` - 1 call per batch

**Rule of thumb**: ~5-6 API calls per 100 modified workspaces

### Best Practices

**✅ DO:**
- Run **frequent short scans** (hourly with `--hours 3`) instead of infrequent long ones
- Use **hash optimization** (enabled by default) - after first scan, it skips unchanged workspaces
- Check the projected rate after each run - if >400/hour, reduce lookback period
- Run during **off-peak hours** if scanning frequently

**❌ DON'T:**
- Run long lookback periods (6+ hours) unless you've verified low workspace activity
- Ignore the rate warnings - they're calculated from your actual usage pattern
- Assume the limit is per-user - it's **tenant-wide** and shared with all Scanner API users

### Why the Calculation is Pessimistic

The script calculates rate as if you sustained the same speed for a full hour:
```
13 calls in 1.7 minutes = 462 calls/hour (if you kept running for 60 minutes)
```

This is **intentionally conservative** - better to underestimate capacity than get throttled (429 errors).

## Notes
- **API Rate Limits**: 500 requests/hour (tenant-wide), 16 concurrent scans maximum
- **Large shared tenant recommendation**: Use `--large-shared-tenants` flag (CLI) or `enable_full_scan_chunked=True` (Python) for tenants with 10,000+ workspaces to avoid rate limit errors
- **Chunked scan behavior**: Automatically processes workspaces in hourly batches, waits between chunks, and saves progress incrementally
- **Rate limit sharing**: The 500/hour limit is shared across all users in your organization. Use `max_batches_per_hour` to leave room for others
- **Retry logic**: Automatic retry with exponential backoff for 429 (rate limit) errors
- **Request timeouts**: All HTTP requests enforce a 30-second timeout (120 seconds for file uploads) to prevent indefinite hangs
- **SQL injection protection**: Table names are validated (alphanumeric, underscores, dots only) before use in any SQL statements
- **Thread-safe API tracking**: API call counter uses a lock for safe concurrent access from parallel workers
- Limits: ≤100 workspace IDs per `getInfo`; poll 30–60s intervals.
- Personal workspaces are **included** when `include_personal=True`.
- **Scan ID retrieval**: Scan results are available for 24 hours after completion.
- **JSON directory scan**: Requires JSON files in the format produced by the Scanner API (with `workspace_sidecar` metadata).
- **Single file mode**: Enable `JSON_SINGLE_FILE_MODE = True` to test individual JSON files.
- **Debug mode**: Enable `DEBUG_MODE = True` to see detailed logging including:
  - JSON structure and payload analysis
  - Workspace table source reads (file paths, row counts, filtering)
  - API call details and fallback behavior
  - Data conversion and validation steps
- **Flexible time windows**: Use `incremental_hours_back` for sub-day precision (e.g., 6 hours, 30 minutes).
- All features can be run independently or in combination.
- Extend `CLOUD_CONNECTORS` set to match your estate's connector types.

## Performance Recommendations

### For Large Shared Tenants (100K+ workspaces)

**⚠️ IMPORTANT**: The 500 API calls/hour limit is **tenant-wide** and **shared** across all users in your organization. Be conservative to avoid impacting others.

**Check API Contention First:**
```powershell
# Always check before running large scans
python fabric_scanner_cloud_connections.py --health-check
# Shows if others are currently using the Scanner API
```

**Initial Baseline Scan:**

**Recommended Settings (Conservative - Shared Tenant):**
- Use CLI: `python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 250`
- Or use `enable_full_scan_chunked=True` with `max_batches_per_hour=250`
- **Why 250?** Leaves 250/hour (50%) for other users/teams
- Expect 10-15 hours for 247K workspaces with these settings
- Run during off-hours or weekends to minimize impact
- Use **High Concurrency** session (16 cores, 32GB RAM) for optimal performance

**Faster Settings (Aggressive - Off-Hours Only):**
- Use `--max-batches-per-hour 400-450` only during nights/weekends when others aren't using API
- Check `--health-check` first to confirm no other active scans
- Expect 6-8 hours for 247K workspaces
- **Risk**: May cause 429 errors if others start using API mid-scan

**Daily Updates:**
- Use CLI: `python fabric_scanner_cloud_connections.py --incremental --hours 24`
- Or use `enable_incremental_scan=True` with `incremental_hours_back=24`
- Fast execution (minutes), well under rate limits
- No special settings needed - incremental scans are lightweight
- Can use Standard session for daily incremental updates

**Example workflow for 247K workspaces (Conservative):**
```powershell
# Week 1: Initial baseline (run once, Friday evening)
# Step 1: Check if anyone else is using the API
python fabric_scanner_cloud_connections.py --health-check

# Step 2: Run conservative scan (leaves room for others)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 250
# Completes in ~12 hours (safe for shared tenant)

# Week 2+: Daily incremental updates (run every morning)
python fabric_scanner_cloud_connections.py --incremental --hours 24
# Only yesterday's changes, completes in minutes
```

**Example workflow for 247K workspaces (Aggressive - Off-Hours):**
```powershell
# Friday 6pm: Confirmed no other users via --health-check
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 450
# Completes by Saturday morning (~6 hours)

# Monday-Friday: Daily incremental updates
python fabric_scanner_cloud_connections.py --incremental --hours 24
```

**Choosing max_batches_per_hour:**

| Value | % of Limit | Shared Tenant Impact | Completion Time (247K) | When to Use |
|-------|-----------|---------------------|----------------------|-------------|
| 200 | 40% | Very safe, 60% for others | 15-18 hours | Highly shared, business hours |
| 250 | 50% | Safe, 50% for others | 12-15 hours | **Recommended default** |
| 300 | 60% | Moderate, 40% for others | 10-12 hours | Off-hours, some sharing |
| 400 | 80% | Aggressive, 20% for others | 7-9 hours | Late night, confirmed no users |
| 450 | 90% | Very aggressive | 6-7 hours | Weekends only, emergency |

## Output Schema

The unified connection schema includes:

**Workspace Information:**
- `workspace_id`, `workspace_name`, `workspace_kind`
- **`workspace_users`** - Comma-separated list of workspace admins/members (up to 5)

**Item Information:**
- `item_id`, `item_name`, `item_type`
- **`item_creator`** - User who created the item (if available)
- **`item_modified_by`** - User who last modified the item (if available)
- **`item_modified_date`** - Last modification timestamp

**Connection Information:**
- `connector` - The type of cloud connector (e.g., azuresqldatabase, snowflake, rest)
- **`target`** - **Consolidated target field** showing the destination in a readable format (e.g., "Server: server.database.windows.net | Database: mydb")
- `server`, `database`, `endpoint` - Individual target components (kept for backwards compatibility)
- `connection_scope` (Cloud/OnPremViaGateway)
- `cloud` (boolean flag)
- `generation` (for Dataflows)

### User/Ownership Information

The Scanner API provides two levels of user information:

1. **Workspace-level users** (`workspace_users`): Shows workspace admins and members who have access to manage the workspace and its items
2. **Item-level creator/modifier** (`item_creator`, `item_modified_by`): Shows who created or last modified specific items (when available in the API response)

> **Note:** The Scanner API does not provide connection-level user information (i.e., which specific user created or uses a particular data source connection). The user fields show workspace and item ownership, which can help identify responsibility and accountability.

### Example Queries

```sql
-- See all connections with workspace owners
SELECT connector, target, workspace_name, workspace_users, item_name
FROM tenant_cloud_connections
WHERE cloud = true
ORDER BY workspace_name;

-- Find connections in workspaces managed by specific user
SELECT connector, target, workspace_name, item_name
FROM tenant_cloud_connections
WHERE workspace_users LIKE '%john.doe@company.com%';

-- Show recently modified items with connections
SELECT item_name, item_modified_by, item_modified_date, connector, target
FROM tenant_cloud_connections
WHERE item_modified_date IS NOT NULL
ORDER BY item_modified_date DESC
LIMIT 20;

-- Group connections by workspace owner
SELECT workspace_users, COUNT(*) as connection_count, 
       COUNT(DISTINCT connector) as unique_connectors
FROM tenant_cloud_connections
WHERE workspace_users IS NOT NULL
GROUP BY workspace_users
ORDER BY connection_count DESC;
```

```sql
SELECT connector, target, workspace_name, item_name, item_type
FROM tenant_cloud_connections
WHERE cloud = true
ORDER BY connector;
```

**Sample Results:**
| connector | target | workspace_name | workspace_users | item_name | item_type |
|-----------|--------|----------------|-----------------|-----------|-----------|
| azuresqldatabase | Server: myserver.database.windows.net \| Database: analytics | Finance WS | john.doe@company.com, jane.smith@company.com | Sales Model | SemanticModel |
| snowflake | Server: xy12345.snowflakecomputing.com \| Database: DW | Data Science | data.team@company.com | Customer360 | SemanticModel |
| rest | Endpoint: https://api.example.com/data | Marketing | marketing.admin@company.com | API Dataflow | Dataflow |

## Frequently Asked Questions (FAQ)

### Checkpoint/Resume

**Q: How does checkpoint/resume handle duplicates when a scan is interrupted and resumed?**

A: The system prevents duplicates through multiple layers:

1. **Batch-level tracking** (primary): Checkpoint stores completed batch indices. On resume, already-completed batches are completely skipped - never re-scanned.
   ```python
   # Example: Resume after interruption at batch 1600
   completed_batch_indices = set([0, 1, 2, ..., 1599])  # From checkpoint
   # Batches 0-1599: SKIPPED (already completed)
   # Batches 1600+: PROCESSED (remaining work)
   ```

2. **Data-level deduplication** (safety net): Even if a batch were somehow processed twice, the merge includes automatic deduplication:
   ```python
   df_combined = df_existing.union(df_new).dropDuplicates(
       ["workspace_id", "item_id", "connector", "server", "database", "endpoint"]
   )
   ```

3. **Incremental merge**: After each chunk, results are merged and deduplicated immediately, so completed chunks are already safe.

**Result**: You can interrupt and resume as many times as needed without worrying about duplicate data.

**Q: Can I change MAX_PARALLEL_SCANS or other settings when resuming a scan?**

A: Yes, but use caution:
- ✅ **Safe to change**: `MAX_PARALLEL_SCANS`, `POLL_INTERVAL_SECONDS`, `SCAN_TIMEOUT_MINUTES`
- ⚠️ **Don't change**: `BATCH_SIZE_WORKSPACES` (100) - changing this invalidates checkpoint batch indices
- ⚠️ **Don't change**: Checkpoint ID or storage type mid-scan

If you need to change batch size, clear the checkpoint and start fresh:
```powershell
python fabric_scanner_cloud_connections.py --clear-checkpoint full_scan_20250116_103000
```

**Q: What's the difference between JSON and Lakehouse checkpoint storage?**

A: Both store the same checkpoint data, but differ in reliability and use cases:

| Feature | JSON (Local Files) | Lakehouse (Fabric Storage) |
|---------|-------------------|---------------------------|
| **Location** | `checkpoints/` directory | Fabric lakehouse Files |
| **Reliability** | ⚠️ Lost if notebook/session restarts | ✅ Survives notebook restarts |
| **Speed** | ✅ Faster (local I/O) | Slightly slower (network) |
| **Best for** | Local execution, short runs (<4 hours) | Fabric notebooks, long runs (>4 hours) |
| **Setup** | None required | Requires mssparkutils |

**Recommendation**: Use `lakehouse` storage for scans >4 hours or in Fabric notebooks that might timeout.

**Q: How much storage do checkpoints use?**

A: Very little - typically 1-5 KB per checkpoint file.

For a 247k workspace tenant:
- Checkpoint every 100 batches = ~25 checkpoints
- Each checkpoint: ~2 KB (stores batch indices + metadata)
- Total storage: ~50 KB for entire scan
- Auto-deleted after successful completion

**Q: Can I pause a scan manually and resume later?**

A: Yes! Simply:
1. Press `Ctrl+C` to interrupt (or let notebook timeout/crash)
2. Re-run the exact same command to resume
3. Checkpoint is loaded automatically

Example:
```powershell
# Start scan Friday evening
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --enable-checkpoints

# Interrupt Saturday morning (Ctrl+C or notebook timeout)
# Resume Saturday evening - picks up where it left off
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --enable-checkpoints
```

**Q: How do I know if my scan is using checkpoints?**

A: Look for these messages in the output:

**On first run:**
```
💾 Checkpointing enabled: Saving every 100 batches to json
📦 Total batches: 2470 | Remaining: 2470
```

**On resume:**
```
🔄 Resuming from checkpoint: 1200 batches already completed
📦 Total batches: 2470 | Remaining: 1270
```

**During scan:**
```
💾 Checkpoint saved: 1300 batches completed
```

**Q: Can I use checkpoints with incremental scans?**

A: Checkpointing is primarily designed for full scans (which can take days). Incremental scans typically complete in minutes, so checkpoints provide less value. However, they work fine if enabled:

```powershell
# Works, but usually unnecessary
python fabric_scanner_cloud_connections.py --incremental --days 60 --enable-checkpoints
```

### Configuration & Dependencies

**Q: Do I need to install tqdm and PyYAML?**

A: No, both are **optional**:

- **tqdm** (progress bars): Script works without it, you just won't see visual progress bars
- **PyYAML** (YAML config files): If not installed, use JSON config files instead, or use CLI parameters

Install if desired:
```powershell
pip install tqdm PyYAML
```

The script detects if they're available and adapts automatically.

**Q: Can I use JSON config files instead of YAML?**

A: Yes! The config loader supports both formats automatically:

```powershell
# YAML config (requires PyYAML)
python fabric_scanner_cloud_connections.py --full-scan --config scanner_config.yaml

# JSON config (no PyYAML needed)
python fabric_scanner_cloud_connections.py --full-scan --config scanner_config.json
```

Both formats support the same settings - just use whichever you prefer.

**JSON Example:**
```json
{
  "api": {
    "max_parallel_scans": 1,
    "poll_interval_seconds": 20,
    "scan_timeout_minutes": 30
  },
  "checkpoint": {
    "enabled": true,
    "storage": "json",
    "interval": 100
  },
  "lakehouse": {
    "upload_enabled": true,
    "workspace_id": "your-workspace-id",
    "lakehouse_id": "your-lakehouse-id",
    "upload_path": "Files/scanner"
  }
}
```

**Q: Should I use CLI parameters, config file, or edit the script directly?**

A: Choose based on your use case:

| Method | Best For | Pros | Cons |
|--------|----------|------|------|
| **CLI Parameters** | One-off runs, testing | Flexible, no files to manage | Long command lines |
| **Config File** | Teams, production, CI/CD | Version control, documentation | Extra file to maintain |
| **Edit Script** | Personal use, legacy | Simple, all in one place | Hard to share, merge conflicts |

**Recommendation**: Use **config file** for teams and production, **CLI parameters** for testing and overrides.

**Q: Can I use both CLI parameters and config file together? What takes precedence?**

A: **Yes!** CLI parameters **override** config file settings.

**Precedence order** (highest to lowest):
1. **CLI parameters** (e.g., `--max-batches-per-hour 300`)
2. **Config file** (e.g., `scanner_config.yaml`)
3. **Script defaults** (hard-coded in `.py` file)

**Example:**
```powershell
# Config file has: max_parallel_scans: 1
# This command uses 3 instead (CLI overrides config)
python fabric_scanner_cloud_connections.py --full-scan --config scanner_config.yaml

# Config file has: checkpoint.enabled: false
# This command enables checkpoints (CLI overrides config)
python fabric_scanner_cloud_connections.py --full-scan --config scanner_config.yaml --enable-checkpoints
```

**Use case**: Keep conservative settings in config file, use CLI to temporarily increase parallelism during off-hours.

### Performance & Rate Limits

**Q: Can I run multiple scans simultaneously?**

A: **Not recommended** - they share the same rate limit:

- ❌ **Don't**: Run full scan + incremental scan at same time
- ❌ **Don't**: Run the same script from multiple notebooks
- ✅ **Do**: Run one scan at a time
- ✅ **Do**: Use `--health-check` to see if others are using the API

Why? All scans in your organization share the **500 API calls/hour** tenant-wide limit. Running multiple scans causes:
- Rate limit errors (429)
- Slower completion for all scans
- Interference with other users' scans

**Q: What happens if I hit the rate limit?**

A: The script handles this automatically:

1. **Automatic retry**: Waits and retries failed requests with exponential backoff
2. **Progress preserved**: Completed batches are saved, so no work is lost
3. **Error message**: You'll see warnings about 429 errors
4. **Resume capability**: If the scan fails completely, resume from checkpoint

To avoid rate limits:
- Use `--large-shared-tenants` for big tenants
- Run `--health-check` before scanning
- Lower `max_batches_per_hour` if others are using the API
- Schedule during off-hours

**Q: How long will a full scan take for my tenant?**

A: Duration depends on workspace count, shared tenant usage, and your settings.

**For Large Shared Tenants (10K+ workspaces) - Conservative Settings:**

**Recommended: `max_batches_per_hour=250` (leaves 50% API capacity for others)**

| Workspaces | Duration (Conservative) | Duration (Aggressive 450) | Notes |
|------------|------------------------|---------------------------|-------|
| 10,000 | 3-4 hours | 1.5-2 hours | Chunked mode recommended |
| 50,000 | 15-20 hours | 6-8 hours | Use checkpoints |
| 100,000 | 1.5-2 days | 12-16 hours | Use checkpoints |
| 247,000 | 3-4 days | 20-24 hours | Use checkpoints + lakehouse storage |

**Small/Dedicated Tenants (<10K workspaces) - Standard Mode:**

| Workspaces | MAX_PARALLEL_SCANS=1 | MAX_PARALLEL_SCANS=3 | Notes |
|------------|---------------------|---------------------|-------|
| 100 | 2-3 minutes | 1 minute | Standard mode fine |
| 1,000 | 15-20 minutes | 5-10 minutes | Standard mode fine |
| 5,000 | 1-1.5 hours | 30-45 minutes | Consider chunked if shared |

**Understanding the Settings:**

**For chunked scans (10K+ workspaces):**
- `max_batches_per_hour` controls speed AND shared tenant impact
- **250** (default) = Conservative, safe for shared tenants, 50% free for others
- **450** = Aggressive, use only during off-hours/weekends

**For standard scans (<10K workspaces):**
- `MAX_PARALLEL_SCANS` controls concurrent API calls
- **1** = Conservative, safe for shared tenants
- **3** = Faster but may impact other users

**Factors that increase scan time:**
- ⚠️ **Shared tenant with active users** (biggest factor - can double or triple duration)
- ⚠️ Peak business hours (slower API responses)
- ⚠️ Lower `max_batches_per_hour` setting (trades speed for being considerate)
- ⚠️ Network latency or connectivity issues
- ⚠️ High API contention (multiple teams scanning)

**Best Practices:**
1. **Always check first**: `python fabric_scanner_cloud_connections.py --health-check`
2. **Start conservative**: Use `max_batches_per_hour=250` for initial scan
3. **Monitor**: Watch for 429 errors (rate limit) - if you see them, you're being too aggressive
4. **Schedule smartly**: Run large scans during off-hours/weekends when possible
5. **Use checkpoints**: For scans >4 hours, enable checkpoints to survive interruptions

**Example Scenarios:**

**Scenario 1: 247K workspaces, shared tenant, business hours**
```powershell
# Ultra-conservative (good citizen, but slow)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 200
# Duration: 4 days, but leaves 60% API for others
```

**Scenario 2: 247K workspaces, shared tenant, Friday evening start**
```powershell
# Moderate (completes over weekend)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 300 --enable-checkpoints
# Duration: 2 days (completes Sunday), leaves 40% API for others
```

**Scenario 3: 247K workspaces, confirmed no other users, Saturday night**
```powershell
# Aggressive (fast completion, confirmed no impact)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 450 --enable-checkpoints
# Duration: 20-24 hours (completes Sunday evening)
```

**Tip**: Run `--health-check` first to check if others are using the API, then choose your settings accordingly.

### Workspace Table Source

**Q: When should I use `--workspace-table-source` instead of letting the scanner discover workspaces via API?**

A: Use workspace table source when you want to:

1. **Reduce API calls**: Skip the GetModifiedWorkspaces API call (saves ~1 API call per full scan)
2. **Pre-filter workspaces**: Only scan specific workspaces from a curated list
3. **Integrate with external systems**: Use workspace lists from governance tools, CMDB, or custom catalogs
4. **Consistent scope**: Ensure all scans use the exact same workspace set
5. **Faster startup**: No waiting for workspace discovery API call

**Don't use it if**:
- You want to discover new workspaces automatically
- Your workspace list changes frequently
- You're doing ad-hoc testing or exploration

**Q: What table schema is required for `--workspace-table-source`?**

A: **Required column:**
- `workspace_id` (string/GUID)

**Optional columns** (enhance the scan):
- `workspace_name` (string) - Used for logging/reporting
- `workspace_type` (string) - Enables `--no-personal` filtering
- `capacity_id` (string) - Enables capacity-based grouping

**Example table creation:**
```sql
-- Create from existing scan results
CREATE TABLE workspace_inventory AS
SELECT DISTINCT
    workspace_id,
    workspace_name,
    workspace_type,
    capacity_id
FROM tenant_cloud_connections;

-- Or create custom filtered list
CREATE TABLE prod_workspaces AS
SELECT workspace_id, workspace_name, workspace_type, capacity_id
FROM tenant_cloud_connections
WHERE workspace_name NOT LIKE '%Test%'
  AND workspace_name NOT LIKE '%Dev%'
  AND workspace_type != 'PersonalGroup';
```

**Q: Can I use a local parquet file for `--workspace-table-source` when running locally?**

A: Yes! The scanner supports both:

**Fabric environment** (reads from Spark table):
```powershell
python fabric_scanner_cloud_connections.py --full-scan --workspace-table-source workspace_inventory
```

**Local environment** (reads from parquet file):
```powershell
# Reads from ./scanner_output/curated/workspace_inventory.parquet
python fabric_scanner_cloud_connections.py --full-scan --workspace-table-source workspace_inventory

# Or specify full path
python fabric_scanner_cloud_connections.py --full-scan --workspace-table-source ./my_workspaces.parquet
```

**Q: What happens if the workspace table read fails?**

A: The scanner **automatically falls back** to API discovery:

```
❌ Error reading workspace table 'bad_table_name': Table or view not found
   Falling back to API call...
📊 Discovered 1,234 workspaces via API
```

This ensures your scan always works, even if the table name is wrong or the table doesn't exist.

**Q: Does `--workspace-table-source` work with all scan modes?**

A: Yes! It works with:
- ✅ Full scan (standard)
- ✅ Full scan (chunked mode with `--large-shared-tenants`)
- ✅ Capacity-based grouping (`--group-by-capacity`)
- ✅ Parallel capacity scanning (`--parallel-capacities`)
- ✅ All filtering options (`--no-personal`, `--capacity-filter`)

**Q: How much does `--workspace-table-source` actually speed things up?**

A: **Minimal speed improvement** for the scan itself (saves 1 API call = ~1-2 seconds), but the real benefits are:

**Primary benefits:**
- **Consistent scope**: Every scan uses the same workspace set (important for compliance/governance)
- **Pre-filtered scans**: Only scan workspaces you care about (could reduce scan time by 50%+ if you exclude many workspaces)
- **External integration**: Use workspace lists from other tools/processes
- **API quota preservation**: Save the GetModifiedWorkspaces call for other operations

**Example scenario where it's valuable:**
```powershell
# Create filtered list once (exclude test/dev workspaces)
CREATE TABLE prod_workspaces AS 
SELECT * FROM workspace_inventory
WHERE workspace_name NOT LIKE '%Test%'
  AND workspace_name NOT LIKE '%Dev%';
# Result: 500 workspaces instead of 2,000

# All future scans use filtered list (4x faster)
python fabric_scanner_cloud_connections.py --full-scan --workspace-table-source prod_workspaces
```

**Q: Can I combine `--workspace-table-source` with `--no-personal`?**

A: Yes, and they work together:

1. **Table read** happens first (loads workspace list from table)
2. **Personal workspace filter** applies second (if `--no-personal` specified and table has `workspace_type` column)

```powershell
# Table has 1,000 workspaces (including 200 personal)
# This command scans 800 workspaces (excludes personal from table)
python fabric_scanner_cloud_connections.py --full-scan --workspace-table-source workspace_inventory --no-personal
```

### Troubleshooting

**Q: How do I verify there are no duplicate records?**

A: Run this SQL query after any scan:

```sql
-- Check for duplicates (should return 0 rows)
SELECT workspace_id, item_id, connector, server, database, endpoint, COUNT(*) as count
FROM tenant_cloud_connections
GROUP BY workspace_id, item_id, connector, server, database, endpoint
HAVING COUNT(*) > 1;
```

If duplicates exist (shouldn't happen), you can manually deduplicate:
```sql
-- Create deduplicated table (if needed)
CREATE TABLE tenant_cloud_connections_dedup AS
SELECT DISTINCT workspace_id, item_id, connector, server, database, endpoint, 
       workspace_name, workspace_kind, workspace_users, item_name, item_type,
       item_creator, item_modified_by, item_modified_date, target, 
       connection_scope, cloud, generation
FROM tenant_cloud_connections;
```

**Q: What happens if a checkpoint file gets corrupted?**

A: The script handles this gracefully:

1. **Automatic fallback**: If checkpoint can't be loaded, starts fresh (logs warning)
2. **No data loss**: Existing scanned data is preserved in the table
3. **Deduplication**: Even if some batches re-scan, `dropDuplicates()` prevents duplicate records

To manually fix:
```powershell
# Option 1: Clear corrupted checkpoint and restart
python fabric_scanner_cloud_connections.py --clear-checkpoint full_scan_20250116_103000

# Option 2: Delete checkpoint file manually
Remove-Item checkpoints/full_scan_20250116_103000_checkpoint.json
```

**Q: My scan seems stuck - how do I check progress?**

A: Several ways to monitor:

1. **Progress bars** (if tqdm installed): Real-time visual feedback
2. **Console output**: Shows "Completed N batches in this chunk"
3. **Check checkpoint file** (JSON storage):
   ```powershell
   Get-Content checkpoints/full_scan_*.json | ConvertFrom-Json
   # Look at: completed_batch_indices.length / total_batches
   ```
4. **Query results table**: See how many workspaces scanned so far
   ```sql
   SELECT COUNT(DISTINCT workspace_id) as workspaces_scanned 
   FROM tenant_cloud_connections;
   ```

If truly stuck (no progress for 10+ minutes):
- Check Fabric notebook logs for errors
- Run `--health-check` to see API health
- Interrupt (Ctrl+C) and resume - checkpoint will preserve progress

**Q: Common configuration mistakes and how to fix them?**

A: Here are the most common issues:

**1. Wrong path format in config file**
```yaml
# ❌ WRONG - Windows-style paths won't work in Fabric
curated_dir: C:\Users\myuser\output

# ✅ CORRECT - Use forward slashes
curated_dir: Files/curated/tenant_cloud_connections
```

**2. Forgetting to enable lakehouse upload**
```yaml
# ❌ WRONG - IDs specified but upload disabled
lakehouse:
  upload_enabled: false  # Still disabled!
  workspace_id: "abc123"
  lakehouse_id: "def456"

# ✅ CORRECT
lakehouse:
  upload_enabled: true
  workspace_id: "abc123"
  lakehouse_id: "def456"
```

**3. YAML indentation errors**
```yaml
# ❌ WRONG - Inconsistent indentation
api:
  max_parallel_scans: 1
    poll_interval_seconds: 20  # Too far indented

# ✅ CORRECT - Consistent 2-space indentation
api:
  max_parallel_scans: 1
  poll_interval_seconds: 20
```

**4. Using string instead of boolean/number**
```yaml
# ❌ WRONG - Strings won't work
checkpoint:
  enabled: "true"  # String, not boolean
  interval: "100"  # String, not number

# ✅ CORRECT
checkpoint:
  enabled: true
  interval: 100
```

**5. Invalid authentication mode**
```yaml
# ❌ WRONG - Invalid mode
auth:
  mode: service_principal  # Not a valid option

# ✅ CORRECT - Must be one of: interactive, spn, delegated
auth:
  mode: spn
```

**Q: Troubleshooting workspace table source issues?**

A: Common problems when using `--workspace-table-source`:

**1. Invalid table name error**
```
Error: Invalid table name 'workspace; DROP TABLE users--'. Only alphanumeric characters, underscores, and dots are allowed.
```

**Cause:** Table name validation prevents SQL injection attacks and empty/invalid names.

**Solution:**
```powershell
# ✅ Valid table names
--workspace-table-source workspace_inventory
--workspace-table-source prod.workspace_catalog
--workspace-table-source my_lakehouse.dbo.workspaces

# ❌ Invalid table names (security risk or invalid format)
--workspace-table-source "workspace; DELETE *"
--workspace-table-source "../../../etc/passwd"
--workspace-table-source ""  # Empty string
--workspace-table-source "   "  # Only whitespace
```

**2. Table not found error**
```
Error: Table 'workspace_inventory' not found
```

**Solution:**
```python
# Verify table exists (run in Fabric notebook):
display(spark.sql("SHOW TABLES"))

# Or check specific table:
try:
    spark.sql("SELECT COUNT(*) FROM workspace_inventory").show()
except:
    print("Table doesn't exist")
```

For local parquet files:
```powershell
# Check file exists
Test-Path "workspace_inventory.parquet"  # Returns True if exists
Test-Path "Files/workspace_inventory.parquet"  # Alternative path
```

**3. Missing workspace_id column**
```
Error: Required column 'workspace_id' not found in table
```

**Solution:**
```sql
-- Check your table schema
DESCRIBE workspace_inventory;

-- Required: workspace_id column must exist
-- Fix by recreating table with correct schema:
CREATE TABLE workspace_inventory AS
SELECT 
    id as workspace_id,  -- Rename if needed
    name as workspace_name,
    type as workspace_type,
    capacityId as capacity_id
FROM your_source_table;
```

**4. Table read succeeds but returns no workspaces**
```
Successfully read 0 workspaces from table. Falling back to API discovery...
```

**Diagnostics:**
```sql
-- Check table has data
SELECT COUNT(*) FROM workspace_inventory;

-- Check for null workspace_id values (these are automatically filtered out)
SELECT COUNT(*) 
FROM workspace_inventory 
WHERE workspace_id IS NULL;

-- Check workspace_id values are valid GUIDs
SELECT workspace_id FROM workspace_inventory 
WHERE workspace_id IS NOT NULL
LIMIT 10;

-- If using --no-personal, check how many non-personal workspaces:
SELECT COUNT(*) 
FROM workspace_inventory 
WHERE workspace_type != 'PersonalGroup'
  AND workspace_id IS NOT NULL;
```

**Note:** Rows with null `workspace_id` are automatically filtered out and logged in DEBUG_MODE.

**5. Performance not improving with table source**

**Common causes:**
- Table doesn't pre-filter workspaces (contains all workspaces)
- Table is slow to query (not optimized, no partitions)
- Network/storage latency for parquet files

**Optimization:**
```sql
-- Create optimized filtered table
CREATE TABLE workspace_inventory_filtered
USING DELTA  -- Use Delta format for better performance
AS
SELECT workspace_id, workspace_name, workspace_type, capacity_id
FROM all_workspaces
WHERE workspace_type NOT IN ('PersonalGroup', 'Test', 'Development')
  AND capacity_id IN ('capacity-prod-1', 'capacity-prod-2');

-- Now use the filtered table
# python ... --workspace-table-source workspace_inventory_filtered
```

**6. Local parquet file path issues**

**Symptoms:**
```
Error: File not found: workspace_inventory.parquet
Trying alternative path: Files/workspace_inventory.parquet
Error: File not found: Files/workspace_inventory.parquet
Falling back to API discovery...
```

**Solutions:**
```powershell
# Check current directory
Get-Location

# Use absolute path if needed
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --workspace-table-source "C:\data\workspace_inventory.parquet"

# Or use relative path from script location
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --workspace-table-source "data/workspace_inventory.parquet"

# Verify file format
(Get-Item workspace_inventory.parquet).Length  # Should show file size
```

**7. Table works in Fabric but fails locally (or vice versa)**

**Expected behavior:**
- In Fabric: Uses `spark.sql()` to read table
- Locally: Uses `pandas.read_parquet()` to read file

**Diagnostics:**
```python
# Check environment
import sys
try:
    from pyspark.sql import SparkSession
    print("Spark available - will try spark.sql() first")
except:
    print("Spark not available - will try parquet files")

try:
    import pandas as pd
    print("Pandas available - can read parquet files")
except:
    print("Pandas not available - install with: pip install pandas")
```

**Solution:**
- Fabric: Ensure table is in default lakehouse or use fully qualified name: `lakehouse_name.workspace_inventory`
- Local: Ensure pandas is installed and parquet file exists in accessible location

**8. Enable DEBUG_MODE for detailed troubleshooting**

When troubleshooting workspace table source issues, enable debug mode to see detailed execution flow:

```python
# In script
DEBUG_MODE = True
```

**Debug output shows:**
```
[DEBUG] full_tenant_scan: Using workspace table source 'workspace_inventory'
[DEBUG] read_workspaces_from_table: Reading from Spark table 'workspace_inventory'
[DEBUG] include_personal=True
📊 Reading workspace list from table: workspace_inventory
[DEBUG] Table row count before filtering: 150
[DEBUG] Filtered out 5 rows with null workspace_id
[DEBUG] Filtering out PersonalGroup workspaces
[DEBUG] Table row count after filtering: 120
[DEBUG] Successfully converted 120 rows to workspace dictionaries
[DEBUG] Sample workspace: {'id': 'abc-123', 'name': 'Sales', 'type': 'Workspace', 'capacityId': 'cap-prod-1'}
✅ Successfully loaded 120 workspaces from table 'workspace_inventory'
[DEBUG] Successfully loaded 120 workspaces from table source
```

**What debug mode reveals:**
- ✅ Which table/file is being read
- ✅ Environment detection (Fabric vs local)
- ✅ File path search attempts and results
- ✅ Row counts before/after filtering
- ✅ Null workspace_id filtering (NEW)
- ✅ Personal workspace filtering details
- ✅ Column availability and validation
- ✅ Sample workspace structure
- ✅ Fallback trigger reasons
- ✅ Pandas availability status
- ✅ Empty table/file detection

**9. API fallback not happening when expected**

If table read fails but script doesn't fall back to API:

**Check:**
```python
# Enable debug mode in script to see fallback logic
DEBUG_MODE = True

# Then run your scan
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --workspace-table-source workspace_inventory
```

**Expected console output:**
```
Successfully read 150 workspaces from table 'workspace_inventory'
# or
Error reading from table 'workspace_inventory': [error details]
Falling back to API workspace discovery...
Successfully retrieved 500 workspaces from API
```

If fallback isn't working, manually verify API access:
```powershell
# Test API access separately
python fabric_scanner_cloud_connections.py --health-check
```

**6. Missing required lakehouse IDs**
```powershell
# ❌ WRONG - IDs not specified
python fabric_scanner_cloud_connections.py --full-scan --upload-to-lakehouse
# Error: LAKEHOUSE_WORKSPACE_ID and LAKEHOUSE_ID required

# ✅ CORRECT - All required parameters
python fabric_scanner_cloud_connections.py --full-scan `
  --upload-to-lakehouse `
  --lakehouse-workspace-id "abc123" `
  --lakehouse-id "def456"
```

**7. Checkpoint storage mismatch**
```powershell
# ❌ WRONG - Started with JSON, switching to lakehouse mid-scan
python fabric_scanner_cloud_connections.py --full-scan --checkpoint-storage json
# ... scan interrupted ...
python fabric_scanner_cloud_connections.py --full-scan --checkpoint-storage lakehouse
# Error: Can't find checkpoint (looking in different storage)

# ✅ CORRECT - Use same storage type to resume
python fabric_scanner_cloud_connections.py --full-scan --checkpoint-storage json
```

**How to validate config file:**
```powershell
# Test config loading
python fabric_scanner_cloud_connections.py --config scanner_config.yaml --health-check

# Check for YAML syntax errors
python -c "import yaml; yaml.safe_load(open('scanner_config.yaml'))"

# Check for JSON syntax errors
python -c "import json; json.load(open('scanner_config.json'))"
```

### Security & Best Practices

**Q: How should I manage Service Principal credentials securely?**

A: **CRITICAL**: Never hardcode credentials in scripts or config files. Use environment variables or Azure Key Vault:

**✅ SECURE Methods (Recommended):**

**1. Environment Variables (Best for local/CI/CD):**
```powershell
# PowerShell (session-only, not persisted)
$env:FABRIC_SP_TENANT_ID = "your-tenant-id"
$env:FABRIC_SP_CLIENT_ID = "your-client-id"
$env:FABRIC_SP_CLIENT_SECRET = "your-secret"

# Linux/Mac (session-only)
export FABRIC_SP_TENANT_ID="your-tenant-id"
export FABRIC_SP_CLIENT_ID="your-client-id"
export FABRIC_SP_CLIENT_SECRET="your-secret"
```

**2. Azure Key Vault (Best for production):**
```python
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://your-vault.vault.azure.net/", credential=credential)

TENANT_ID = client.get_secret("fabric-tenant-id").value
CLIENT_ID = client.get_secret("fabric-client-id").value
CLIENT_SECRET = client.get_secret("fabric-client-secret").value
```

**3. GitHub Secrets (For GitHub Actions):**
```yaml
# .github/workflows/scan.yml
env:
  FABRIC_SP_TENANT_ID: ${{ secrets.FABRIC_SP_TENANT_ID }}
  FABRIC_SP_CLIENT_ID: ${{ secrets.FABRIC_SP_CLIENT_ID }}
  FABRIC_SP_CLIENT_SECRET: ${{ secrets.FABRIC_SP_CLIENT_SECRET }}
```

**❌ INSECURE Methods (Never do this):**
```python
# ❌ DON'T hardcode in script
TENANT_ID = "real-tenant-guid-here"  # SECURITY RISK!
CLIENT_SECRET = "real-secret-here"  # SECURITY RISK!

# ❌ DON'T commit to Git
# scanner_config.yaml with real credentials committed to repo

# ❌ DON'T store in plain text files
# credentials.txt in project directory
```

**Security Checklist:**
- ✅ Use environment variables or Key Vault
- ✅ `.gitignore` is pre-configured (included in repo)
- ✅ Rotate Service Principal secrets regularly (every 90 days)
- ✅ Use separate Service Principals for dev/test/prod
- ✅ Grant minimum required permissions (Fabric.Read.All, not Global Admin)
- ✅ Monitor authentication logs for suspicious activity
- ❌ Never commit credentials to Git (even private repos)
- ❌ Never share credentials via email/chat
- ❌ Never log credentials to console or files

**Built-in Security Hardening:**

The script includes the following security measures:

| Protection | Description |
|---|---|
| **HTTP Request Timeouts** | All HTTP requests enforce a 30-second timeout (120s for file uploads) to prevent indefinite hangs from network issues or unresponsive endpoints. |
| **SQL Identifier Validation** | Table names used in Spark SQL statements are validated against `^[a-zA-Z0-9_\.]+$` to prevent SQL injection. Invalid names raise a `ValueError`. |
| **Thread-safe API Tracking** | The API call counter uses a `threading.Lock` to prevent race conditions when parallel workers update quota statistics concurrently. |
| **Token Caching with Expiry** | Access tokens are cached with a 5-minute pre-expiry buffer and auto-refreshed, avoiding unnecessary credential round-trips. |
| **No Credential Logging** | Secrets and tokens are never printed to console or written to log files. |

**Q: What permissions does the Service Principal need? (Principle of Least Privilege)**

A: Grant **minimum required permissions** only:

**Required API Permissions:**
- `Tenant.Read.All` - Read workspace and item metadata
- `Workspace.Read.All` - Read workspace information

**NOT Required (Don't grant):**
- ❌ `Tenant.ReadWrite.All` - Too broad, allows modifications
- ❌ Global Administrator - Excessive privilege
- ❌ `*.Write.*` - Script is read-only

**Fabric Admin Portal Settings:**
1. Navigate to: **Admin Portal** → **Developer settings**
2. Enable: **"Allow service principals to use Fabric APIs"**
3. Security group: Limit to specific SPN if possible (not "Entire organization")

**Workspace-level permissions:**

**Option 1: Single Service Principal (Simple)**
- Role: **Contributor** (required for both scanning and lakehouse uploads)
- Use when: You want simple setup and don't need separation of concerns

**Option 2: Separate Service Principal Credentials (Recommended - Principle of Least Privilege)**
- **Scanning SPN**: **Viewer** role (read-only for workspace scanning)
- **Upload SPN**: **Contributor** role (write access for lakehouse file uploads)
- Use when: You want to follow security best practices for automated/scheduled scans
- Configure via:
  ```bash
  # Main credentials (for scanning) - Viewer role
  FABRIC_SP_TENANT_ID=your-tenant-id
  FABRIC_SP_CLIENT_ID=your-scanning-spn-client-id
  FABRIC_SP_CLIENT_SECRET=your-scanning-spn-secret
  
  # Upload credentials (for lakehouse writes) - Contributor role
  UPLOAD_TENANT_ID=your-tenant-id
  UPLOAD_CLIENT_ID=your-upload-spn-client-id
  UPLOAD_CLIENT_SECRET=your-upload-spn-secret
  ```

**Option 3: User Authentication for Uploads (Recommended - Manual Runs)**
- **Scanning SPN**: **Viewer** role (read-only for workspace scanning)
- **Upload User**: **Contributor** role (your personal account with write access)
- Use when: Running scans manually and want individual user accountability
- Requires: `pip install msal`
- Configure via:
  ```bash
  # Main credentials (for scanning) - Viewer role
  FABRIC_SP_TENANT_ID=your-tenant-id
  FABRIC_SP_CLIENT_ID=your-scanning-spn-client-id
  FABRIC_SP_CLIENT_SECRET=your-scanning-spn-secret
  
  # Use interactive user auth for uploads
  UPLOAD_USE_USER_AUTH=true
  UPLOAD_TENANT_ID=your-tenant-id  # Optional, defaults to main tenant
  ```
- **Benefits**:
  - Uses your personal Fabric credentials (same as web portal)
  - Audit logs show who uploaded files (better accountability)
  - No need to create separate Service Principal for uploads
  - Browser-based or device code authentication (works in terminals)
  - Token cached to avoid repeated logins during session

**Benefits of separate credentials (Options 2 & 3):**
- ✅ Scanning SPN cannot accidentally modify/delete data
- ✅ If scanning credentials compromised, attacker cannot write to lakehouse
- ✅ Upload credentials only used when explicitly uploading files
- ✅ Easier to rotate upload credentials without affecting scanning operations
- ✅ User auth provides audit trail of who performed uploads

**Audit regularly:**
```powershell
# Review SPN permissions
Get-AzADServicePrincipal -DisplayName "Fabric Scanner SPN" | Get-AzADServicePrincipalAppRole

# Check last authentication
Get-AzureADAuditSignInLogs -Filter "servicePrincipalId eq 'your-spn-id'" -Top 10
```

**Q: Is it safe to run this script from my local machine?**

A: Yes, with proper precautions:

**Security Measures:**
1. **Use authenticated machine**: Work laptop with MFA and device compliance
2. **Network security**: Use corporate VPN, avoid public WiFi
3. **Credential isolation**: Use environment variables, never hardcode
4. **Antivirus**: Keep Windows Defender or endpoint protection enabled
5. **Audit trail**: Script creates logs showing who ran what and when

**Additional Recommendations:**
- ✅ Run from corporate-managed device
- ✅ Use locked screen when away
- ✅ Clear environment variables after scan: `Remove-Item Env:FABRIC_SP_*`
- ✅ Review output for sensitive data before sharing
- ⚠️ Don't run on personal/unmanaged devices
- ⚠️ Don't run over public/untrusted networks

**Q: Does the script log or expose sensitive information?**

A: The script is designed to **NOT log credentials**, but be aware:

**What is NOT logged:**
- ✅ Access tokens (never printed or saved)
- ✅ Client secrets (never printed or saved)
- ✅ User passwords (not used)

**What IS logged (safe):**
- ✅ Tenant ID (first 8 chars only: `12345678...`)
- ✅ Workspace names and IDs (metadata)
- ✅ Connection strings (database servers, endpoints)
- ✅ Scan progress and statistics

**Potential sensitive data in OUTPUT:**
- ⚠️ Connection strings may contain server names
- ⚠️ Workspace names may contain project names
- ⚠️ User emails in `workspace_users` field

**Redact before sharing publicly:**
```sql
-- Redact sensitive columns before sharing
SELECT 
  connector,
  'REDACTED' as server,  -- Hide server names
  'REDACTED' as workspace_users,  -- Hide user emails
  COUNT(*) as connection_count
FROM tenant_cloud_connections
GROUP BY connector;
```

**Q: How do I verify the script hasn't been tampered with?**

A: Validate script integrity before running:

**Method 1: File Hash Verification**
```powershell
# Generate hash
Get-FileHash fabric_scanner_cloud_connections.py -Algorithm SHA256

# Compare with known-good hash from trusted source
# SHA256: <hash-value-here>
```

**Method 2: Git Commit Verification**
```powershell
# Check Git history
git log --oneline fabric_scanner_cloud_connections.py

# View recent changes
git diff HEAD~1 fabric_scanner_cloud_connections.py

# Verify no uncommitted changes
git status
```

**Method 3: Code Review (Before first run)**
- Review authentication functions (`get_access_token_*`)
- Verify no `print(token)` or `print(secret)` statements
- Check no outbound connections to unknown servers
- Confirm all API calls go to `*.powerbi.com` or `*.fabric.microsoft.com`

**Red flags (investigate immediately):**
- ❌ Calls to unknown external APIs
- ❌ Writing credentials to files
- ❌ Sending data to non-Microsoft domains
- ❌ Obfuscated or base64-encoded code
- ❌ Requests for Global Admin permissions

### Data & Results

**Q: Will incremental scans capture new workspaces that were created?**

A: **Yes**, as long as they fall within your time window:

- Workspace created yesterday + `--incremental --days 1` = ✅ Captured
- Workspace created 10 days ago + `--incremental --days 7` = ❌ Missed

For comprehensive coverage:
```powershell
# First run: 60-day incremental (captures most workspaces)
python fabric_scanner_cloud_connections.py --incremental --days 60

# Ongoing: Daily incremental (captures new + modified)
python fabric_scanner_cloud_connections.py --incremental --days 1
```

**Q: Why are some connections missing from the results?**

A: Common reasons:

1. **Workspace not scanned**: Check if workspace is in your scan scope
   - Personal workspaces: Use `--full-scan` (not `--no-personal`)
   - New workspaces: May need longer `--days` for incremental
   
2. **Connection not visible to Scanner API**: Some connection types aren't exposed
   - Dataflows Gen1: Most connections visible
   - Dataflows Gen2: Limited connection metadata
   - Direct Query connections: May appear as dataset connections
   
3. **Permissions**: Scanner API requires Fabric Administrator role
   
4. **Hash optimization**: Disabled by default for incremental, but check:
   ```powershell
   # Force re-scan everything
   python fabric_scanner_cloud_connections.py --incremental --days 7 --no-hash-optimization
   ```

**Q: How do I export results to CSV or Excel?**

A: Several options:

**In Fabric (from SQL table):**
```python
# Export to CSV in lakehouse
df = spark.sql("SELECT * FROM tenant_cloud_connections")
df.write.mode("overwrite").option("header", True).csv("Files/exports/connections.csv")

# Or pandas for smaller datasets
import pandas as pd
df_pd = df.toPandas()
df_pd.to_csv("connections.csv", index=False)
df_pd.to_excel("connections.xlsx", index=False)
```

**In Local Execution:**
Results are automatically saved as CSV:
```
./scanner_output/curated/tenant_cloud_connections.csv
```

**Q: Can I scan a specific workspace or subset of workspaces?**

A: Not directly via CLI, but you can modify the code:

```python
# Scan specific workspaces by ID
workspace_ids = ["workspace-id-1", "workspace-id-2", "workspace-id-3"]
ws_list = [{"id": ws_id, "name": "", "type": "workspace"} for ws_id in workspace_ids]

batches = [ws_list[i:i+100] for i in range(0, len(ws_list), 100)]
# Then call run_one_batch() for each batch
```

For more targeted scanning, use incremental scans with time windows to focus on recently modified workspaces.

### Local Execution & Lakehouse Integration

**Q: Can I run the script locally and still output results to a Fabric lakehouse?**

A: **Yes!** When running locally, the script:

**ALWAYS does this:**
- ✅ Saves results to **local files** in `./scanner_output/`
  - `scanner_output/raw/` - JSON responses from API
  - `scanner_output/curated/` - Parquet and CSV files

**OPTIONALLY does this (if configured):**
- ⬆️ Uploads same results to **Fabric lakehouse**
  - Same file structure in lakehouse `Files/scanner/`
  - Enables SQL querying via Fabric

**Why use lakehouse upload:**
- ✅ Run on your local machine (easier debugging, no notebook timeouts)
- ✅ Use your local Python environment and tools
- ✅ **Still store results centrally** in Fabric lakehouse
- ✅ Results accessible via Fabric SQL queries
- ✅ Team members can access results without local files

**Setup:**

**To enable lakehouse upload (optional):**

**Option 1: Using CLI Parameters (Recommended)**

```powershell
# Authenticate with Service Principal
$env:FABRIC_SP_TENANT_ID = "your-tenant-id"
$env:FABRIC_SP_CLIENT_ID = "your-client-id"
$env:FABRIC_SP_CLIENT_SECRET = "your-secret"

# Run the scan with lakehouse upload
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants `
  --upload-to-lakehouse `
  --lakehouse-workspace-id "your-workspace-id" `
  --lakehouse-id "your-lakehouse-id"

# Results saved to ./scanner_output/ AND uploaded to lakehouse
```

**Option 2: Using Configuration File**

1. **Edit `scanner_config.yaml`:**
   ```yaml
   lakehouse:
     upload_enabled: true
     workspace_id: "your-workspace-id"
     lakehouse_id: "your-lakehouse-id"
     upload_path: "Files/scanner"
   ```

2. **Run with config file:**
   ```powershell
   python fabric_scanner_cloud_connections.py --full-scan --config scanner_config.yaml
   # Results saved to ./scanner_output/ AND uploaded to lakehouse
   ```

**Option 3: Edit Script Directly**

Edit `fabric_scanner_cloud_connections.py` (lines 98-101):
```python
UPLOAD_TO_LAKEHOUSE = True
LAKEHOUSE_WORKSPACE_ID = "your-workspace-id"
LAKEHOUSE_ID = "your-lakehouse-id"
LAKEHOUSE_UPLOAD_PATH = "Files/scanner"
```

**Results location:**
   - **Local (always):** `./scanner_output/curated/tenant_cloud_connections.parquet`
   - **Lakehouse (if enabled):** `Files/scanner/curated/tenant_cloud_connections.parquet`

**What gets uploaded (if enabled):**
- ✅ Parquet file (optimized for Fabric)
- ✅ CSV file (for easy viewing)
- ✅ Raw JSON files (optional, if enabled)
- ✅ Automatically creates lakehouse directory structure
- ✅ Uses same Service Principal authentication

**Requirements:**
- Service Principal with:
  - Scanner API access (for scanning)
  - Fabric workspace contributor role (for lakehouse upload)
- Internet connection (uploads via Fabric REST API)

**Q: How do I prevent my workstation from going to sleep during a long local scan?**

A: For large tenants, scans can take **hours**. Windows may put your computer to sleep, interrupting the scan.

**Quick Solution (PowerShell):**

```powershell
# Before starting scan - disable sleep temporarily
powercfg /change standby-timeout-ac 0

# Run your scan with checkpoints enabled
python fabric_scanner_cloud_connections.py --full-scan --enable-checkpoints

# After scan completes - restore sleep settings
powercfg /change standby-timeout-ac 30  # 30 minutes
```

**Alternative Solutions:**

**1. Presentation Mode (Easiest):**
```powershell
presentationsettings /start  # Prevents sleep
python fabric_scanner_cloud_connections.py --full-scan --enable-checkpoints
presentationsettings /stop   # Re-enable sleep
```

**2. GUI Method:**
- Settings → System → Power & Sleep
- Set "When plugged in, PC goes to sleep after" → **Never**
- ⚠️ Remember to restore after scan!

**3. Run as Scheduled Task (Advanced):**
Scheduled tasks can prevent sleep and run even when locked:
```powershell
$action = New-ScheduledTaskAction -Execute "python" -Argument "fabric_scanner_cloud_connections.py --full-scan --enable-checkpoints"
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName "FabricScanner" -Action $action -Settings $settings -RunLevel Highest
```

**Best Practice:**
- ✅ **Always use `--enable-checkpoints`** - Automatically resumes if interrupted
- ✅ Keep laptop plugged in during scan
- ✅ Disable sleep temporarily (restore after)
- ✅ For very large tenants, consider running overnight or on a dedicated VM

**If Scan Is Interrupted:**
Simply re-run with checkpoints enabled - it will resume from where it stopped:
```powershell
python fabric_scanner_cloud_connections.py --full-scan --enable-checkpoints
# Resumes automatically from last checkpoint
```

See [README_LOCAL_EXECUTION.md](README_LOCAL_EXECUTION.md#preventing-sleep-during-long-running-scans) for more details.

**Q: What's the difference between running locally vs. in Fabric notebook?**

A: Key differences:

| Aspect | Local Execution | Fabric Notebook |
|--------|----------------|-----------------|
| **Data processing** | pandas (Python) | PySpark (distributed) |
| **Output format** | Parquet + CSV files | Lakehouse Tables (SQL) |
| **Authentication** | Service Principal only | Delegated or SPN |
| **Progress** | Terminal output + tqdm | Notebook output |
| **Checkpoints** | JSON files (local) | JSON or Lakehouse |
| **Best for** | Development, debugging, CI/CD | Production, large datasets |
| **Upload to lakehouse** | Optional (via UPLOAD_TO_LAKEHOUSE) | Built-in (native lakehouse) |

**Use local execution when:**
- 🔧 Developing or debugging the script
- 🚀 Running from CI/CD pipelines
- 💻 You prefer local Python environments
- ⏱️ Need to avoid notebook session timeouts
- 🎯 Want local file outputs for analysis

**Use Fabric notebook when:**
- 📊 Processing very large result sets (PySpark scales better)
- 🔄 Results need to be immediately queryable via SQL
- 👥 Multiple users need access to results
- 🏢 Organization prefers notebook-based workflows
- 🔐 Using delegated authentication (user identity)

**Q: What happens if I specify lakehouse upload settings but I'm running in Fabric notebook?**

A: The lakehouse upload settings are **ignored** when running in Fabric notebooks because Fabric has **native lakehouse integration**.

**How Native Lakehouse Saves Work (Fabric Notebooks):**

When running in Fabric, the script uses **direct lakehouse access** via Spark and mssparkutils:

```python
# Fabric Notebook - Native Integration
# 1. Detect environment
RUNNING_IN_FABRIC = True  # Auto-detected

# 2. Write directly using Spark
df.write.mode("overwrite").parquet("Files/scanner/curated/tenant_cloud_connections.parquet")

# 3. Create SQL table using Spark SQL
spark.sql(f"CREATE TABLE tenant_cloud_connections USING PARQUET LOCATION 'Files/scanner/curated/tenant_cloud_connections.parquet'")

# Result: Data appears instantly in attached lakehouse
# - Files visible in lakehouse Files explorer
# - Table queryable via SQL endpoint
# - No authentication or upload needed (notebook already has access)
```

**Advantages of Native Fabric Approach:**
- ✅ **Direct write**: No upload step, writes directly to lakehouse storage
- ✅ **Automatic authentication**: Uses notebook's delegated or service principal credentials
- ✅ **Instant availability**: SQL table created immediately
- ✅ **Spark optimizations**: Leverages Spark's distributed write capabilities
- ✅ **No REST API calls**: Doesn't consume API quota

**How Lakehouse Upload Works (Local Execution):**

When running locally, the script must **upload via REST API** because it has no direct lakehouse access:

```python
# Local Execution - REST API Upload
# 1. Detect environment
RUNNING_IN_FABRIC = False  # Auto-detected

# 2. Save to local files first
df.to_parquet("./scanner_output/curated/tenant_cloud_connections.parquet")

# 3. If UPLOAD_TO_LAKEHOUSE is enabled, upload via API
if UPLOAD_TO_LAKEHOUSE:
    # Authenticate using Service Principal
    token = get_access_token_service_principal()
    
    # Upload file to lakehouse using Fabric Files REST API
    upload_url = f"https://api.fabric.microsoft.com/v1/workspaces/{LAKEHOUSE_WORKSPACE_ID}/lakehouses/{LAKEHOUSE_ID}/files/upload"
    
    with open("./scanner_output/curated/tenant_cloud_connections.parquet", "rb") as f:
        response = requests.post(upload_url, headers={"Authorization": f"Bearer {token}"}, files={"file": f})
    
    # Result: File uploaded to lakehouse Files, but SQL table NOT auto-created
```

**Key Differences:**

| Aspect | Fabric Native | Local Upload |
|--------|---------------|--------------|
| **Write Method** | `spark.write.parquet()` | REST API `POST /files/upload` |
| **Authentication** | Notebook credentials (automatic) | Service Principal (manual config) |
| **SQL Table** | Auto-created via `spark.sql()` | NOT created (files only) |
| **Speed** | Fast (direct write) | Slower (network upload) |
| **Dependencies** | mssparkutils, Spark | requests, pandas |
| **Lakehouse Settings Needed** | None (uses attached lakehouse) | Workspace ID + Lakehouse ID required |

**Environment Detection (Automatic):**

The script automatically detects where it's running:

```python
# Auto-detection logic
try:
    from notebookutils import mssparkutils
    from pyspark.sql import SparkSession
    RUNNING_IN_FABRIC = True
    print("✅ Detected: Running in Fabric notebook (native lakehouse access)")
except ImportError:
    RUNNING_IN_FABRIC = False
    print("✅ Detected: Running locally (will use pandas + optional REST upload)")

# Then adapts behavior
if RUNNING_IN_FABRIC:
    # Use Spark for everything
    # UPLOAD_TO_LAKEHOUSE, LAKEHOUSE_WORKSPACE_ID, LAKEHOUSE_ID all IGNORED
    save_with_spark(df, "Files/scanner/curated/tenant_cloud_connections.parquet")
else:
    # Use pandas, save locally
    df.to_parquet("./scanner_output/curated/tenant_cloud_connections.parquet")
    
    # Optionally upload to lakehouse if configured
    if UPLOAD_TO_LAKEHOUSE:
        if not LAKEHOUSE_WORKSPACE_ID or not LAKEHOUSE_ID:
            raise ValueError("Lakehouse upload enabled but IDs not configured")
        upload_to_fabric_lakehouse(df, LAKEHOUSE_WORKSPACE_ID, LAKEHOUSE_ID)
```

**Bottom Line:**
- **In Fabric**: Lakehouse settings completely ignored - uses attached lakehouse automatically
- **Locally**: Lakehouse settings control whether/where to upload (optional feature)
- **Config files**: Safe to include lakehouse settings - script ignores them when not needed

**Q: How do I verify lakehouse upload succeeded when running locally?**

A: Check the console output and verify in Fabric portal:

**1. Console output shows upload status:**
```
✅ Results saved locally: ./scanner_output/curated/tenant_cloud_connections.parquet
📤 Uploading to Fabric lakehouse...
✅ Uploaded to lakehouse: Files/scanner/tenant_cloud_connections.parquet
```

**2. Verify in Fabric portal:**
- Navigate to workspace → Open lakehouse
- Browse to `Files/scanner/`
- Look for `tenant_cloud_connections.parquet`
- Check file timestamp matches scan completion time

**3. Query via SQL endpoint:**
```sql
-- In Fabric SQL endpoint
SELECT COUNT(*) as row_count, MAX(item_modified_date) as latest_scan
FROM OPENROWSET(
    BULK 'Files/scanner/tenant_cloud_connections.parquet',
    FORMAT = 'PARQUET'
) AS connections;
```

**4. Troubleshooting lakehouse uploads:**

**Check if lakehouse upload is configured:**
```powershell
# Option 1: Full debug output (includes lakehouse config + detailed logging)
python fabric_scanner_cloud_connections.py --incremental --hours 3 --debug

# Option 2: Just lakehouse configuration (no other debug output)
python fabric_scanner_cloud_connections.py --incremental --hours 3 --lakehouse-upload-debug
```

This will show:
- Whether lakehouse upload is enabled/disabled
- Workspace ID and Lakehouse ID being used
- Upload path configuration
- Missing configuration values (if any)

**Expected output when properly configured:**
```
[DEBUG] Lakehouse upload: ENABLED
[DEBUG]   Workspace ID: abc123...
[DEBUG]   Lakehouse ID: def456...
[DEBUG]   Upload path: Files/scanner/YOUR_PREFIX
```

**If configuration is missing:**
```
[DEBUG] ⚠️  Lakehouse upload configured but missing workspace_id or lakehouse_id
[DEBUG]   UPLOAD_TO_LAKEHOUSE: True
[DEBUG]   LAKEHOUSE_WORKSPACE_ID: NOT SET
[DEBUG]   LAKEHOUSE_ID: NOT SET
```

**Verify Service Principal permissions:**
```powershell
# Need: Workspace Contributor role in target workspace

# Check .env file has correct IDs
cat .env | Select-String "LAKEHOUSE"
```

**Common issues:**
- Files uploaded successfully locally but not appearing in lakehouse → Wrong workspace/lakehouse IDs
- "✅ Uploaded" but files not there → API returning success but not persisting (permissions issue)
- 404 errors → Wrong lakehouse ID or workspace ID (Fabric API auto-creates directories during file upload)

**5. Troubleshooting authentication:**
```powershell
# Test authentication token
python -c "from fabric_scanner_cloud_connections import get_token; print('Token:', get_token()[:50])"
```
