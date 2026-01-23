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

**Option 1: Using .env file (Recommended for local development)**

Rename `.env.template` to `.env` and edit with your credentials:

```bash
# On Windows/PowerShell
Copy-Item .env.template .env

# On Linux/Mac
cp .env.template .env
```

Then edit `.env` file:

```bash
FABRIC_SP_TENANT_ID=your-tenant-id
FABRIC_SP_CLIENT_ID=your-app-client-id
FABRIC_SP_CLIENT_SECRET=your-app-secret

# Optional: Upload results to Fabric Lakehouse
LAKEHOUSE_WORKSPACE_ID=your-workspace-id
LAKEHOUSE_ID=your-lakehouse-id
```

**Note:** The `.env` file is in `.gitignore` so your credentials won't be committed to Git.

**Option 2: Using environment variables (Recommended for production/CI)**

```powershell
# PowerShell
$env:FABRIC_SP_TENANT_ID = "your-tenant-id"
$env:FABRIC_SP_CLIENT_ID = "your-client-id"
$env:FABRIC_SP_CLIENT_SECRET = "your-secret"
```

### 4. Service Principal Setup

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

**Option 1: Using CLI (Recommended)**

```bash
# Sequential scan (most conservative)
python fabric_scanner_cloud_connections.py --full-scan

# Parallel capacity scanning - Conservative (2 workers, reduced quota)
python fabric_scanner_cloud_connections.py --full-scan \
  --parallel-capacities 2 \
  --max-calls-per-hour 300

# Parallel capacity scanning - Balanced (2 workers, standard quota)
python fabric_scanner_cloud_connections.py --full-scan \
  --parallel-capacities 2 \
  --max-calls-per-hour 450

# Parallel capacity scanning - Faster (3 workers, full quota)
python fabric_scanner_cloud_connections.py --full-scan \
  --parallel-capacities 3 \
  --max-calls-per-hour 450

# With capacity filtering (only scan specific capacities)
python fabric_scanner_cloud_connections.py --full-scan \
  --parallel-capacities 2 \
  --capacity-filter "prod-capacity-id,critical-capacity-id"

# With capacity exclusion (skip test/dev environments)
python fabric_scanner_cloud_connections.py --full-scan \
  --parallel-capacities 2 \
  --exclude-capacities "test-capacity,dev-capacity"
```

**Option 2: Edit the script directly**

Edit the `if __name__ == "__main__":` section at the bottom of `fabric_scanner_cloud_connections.py`:

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

### Local Files (ALWAYS created)

When running locally, data is **always saved** to:

```
scanner_output/
├── raw/               # Raw JSON responses from Scanner API
│   ├── full/          # Full scan results
│   └── incremental/   # Incremental scan results
└── curated/           # Processed data
    ├── tenant_cloud_connections.parquet
    └── tenant_cloud_connections.csv
```

### Optional: Upload to Fabric Lakehouse

**In addition to local files**, you can optionally upload results to a Fabric Lakehouse:

1. **Enable upload in script:**
   ```python
   UPLOAD_TO_LAKEHOUSE = True
   ```

2. **Configure lakehouse connection (optional - only if you want to upload):**
   ```bash
   # Set environment variables (only needed for lakehouse upload)
   LAKEHOUSE_WORKSPACE_ID=your-workspace-id
   LAKEHOUSE_ID=your-lakehouse-id
   LAKEHOUSE_UPLOAD_PATH=Files/scanner  # Optional, defaults to Files/scanner
   ```

   **Note:** If you don't set these variables, results will still be saved to `./scanner_output/` - the lakehouse upload will simply be skipped.

3. **Choose authentication approach:**

   **Option A: Single Service Principal (Simple Setup)**
   ```bash
   # Use the same Service Principal for both scanning and uploads
   # Requires: Contributor role in workspace (for write access)
   FABRIC_SP_TENANT_ID=your-tenant-id
   FABRIC_SP_CLIENT_ID=your-spn-client-id
   FABRIC_SP_CLIENT_SECRET=your-spn-secret
   ```

   **Option B: Separate Service Principal (Recommended - Automated Scans)**
   ```bash
   # Main credentials for scanning (Viewer role - read-only)
   FABRIC_SP_TENANT_ID=your-tenant-id
   FABRIC_SP_CLIENT_ID=your-scanning-spn-client-id
   FABRIC_SP_CLIENT_SECRET=your-scanning-spn-secret
   
   # Separate credentials for uploads (Contributor role - write access)
   UPLOAD_TENANT_ID=your-tenant-id
   UPLOAD_CLIENT_ID=your-upload-spn-client-id
   UPLOAD_CLIENT_SECRET=your-upload-spn-secret
   ```
   
   **Option C: User Authentication for Uploads (Recommended - Manual Runs)**
   ```bash
   # Main credentials for scanning (Viewer role - read-only)
   FABRIC_SP_TENANT_ID=your-tenant-id
   FABRIC_SP_CLIENT_ID=your-scanning-spn-client-id
   FABRIC_SP_CLIENT_SECRET=your-scanning-spn-secret
   
   # Use your personal Fabric account for uploads
   UPLOAD_USE_USER_AUTH=true
   ```
   
   Install required library for user auth:
   ```powershell
   pip install msal
   ```
   
   **Benefits of user authentication:**
   - ✅ Uses your personal Fabric credentials (same as web portal)
   - ✅ Audit logs show who uploaded files (better accountability)
   - ✅ No need to create separate Service Principal for uploads
   - ✅ Browser-based or device code login (works in terminals)
   - ✅ Token cached to avoid repeated logins during session
   
   **Benefits of separate Service Principal:**
   - ✅ Best for automated/scheduled scans
   - ✅ No interactive login required
   - ✅ Works in headless environments
   
   **Benefits of both Options B & C:**
   - ✅ Scanning SPN cannot accidentally modify/delete lakehouse data
   - ✅ If scanning credentials compromised, attacker cannot write files
   - ✅ Upload credentials only used when explicitly uploading
   - ✅ Easier to rotate upload credentials without affecting scans
   - ✅ Follows security best practice: minimal permissions for read operations

4. **Run the script** - Results will be:
   - `Files/scanner/raw/full/*.json` - Raw scan data
   - `Files/scanner/raw/incremental/*.json` - Incremental scan data
   - `Files/scanner/curated/tenant_cloud_connections.parquet` - Curated data (parquet)
   - `Files/scanner/curated/tenant_cloud_connections.csv` - Curated data (CSV)

**How to find your IDs:**
- **Workspace ID**: In Fabric, open workspace → URL shows `/groups/{workspace-id}`
- **Lakehouse ID**: Open lakehouse → URL shows `...&objectId={lakehouse-id}`

Files are uploaded using the Fabric REST API with your service principal credentials.

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

### Verifying Lakehouse Upload Configuration

If files are saved locally but not appearing in the lakehouse:

```powershell
# Option 1: Full debug output (includes lakehouse config + detailed API logging)
python fabric_scanner_cloud_connections.py --incremental --hours 3 --debug

# Option 2: Just lakehouse configuration (no other debug output)
python fabric_scanner_cloud_connections.py --incremental --hours 3 --lakehouse-upload-debug
```

**Expected output when properly configured:**
```
[DEBUG] Lakehouse upload: ENABLED
[DEBUG]   Workspace ID: abc123...
[DEBUG]   Lakehouse ID: def456...
[DEBUG]   Upload path: Files/scanner/YOUR_PREFIX
[DEBUG]   Upload auth: Main Service Principal (FABRIC_SP_TENANT_ID/CLIENT_ID)
```

**If using separate Service Principal for uploads:**
```
[DEBUG] Lakehouse upload: ENABLED
[DEBUG]   Workspace ID: abc123...
[DEBUG]   Lakehouse ID: def456...
[DEBUG]   Upload path: Files/scanner/YOUR_PREFIX
[DEBUG]   Upload auth: Separate Service Principal (UPLOAD_TENANT_ID/CLIENT_ID)
```

**If using user authentication for uploads:**
```
[DEBUG] Lakehouse upload: ENABLED
[DEBUG]   Workspace ID: abc123...
[DEBUG]   Lakehouse ID: def456...
[DEBUG]   Upload path: Files/scanner/YOUR_PREFIX
[DEBUG]   Upload auth: Interactive user authentication (UPLOAD_USE_USER_AUTH=true)

🔐 User authentication required for lakehouse uploads...
   Opening browser for login (or follow device code instructions)...

To sign in, use a web browser to open the page https://microsoft.com/devicelogin
and enter the code ABC123DEF to authenticate.

✅ User authentication successful!
```

**If configuration is missing or incorrect:**
- Check your `.env` file has `LAKEHOUSE_WORKSPACE_ID` and `LAKEHOUSE_ID`
- Verify IDs are correct (not placeholder values)
- **Permissions check**:
  - **Single SPN**: Requires **Contributor** role in workspace
  - **Separate credentials**: 
    - Scanning SPN needs **Viewer** role (or Admin for full scanning)
    - Upload SPN needs **Contributor** role
- Check that `UPLOAD_TO_LAKEHOUSE=True` in `.env`

**Common issues:**
- ✅ "Uploaded to lakehouse" message but files not there → Wrong workspace/lakehouse IDs
- ✅ 404 errors with valid IDs → **Permissions issue** (Viewer role cannot write)
  - Solution 1: Upgrade to Contributor role
  - Solution 2: Use separate upload credentials with Contributor role
- ✅ API returns 200 but files don't persist → Service Principal lacks write permission
- ⚠️ 404 "EntityNotFound" can mean permissions issue, not literally missing resource (security through obscurity)

### Preventing Sleep During Long-Running Scans

**Problem:** For large tenants (>10k workspaces), scans can take hours. If your workstation goes to sleep, the scan will be interrupted.

**Solutions:**

**Option 1: PowerShell Keep-Awake Command (Recommended)**

Run the script with PowerShell's `-NoSleep` equivalent using `Start-Process`:

```powershell
# Keep system awake during scan
powercfg /change standby-timeout-ac 0  # Disable sleep on AC power
powercfg /change standby-timeout-dc 0  # Disable sleep on battery

# Run your scan
python fabric_scanner_cloud_connections.py --full-scan --enable-checkpoints

# Restore power settings after scan
powercfg /change standby-timeout-ac 30  # Restore to 30 minutes
powercfg /change standby-timeout-dc 15  # Restore to 15 minutes
```

**Option 2: Windows Presentation Mode (Easiest)**

```powershell
# Enable Presentation Mode (prevents sleep, keeps display on)
presentationsettings /start

# Run your scan
python fabric_scanner_cloud_connections.py --full-scan --enable-checkpoints

# Disable Presentation Mode after scan
presentationsettings /stop
```

**Option 3: Caffeinate Utility (Third-party)**

Install `caffeinate` for Windows:
```powershell
winget install --id=den4b.Shutter -e
# Or use https://github.com/haimgel/display-switch or similar tools
```

**Option 4: Change Power Settings Temporarily**

1. **Via GUI:**
   - Settings → System → Power & Sleep
   - Set "When plugged in, PC goes to sleep after" → **Never**
   - Remember to restore after scan!

2. **Via PowerShell:**
   ```powershell
   # Save current settings
   $currentAC = (powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE).Split(":")[-1].Trim()
   
   # Disable sleep
   powercfg /change standby-timeout-ac 0
   
   # Run scan
   python fabric_scanner_cloud_connections.py --full-scan
   
   # Restore settings
   powercfg /change standby-timeout-ac 30
   ```

**Best Practice: Use Checkpoints**

Even with sleep prevention, enable checkpoints to resume if interrupted:

```powershell
# If scan is interrupted (sleep, network issue, etc.), simply re-run to resume
python fabric_scanner_cloud_connections.py --full-scan --enable-checkpoints
```

Checkpoints save progress every chunk, so you won't lose work if interrupted.

**Alternative: Run in Background/Scheduled Task**

For very large scans, consider running as a scheduled task that runs even when locked:

```powershell
# Create scheduled task that prevents sleep
$action = New-ScheduledTaskAction -Execute "python" -Argument "fabric_scanner_cloud_connections.py --full-scan --enable-checkpoints"
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1)
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 24)
Register-ScheduledTask -TaskName "FabricScanner" -Action $action -Trigger $trigger -Settings $settings
```

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
