
# Microsoft Fabric — Scanner API Cloud Connections Inventory (PySpark Notebook)
# Full tenant scan + Incremental scan (includes Personal workspaces)
# Auth: Delegated Fabric Admin (default) or Service Principal

import os
import json
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from pathlib import Path

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load .env file for local execution
try:
    from dotenv import load_dotenv
    load_dotenv()  # Load environment variables from .env file
except ImportError:
    pass  # python-dotenv not installed, skip

# Progress bars
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    tqdm = None

# Config file support (YAML)
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    yaml = None

# Try to import Spark dependencies (Fabric environment)
try:
    from pyspark.sql import Row, SparkSession
    import pyspark.sql.functions as F
    SPARK_AVAILABLE = True
    try:
        spark = SparkSession.builder.getOrCreate()
    except:
        SPARK_AVAILABLE = False
        spark = None
except ImportError:
    SPARK_AVAILABLE = False
    spark = None
    Row = None
    F = None

# Try to import Fabric utilities
try:
    from notebookutils import mssparkutils
except ImportError:
    try:
        import mssparkutils
    except ImportError:
        mssparkutils = None

# Import pandas for local execution
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

# Detect execution environment
RUNNING_IN_FABRIC = mssparkutils is not None and SPARK_AVAILABLE

# --- Configuration ---
# Configuration File (optional)
CONFIG_FILE = os.getenv("SCANNER_CONFIG_FILE", "scanner_config.yaml")  # Path to config file

# Authentication Mode:
#   "spn"         -> Service Principal (app registration with client secret)
#   "delegated"   -> Delegated/User auth in Fabric (uses mssparkutils)
#   "interactive" -> Interactive user login (Azure CLI or browser)
AUTH_MODE = "interactive"  # Change to "spn" for service principal

# --- Checkpoint/Resume Configuration ---
ENABLE_CHECKPOINTING = True  # Set to True to enable checkpoint/resume for large scans
CHECKPOINT_STORAGE = "json"  # "json" for local file, "lakehouse" for Fabric lakehouse storage
CHECKPOINT_INTERVAL = 100  # Save checkpoint every N batches
CHECKPOINT_DIR = "checkpoints"  # Directory for checkpoint files (local or lakehouse path)

DEBUG_MODE = False     # Set to True for detailed JSON structure logging
JSON_SINGLE_FILE_MODE = False  # Set to True to process only one specific JSON file
JSON_TARGET_FILE = "Files/scanner/raw/scan_result_20241208.json"  # Target file when JSON_SINGLE_FILE_MODE is True

# --- Local Execution: Upload to Lakehouse (Optional) ---
UPLOAD_TO_LAKEHOUSE = False  # Set to True to upload results to Fabric Lakehouse when running locally
LAKEHOUSE_WORKSPACE_ID = os.getenv("LAKEHOUSE_WORKSPACE_ID", "")  # Workspace ID containing the lakehouse
LAKEHOUSE_ID = os.getenv("LAKEHOUSE_ID", "")  # Lakehouse ID to upload to
LAKEHOUSE_UPLOAD_PATH = "Files/scanner"  # Path within lakehouse to upload files

# --- Activity Event Analysis (Inbound Connection Detection) ---
ENABLE_ACTIVITY_ANALYSIS = False  # Set to True to analyze inbound connections via Activity Event API
ACTIVITY_DAYS_BACK = 30  # Number of days of activity events to analyze

# --- Service Principal secrets (override or use env/Key Vault) ---
TENANT_ID      = os.getenv("FABRIC_SP_TENANT_ID", "<YOUR_TENANT_ID>")
CLIENT_ID      = os.getenv("FABRIC_SP_CLIENT_ID", "<YOUR_APP_CLIENT_ID>")
CLIENT_SECRET  = os.getenv("FABRIC_SP_CLIENT_SECRET", "<YOUR_APP_CLIENT_SECRET>")

AUTH_URL       = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
FABRIC_SCOPE   = "https://analysis.windows.net/powerbi/api/.default"
PBI_ADMIN_BASE = "https://api.powerbi.com/v1.0/myorg/admin"

BATCH_SIZE_WORKSPACES  = 100  # Max allowed by Scanner API

# MAX_PARALLEL_SCANS: Controls how many batches scan simultaneously
# API Call Implications:
#   Each batch uses ~8 API calls (1 post + 6 polls + 1 read)
#   Total API calls/hour ≈ (batches/hour) × 8
#
# 🛡️ AUTOMATIC 429 HANDLING:
#   If rate limit (429) is hit, scans automatically:
#   - Pause and cool down (typically 60 seconds)
#   - Retry the failed operation (up to 5 times)
#   - Continue scanning after cooldown
#   No manual intervention needed!
#
# Performance vs API Usage (500 API calls/hour tenant limit):
#   Value | Batches/Hr | Workspaces/Hr | API Calls/Hr | % of Limit | Shared Tenant?
#   ------|------------|---------------|--------------|------------|---------------
#     1   |    ~15     |    ~1,500     |    ~120      |    24%     | ✅ Yes (leaves 76%) - DEFAULT
#     3   |    ~45     |    ~4,500     |    ~360      |    72%     | ✅ Yes (leaves 28%)
#     5   |    ~52     |    ~5,200     |    ~416      |    83%     | ⚠️  Aggressive (17% left)
#     8   |    ~60     |    ~6,000     |    ~480      |    96%     | ❌ No (only 4% for others!)
#
# SHARED TENANT GUIDANCE:
#   - Default (1): Leaves ~380 API calls/hour (76%) for other users/processes - ULTRA-SAFE
#   - If health check shows 'clear', increase to 3 (leaves 28%)
#   - For off-hours/weekend scans, increase to 5-8 for faster completion
#   - Coordinate with tenant admins before running large scans
#   - If you hit 429 errors, the scan will auto-pause and retry (no data loss)
#
# MULTIPLE SCANNER API PROCESSES (quota sharing):
#   If N processes are using Scanner API, divide 500 calls/hour by N:
#   
#   Processes | Calls Each | Batches/Hr | MAX_PARALLEL_SCANS | Workspaces/Hr
#   ----------|------------|------------|-------------------|---------------
#      1      |    500     |    ~62     |        5-8        |   ~5,200-6,200
#      2      |    250     |    ~31     |        3          |   ~3,100
#      3      |    167     |    ~21     |        2          |   ~2,100
#      4      |    125     |    ~16     |        1-2        |   ~1,600
#
#   Formula: MAX_PARALLEL_SCANS ≈ (500 ÷ N ÷ 8) ÷ 1.2
#   Where N = number of concurrent Scanner API processes
#
#   Example: 3 teams running scans → 500÷3÷8÷1.2 ≈ 1.7 → Use MAX_PARALLEL_SCANS=2
#
# Why parallel scans don't multiply API calls linearly:
#   - Polling overlaps across batches (polls happen every 20 seconds)
#   - Scanner API processes batches in ~2-3 minutes
#   - Multiple batches polling simultaneously share the hourly window
#
# For 247k workspaces (2,470 batches @ 100 workspaces each):
#   MAX_PARALLEL_SCANS=1  → ~165 hours (~7 days) for full scan (ULTRA-SAFE - 20% quota usage) - DEFAULT
#   MAX_PARALLEL_SCANS=3  → ~55 hours (~2.3 days) (72% quota, good for shared tenants)
#   MAX_PARALLEL_SCANS=5  → ~47 hours (~2 days) (83% quota)
#   MAX_PARALLEL_SCANS=8  → ~41 hours (~1.7 days) (96% quota - only if you're the ONLY Scanner API user!)
#
# 💡 RECOMMENDATION FOR SHARED TENANT (247k workspaces):
#    Use MAX_PARALLEL_SCANS=1 (default) for maximum consideration to other users
#    Leaves ~400 API calls/hour (80%) for other users/processes
#    Enable hash optimization for incremental scans to maximize efficiency
#
# ⚡ PRO TIP: Run health check before large scans:
#    health = check_scanner_api_health()
#    if health['safe_to_proceed'] and health['status'] == 'clear':
#        MAX_PARALLEL_SCANS = 3  # Can safely increase if no contention
MAX_PARALLEL_SCANS     = 1  # Ultra-conservative for 247k workspace shared tenant (20% quota)

POLL_INTERVAL_SECONDS  = 20   # Scans typically complete in 2-3 minutes
                               # Lower = faster detection but more API calls
                               # 15 sec = +33% API calls, 30 sec = -25% API calls
SCAN_TIMEOUT_MINUTES   = 30

# Paths - adapt based on environment
if RUNNING_IN_FABRIC:
    # Spark-relative paths (no lakehouse:// prefix needed for Spark operations)
    RAW_DIR     = "Files/scanner/raw"
    CURATED_DIR = "Tables/dbo"
else:
    # Local filesystem paths
    BASE_DIR = Path(os.getcwd()) / "scanner_output"
    RAW_DIR = str(BASE_DIR / "raw")
    CURATED_DIR = str(BASE_DIR / "curated")
    # Create directories if they don't exist
    Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
    Path(CURATED_DIR).mkdir(parents=True, exist_ok=True)

# Helper function to convert Spark paths to mssparkutils paths
def _to_lakehouse_path(spark_path: str) -> str:
    """Convert Spark-relative path to mssparkutils lakehouse URI format."""
    if spark_path.startswith(("file:", "abfss:", "lakehouse:")):
        return spark_path
    # For Files/ paths, use file: prefix for mssparkutils
    if spark_path.startswith("Files/"):
        return f"file:/lakehouse/default/{spark_path}"
    # For absolute paths starting with /lakehouse/
    if spark_path.startswith("/lakehouse/"):
        return f"file:{spark_path}"
    # For Tables/ paths, they're managed tables and don't need filesystem operations
    return f"file:/lakehouse/default/{spark_path}"

if RUNNING_IN_FABRIC and mssparkutils is not None:
    for path in [RAW_DIR]:  # Only create Files/ directories, Tables are managed by Spark
        try:
            lakehouse_path = _to_lakehouse_path(path)
            mssparkutils.fs.mkdirs(lakehouse_path)
        except Exception:
            pass

CLOUD_CONNECTORS = {
    "azuresqldatabase", "sqlserverless", "synapse", "kusto",
    "onelake", "adls", "abfss", "s3", "rest",
    "sharepointonline", "dynamics365", "salesforce", "snowflake",
    "fabriclakehouse"
}

# --- API Call Tracking (for quota monitoring) ---
API_CALL_COUNTER = {
    'count': 0,
    'start_time': None,
    'last_reset': None
}

def _track_api_call():
    """Internal: Track API calls for quota monitoring."""
    global API_CALL_COUNTER
    if API_CALL_COUNTER['start_time'] is None:
        API_CALL_COUNTER['start_time'] = time.time()
        API_CALL_COUNTER['last_reset'] = time.time()
    
    API_CALL_COUNTER['count'] += 1
    
    # Auto-reset counter every hour
    elapsed = time.time() - API_CALL_COUNTER['last_reset']
    if elapsed >= 3600:
        API_CALL_COUNTER['count'] = 1
        API_CALL_COUNTER['last_reset'] = time.time()

def get_api_call_stats() -> dict:
    """Get current API call statistics."""
    if API_CALL_COUNTER['start_time'] is None:
        return {'calls': 0, 'elapsed_minutes': 0, 'rate_per_hour': 0}
    
    elapsed = time.time() - API_CALL_COUNTER['start_time']
    elapsed_minutes = elapsed / 60
    rate_per_hour = (API_CALL_COUNTER['count'] / elapsed) * 3600 if elapsed > 0 else 0
    
    return {
        'calls': API_CALL_COUNTER['count'],
        'elapsed_minutes': elapsed_minutes,
        'rate_per_hour': rate_per_hour,
        'percentage_used': (rate_per_hour / 500) * 100 if rate_per_hour > 0 else 0
    }

def print_api_call_stats():
    """Print current API usage statistics."""
    stats = get_api_call_stats()
    if stats['calls'] == 0:
        print("\n📊 API Usage: No calls tracked yet")
        return
    
    print(f"\n📊 API Usage Statistics:")
    print(f"   Calls made: {stats['calls']}")
    print(f"   Elapsed time: {stats['elapsed_minutes']:.1f} minutes")
    print(f"   Projected rate: {stats['rate_per_hour']:.0f} calls/hour ({stats['percentage_used']:.1f}% of 500/hour tenant-wide limit)")
    
    if stats['rate_per_hour'] > 450:
        print(f"   ⚠️  WARNING: Approaching rate limit! Longer lookback periods may be throttled.")
        print(f"   💡 TIP: Use shorter incremental windows (--hours 3) instead of longer periods (--hours 6+)")
    elif stats['rate_per_hour'] > 350:
        print(f"   ⚠️  Moderate usage - {500 - stats['rate_per_hour']:.0f} calls/hour capacity remaining")
        print(f"   💡 Consider running during off-peak hours if scanning frequently")
    else:
        print(f"   ✅ Healthy rate - {500 - stats['rate_per_hour']:.0f} calls/hour available for other users/processes")

# --- Configuration File Loading ---

def load_config_file(config_path: str = None) -> dict:
    """
    Load configuration from YAML or JSON file.
    
    Args:
        config_path: Path to config file (defaults to CONFIG_FILE global)
    
    Returns:
        Dictionary of configuration settings
    """
    if config_path is None:
        config_path = CONFIG_FILE
    
    if not os.path.exists(config_path):
        print(f"ℹ️  Config file not found: {config_path} (using defaults)")
        return {}
    
    try:
        with open(config_path, 'r') as f:
            if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                if not YAML_AVAILABLE:
                    print(f"⚠️  PyYAML not installed. Install with: pip install pyyaml")
                    return {}
                config = yaml.safe_load(f)
            elif config_path.endswith('.json'):
                config = json.load(f)
            else:
                print(f"⚠️  Unsupported config file format: {config_path}")
                return {}
        
        print(f"✅ Loaded configuration from: {config_path}")
        return config or {}
    except Exception as e:
        print(f"⚠️  Error loading config file: {e}")
        return {}

def apply_config(config: dict):
    """
    Apply configuration settings to global variables.
    
    Args:
        config: Configuration dictionary
    """
    global MAX_PARALLEL_SCANS, POLL_INTERVAL_SECONDS, SCAN_TIMEOUT_MINUTES
    global ENABLE_CHECKPOINTING, CHECKPOINT_STORAGE, CHECKPOINT_INTERVAL
    global AUTH_MODE, DEBUG_MODE
    global UPLOAD_TO_LAKEHOUSE, LAKEHOUSE_WORKSPACE_ID, LAKEHOUSE_ID, LAKEHOUSE_UPLOAD_PATH
    
    # API settings
    if 'api' in config:
        api_config = config['api']
        if 'max_parallel_scans' in api_config:
            MAX_PARALLEL_SCANS = api_config['max_parallel_scans']
        if 'poll_interval_seconds' in api_config:
            POLL_INTERVAL_SECONDS = api_config['poll_interval_seconds']
        if 'scan_timeout_minutes' in api_config:
            SCAN_TIMEOUT_MINUTES = api_config['scan_timeout_minutes']
    
    # Checkpoint settings
    if 'checkpoint' in config:
        cp_config = config['checkpoint']
        if 'enabled' in cp_config:
            ENABLE_CHECKPOINTING = cp_config['enabled']
        if 'storage' in cp_config:
            CHECKPOINT_STORAGE = cp_config['storage']
        if 'interval' in cp_config:
            CHECKPOINT_INTERVAL = cp_config['interval']
    
    # Auth settings
    if 'auth' in config:
        auth_config = config['auth']
        if 'mode' in auth_config:
            AUTH_MODE = auth_config['mode']
    
    # Lakehouse upload settings (for local execution)
    if 'lakehouse' in config:
        lh_config = config['lakehouse']
        if 'upload_enabled' in lh_config:
            UPLOAD_TO_LAKEHOUSE = lh_config['upload_enabled']
        if 'workspace_id' in lh_config:
            LAKEHOUSE_WORKSPACE_ID = lh_config['workspace_id']
        if 'lakehouse_id' in lh_config:
            LAKEHOUSE_ID = lh_config['lakehouse_id']
        if 'upload_path' in lh_config:
            LAKEHOUSE_UPLOAD_PATH = lh_config['upload_path']
    
    # Debug settings
    if 'debug' in config:
        DEBUG_MODE = config.get('debug', False)

# --- Checkpoint/Resume Management ---

class CheckpointManager:
    """Manages checkpoint/resume state for long-running scans."""
    
    def __init__(self, checkpoint_id: str, storage: str = "json", checkpoint_dir: str = CHECKPOINT_DIR):
        """
        Initialize checkpoint manager.
        
        Args:
            checkpoint_id: Unique identifier for this scan (e.g., 'full_scan_20260116')
            storage: 'json' for local file or 'lakehouse' for Fabric storage
            checkpoint_dir: Directory for checkpoint files
        """
        self.checkpoint_id = checkpoint_id
        self.storage = storage
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_file = f"{checkpoint_id}_checkpoint.json"
        
        # Create checkpoint directory if it doesn't exist (local only)
        if storage == "json" and not RUNNING_IN_FABRIC:
            Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
    
    def save_checkpoint(self, state: dict):
        """
        Save checkpoint state.
        
        Args:
            state: Dictionary containing scan state (completed_batch_indices, total_batches, etc.)
        """
        state['last_checkpoint_time'] = datetime.now(timezone.utc).isoformat()
        
        checkpoint_data = json.dumps(state, indent=2)
        
        if self.storage == "lakehouse" and RUNNING_IN_FABRIC and mssparkutils is not None:
            try:
                checkpoint_path = f"{_to_lakehouse_path(self.checkpoint_dir)}/{self.checkpoint_file}"
                mssparkutils.fs.put(checkpoint_path, checkpoint_data, overwrite=True)
                print(f"💾 Checkpoint saved to lakehouse: {checkpoint_path}")
            except Exception as e:
                print(f"⚠️  Failed to save checkpoint to lakehouse: {e}")
                # Fallback to local storage
                self._save_local_checkpoint(checkpoint_data)
        else:
            self._save_local_checkpoint(checkpoint_data)
    
    def _save_local_checkpoint(self, checkpoint_data: str):
        """Save checkpoint to local file."""
        checkpoint_path = Path(self.checkpoint_dir) / self.checkpoint_file
        try:
            with open(checkpoint_path, 'w') as f:
                f.write(checkpoint_data)
            print(f"💾 Checkpoint saved to: {checkpoint_path}")
        except Exception as e:
            print(f"⚠️  Failed to save checkpoint: {e}")
    
    def load_checkpoint(self) -> Optional[dict]:
        """
        Load checkpoint state if it exists.
        
        Returns:
            Dictionary containing saved state, or None if no checkpoint exists
        """
        if self.storage == "lakehouse" and RUNNING_IN_FABRIC and mssparkutils is not None:
            try:
                checkpoint_path = f"{_to_lakehouse_path(self.checkpoint_dir)}/{self.checkpoint_file}"
                checkpoint_data = mssparkutils.fs.head(checkpoint_path, 1000000)  # Read up to 1MB
                state = json.loads(checkpoint_data)
                print(f"📂 Loaded checkpoint from lakehouse: {checkpoint_path}")
                return state
            except Exception:
                # Try local fallback
                return self._load_local_checkpoint()
        else:
            return self._load_local_checkpoint()
    
    def _load_local_checkpoint(self) -> Optional[dict]:
        """Load checkpoint from local file."""
        checkpoint_path = Path(self.checkpoint_dir) / self.checkpoint_file
        if checkpoint_path.exists():
            try:
                with open(checkpoint_path, 'r') as f:
                    state = json.load(f)
                print(f"📂 Loaded checkpoint from: {checkpoint_path}")
                return state
            except Exception as e:
                print(f"⚠️  Failed to load checkpoint: {e}")
        return None
    
    def clear_checkpoint(self):
        """Delete checkpoint file after successful completion."""
        if self.storage == "lakehouse" and RUNNING_IN_FABRIC and mssparkutils is not None:
            try:
                checkpoint_path = f"{_to_lakehouse_path(self.checkpoint_dir)}/{self.checkpoint_file}"
                mssparkutils.fs.rm(checkpoint_path)
                print(f"🗑️  Checkpoint cleared: {checkpoint_path}")
            except Exception:
                pass
        else:
            checkpoint_path = Path(self.checkpoint_dir) / self.checkpoint_file
            if checkpoint_path.exists():
                try:
                    checkpoint_path.unlink()
                    print(f"🗑️  Checkpoint cleared: {checkpoint_path}")
                except Exception as e:
                    print(f"⚠️  Failed to clear checkpoint: {e}")

# --- Auth ---
def get_access_token_spn() -> str:
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": FABRIC_SCOPE,
        "grant_type": "client_credentials",
    }
    r = requests.post(AUTH_URL, data=data)
    r.raise_for_status()
    return r.json().get("access_token")


# Cache for created lakehouse directories to avoid redundant API calls
_created_lakehouse_dirs = set()

def ensure_lakehouse_directory(directory_path: str, workspace_id: str, lakehouse_id: str) -> bool:
    """
    Ensure a directory exists in the Fabric lakehouse by uploading a placeholder file.
    
    Args:
        directory_path: Directory path in lakehouse (e.g., 'Files/scanner/raw/incremental')
        workspace_id: Fabric workspace ID
        lakehouse_id: Lakehouse ID
    
    Returns:
        True if directory was created or already exists, False on error
    """
    # Check cache first to avoid redundant API calls
    cache_key = f"{workspace_id}/{lakehouse_id}/{directory_path}"
    if cache_key in _created_lakehouse_dirs:
        return True
    
    try:
        # Upload a tiny placeholder file to create the directory structure
        # Fabric automatically creates parent directories when uploading files
        placeholder_path = f"{directory_path}/.placeholder"
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/files/{placeholder_path}"
        
        headers = {
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Content-Type": "application/octet-stream"
        }
        
        # Upload empty file to create directory
        response = requests.put(url, headers=headers, data=b"")
        
        if DEBUG_MODE:
            print(f"   [DEBUG] Directory creation: {directory_path}")
            print(f"   [DEBUG] Response: {response.status_code}")
            if response.status_code not in [200, 201, 409]:
                print(f"   [DEBUG] Error: {response.text}")
        
        if response.status_code in [200, 201]:
            _created_lakehouse_dirs.add(cache_key)
            return True
        elif response.status_code == 409:  # Conflict - directory already exists
            _created_lakehouse_dirs.add(cache_key)
            return True
        else:
            if DEBUG_MODE:
                print(f"   Directory creation response ({response.status_code}): {response.text}")
            return False
            
    except Exception as e:
        if DEBUG_MODE:
            print(f"   Directory creation error: {e}")
        return False


def upload_to_fabric_lakehouse(local_file_path: str, lakehouse_path: str, workspace_id: str, lakehouse_id: str) -> bool:
    """
    Upload a file from local filesystem to Fabric Lakehouse using REST API.
    
    Args:
        local_file_path: Local file path to upload
        lakehouse_path: Path within lakehouse (e.g., 'Files/scanner/raw/file.json')
        workspace_id: Fabric workspace ID containing the lakehouse
        lakehouse_id: Lakehouse ID
    
    Returns:
        True if upload succeeded, False otherwise
    """
    if not workspace_id or not lakehouse_id:
        print(f"⚠️  Skipping upload: workspace_id or lakehouse_id not configured")
        return False
    
    try:
        # Ensure parent directory exists
        path_parts = lakehouse_path.rsplit('/', 1)
        if len(path_parts) > 1:
            directory_path = path_parts[0]
            ensure_lakehouse_directory(directory_path, workspace_id, lakehouse_id)
        
        # Read file content
        with open(local_file_path, 'rb') as f:
            file_content = f.read()
        
        # Fabric Files API endpoint
        # Format: /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}/files/{path}
        # Note: The API should auto-create parent directories, but if it fails with 404,
        # we'll retry with explicit path parameter
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/files/{lakehouse_path}"
        
        headers = {
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Content-Type": "application/octet-stream"
        }
        
        # PUT request to upload/overwrite file
        response = requests.put(url, headers=headers, data=file_content)
        
        if response.status_code in [200, 201]:
            print(f"✅ Uploaded to lakehouse: {lakehouse_path}")
            return True
        else:
            print(f"⚠️  Upload failed ({response.status_code}): {lakehouse_path}")
            if DEBUG_MODE:
                print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"⚠️  Upload error for {lakehouse_path}: {e}")
        return False

def get_access_token_interactive() -> str:
    """
    Get access token using interactive user authentication.
    Tries Azure CLI first, falls back to browser-based login.
    """
    try:
        # Try importing azure-identity (install with: pip install azure-identity)
        from azure.identity import AzureCliCredential, InteractiveBrowserCredential
        
        # First try Azure CLI (if user is already logged in via 'az login')
        try:
            print("Attempting authentication via Azure CLI...")
            credential = AzureCliCredential()
            token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            print("✅ Authenticated via Azure CLI")
            return token.token
        except Exception as cli_error:
            print(f"Azure CLI not available: {cli_error}")
            print("Falling back to interactive browser login...")
            
            # Fall back to browser-based interactive login
            credential = InteractiveBrowserCredential(
                tenant_id=TENANT_ID if TENANT_ID != "<YOUR_TENANT_ID>" else None
            )
            token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            print("✅ Authenticated via browser login")
            return token.token
            
    except ImportError:
        print("ERROR: azure-identity library not found.")
        print("Install it with: pip install azure-identity")
        print("Then re-run the script.")
        raise


HEADERS = None
ACCESS_TOKEN = None

def initialize_authentication():
    """Initialize authentication based on AUTH_MODE. Call this before making API requests."""
    global HEADERS, ACCESS_TOKEN, AUTH_MODE
    
    if AUTH_MODE == "delegated":
        if not RUNNING_IN_FABRIC:
            print("WARNING: Delegated auth requires Fabric environment. Switching to interactive mode.")
            AUTH_MODE = "interactive"
        else:
            print("Using Fabric delegated authentication...")
            ACCESS_TOKEN = mssparkutils.credentials.getToken("powerbi")
            HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
            return

    if AUTH_MODE == "interactive":
        print("Using interactive user authentication...")
        ACCESS_TOKEN = get_access_token_interactive()
        HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}

    elif AUTH_MODE == "spn":
        print(f"Using Service Principal authentication (Tenant: {TENANT_ID[:8]}...)")
        ACCESS_TOKEN = get_access_token_spn()
        HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}

    if HEADERS is None:
        raise RuntimeError(f"Invalid AUTH_MODE: {AUTH_MODE}. Use 'spn', 'delegated', or 'interactive'")

# --- Shared Rate Limiter for Parallel Scanning ---

class SharedRateLimiter:
    """
    Thread-safe rate limiter for distributing API quota across parallel capacity scans.
    
    Ensures that parallel workers respect the global API rate limit by coordinating
    API call allocations across threads.
    """
    
    def __init__(self, max_calls_per_hour: int = 450, max_parallel_workers: int = 3):
        """
        Args:
            max_calls_per_hour: Total API calls allowed per hour (default 450 for safety margin)
            max_parallel_workers: Maximum number of parallel capacity scans
        """
        self.max_calls_per_hour = max_calls_per_hour
        self.max_parallel_workers = max_parallel_workers
        self.calls_per_worker = max_calls_per_hour // max_parallel_workers
        
        # Thread-safe tracking
        self._lock = threading.Lock()
        self._worker_quotas = {}  # worker_id -> remaining quota
        self._total_calls_made = 0
        self._start_time = time.time()
        
        print(f"\n📊 SharedRateLimiter initialized:")
        print(f"   Total quota: {max_calls_per_hour} calls/hour")
        print(f"   Parallel workers: {max_parallel_workers}")
        print(f"   Per-worker quota: {self.calls_per_worker} calls/hour")
    
    def allocate_worker(self, worker_id: str) -> int:
        """Allocate quota to a new worker."""
        with self._lock:
            if worker_id not in self._worker_quotas:
                self._worker_quotas[worker_id] = self.calls_per_worker
            return self._worker_quotas[worker_id]
    
    def acquire(self, worker_id: str, count: int = 1) -> bool:
        """
        Request permission to make API calls.
        
        Args:
            worker_id: Unique identifier for the worker
            count: Number of API calls to make
        
        Returns:
            True if quota available, False if limit reached
        """
        with self._lock:
            # Allocate worker if first time
            if worker_id not in self._worker_quotas:
                self.allocate_worker(worker_id)
            
            # Check if worker has quota
            if self._worker_quotas[worker_id] >= count:
                self._worker_quotas[worker_id] -= count
                self._total_calls_made += count
                return True
            else:
                return False
    
    def release(self, worker_id: str, count: int = 1):
        """Return unused quota (e.g., if call failed)."""
        with self._lock:
            if worker_id in self._worker_quotas:
                self._worker_quotas[worker_id] += count
                self._total_calls_made -= count
    
    def get_stats(self) -> dict:
        """Get current rate limiter statistics."""
        with self._lock:
            elapsed_hours = (time.time() - self._start_time) / 3600
            return {
                'total_calls_made': self._total_calls_made,
                'remaining_quota': self.max_calls_per_hour - self._total_calls_made,
                'elapsed_hours': elapsed_hours,
                'calls_per_hour_rate': self._total_calls_made / elapsed_hours if elapsed_hours > 0 else 0,
                'worker_quotas': dict(self._worker_quotas)
            }
    
    def wait_if_needed(self, worker_id: str) -> bool:
        """
        Check if worker should wait due to quota exhaustion.
        
        Returns:
            True if worker should continue, False if quota exhausted
        """
        with self._lock:
            if worker_id in self._worker_quotas:
                return self._worker_quotas[worker_id] > 0
            return True

# --- Connection Hash Tracker for Optimization ---

class ConnectionHashTracker:
    """
    Tracks connection hashes to detect changes in incremental scans.
    
    Calculates and stores SHA256 hashes of workspace connections to enable
    smart filtering during incremental scans (80-90% reduction in API calls).
    """
    
    def __init__(self, config, running_in_fabric: bool = False):
        """
        Initialize hash tracker.
        
        Args:
            config: Configuration object with curated_dir, tenant_id, etc.
            running_in_fabric: Whether running in Fabric environment
        """
        self.config = config
        self.running_in_fabric = running_in_fabric
        self.hash_table = "workspace_connection_hashes"
        
    def calculate_workspace_hash(self, connections: list) -> str:
        """
        Calculate SHA256 hash of workspace connections.
        
        Args:
            connections: List of connection dictionaries
            
        Returns:
            64-character hex string (SHA256 hash)
        """
        import hashlib
        import json
        
        if not connections:
            return hashlib.sha256(b"").hexdigest()
        
        # Sort connections for deterministic hashing
        sorted_connections = sorted(
            connections,
            key=lambda c: f"{c.get('connector', '')}_{c.get('server', '')}_{c.get('database', '')}"
        )
        
        # Create normalized representation
        hash_data = []
        for conn in sorted_connections:
            hash_data.append({
                'connector': conn.get('connector', ''),
                'server': conn.get('server', ''),
                'database': conn.get('database', ''),
                'endpoint': conn.get('endpoint', '')
            })
        
        # Calculate hash
        json_str = json.dumps(hash_data, sort_keys=True)
        return hashlib.sha256(json_str.encode('utf-8')).hexdigest()
    
    def calculate_workspace_hashes(self, workspace_connections: dict) -> dict:
        """
        Calculate hashes for multiple workspaces.
        
        Args:
            workspace_connections: Dict of {workspace_id: [connections]}
            
        Returns:
            Dict of {workspace_id: hash_value}
        """
        return {
            ws_id: self.calculate_workspace_hash(connections)
            for ws_id, connections in workspace_connections.items()
        }
    
    def get_stored_hashes(self) -> dict:
        """
        Load stored hashes from previous scans.
        
        Returns:
            Dict of {workspace_id: {'hash': hash_value, 'last_scan_time': timestamp}}
        """
        try:
            from pathlib import Path
            from datetime import datetime
            
            if self.running_in_fabric and SPARK_AVAILABLE:
                # Read from lakehouse table
                try:
                    spark = SparkSession.builder.getOrCreate()
                    df = spark.table(self.hash_table)
                    
                    stored_hashes = {}
                    for row in df.collect():
                        stored_hashes[row.workspace_id] = {
                            'hash': row.connection_hash,
                            'last_scan_time': row.last_scan_time
                        }
                    return stored_hashes
                except:
                    return {}
            else:
                # Read from local file (pandas)
                import pandas as pd
                output_path = Path(self.config.curated_dir)
                hash_file = output_path / f"{self.hash_table}.parquet"
                
                if hash_file.exists():
                    df = pd.read_parquet(hash_file)
                    stored_hashes = {}
                    for _, row in df.iterrows():
                        stored_hashes[row['workspace_id']] = {
                            'hash': row['connection_hash'],
                            'last_scan_time': row['last_scan_time']
                        }
                    return stored_hashes
                return {}
        except Exception as e:
            print(f"   Warning: Could not load stored hashes: {e}")
            return {}
    
    def save_hashes(self, workspace_hashes: dict, workspace_metadata: dict) -> None:
        """
        Save workspace connection hashes for future scans.
        
        Args:
            workspace_hashes: Dict of {workspace_id: hash_value}
            workspace_metadata: Dict of {workspace_id: {'name': ..., 'type': ...}}
        """
        try:
            from datetime import datetime, timezone
            from pathlib import Path
            
            # Prepare data
            now = datetime.now(timezone.utc).isoformat()
            hash_records = []
            
            for ws_id, hash_value in workspace_hashes.items():
                metadata = workspace_metadata.get(ws_id, {})
                hash_records.append({
                    'workspace_id': ws_id,
                    'workspace_name': metadata.get('name', ''),
                    'workspace_type': metadata.get('type', ''),
                    'connection_hash': hash_value,
                    'last_scan_time': now
                })
            
            if self.running_in_fabric and SPARK_AVAILABLE:
                # Save to lakehouse table
                spark = SparkSession.builder.getOrCreate()
                df_new = spark.createDataFrame(hash_records)
                
                # Merge with existing
                try:
                    df_existing = spark.table(self.hash_table)
                    df_merged = df_existing.alias("old").join(
                        df_new.alias("new"),
                        on="workspace_id",
                        how="full_outer"
                    ).selectExpr(
                        "coalesce(new.workspace_id, old.workspace_id) as workspace_id",
                        "coalesce(new.workspace_name, old.workspace_name) as workspace_name",
                        "coalesce(new.workspace_type, old.workspace_type) as workspace_type",
                        "coalesce(new.connection_hash, old.connection_hash) as connection_hash",
                        "coalesce(new.last_scan_time, old.last_scan_time) as last_scan_time"
                    )
                    df_merged.write.mode("overwrite").saveAsTable(self.hash_table)
                except:
                    # Table doesn't exist yet
                    df_new.write.mode("overwrite").saveAsTable(self.hash_table)
            else:
                # Save to local file (pandas)
                import pandas as pd
                output_path = Path(self.config.curated_dir)
                output_path.mkdir(parents=True, exist_ok=True)
                hash_file = output_path / f"{self.hash_table}.parquet"
                
                df_new = pd.DataFrame(hash_records)
                
                # Merge with existing
                if hash_file.exists():
                    df_existing = pd.read_parquet(hash_file)
                    df_merged = pd.concat([df_existing, df_new]).drop_duplicates(
                        subset=['workspace_id'],
                        keep='last'
                    )
                    df_merged.to_parquet(hash_file, index=False)
                else:
                    df_new.to_parquet(hash_file, index=False)
                    
        except Exception as e:
            print(f"   Warning: Could not save hashes: {e}")

# --- Scanner API helpers ---


def get_all_workspaces(include_personal: bool = True) -> List[Dict[str, Any]]:
    url = f"{PBI_ADMIN_BASE}/workspaces/modified"
    params = {"excludePersonalWorkspaces": str(not include_personal).lower()}
    _track_api_call()  # Track API usage
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    payload = r.json() or {}
    
    # Handle different response structures
    if isinstance(payload, list):
        workspaces = payload
    elif isinstance(payload, dict):
        workspaces = payload.get("workspaces", [])
    else:
        workspaces = []
    
    # Ensure all items are dicts
    return [ws for ws in workspaces if isinstance(ws, dict)]


def modified_workspace_ids(modified_since_iso: str, include_personal: bool = True) -> List[Dict[str, Any]]:
    url = f"{PBI_ADMIN_BASE}/workspaces/modified"
    params = {
        "modifiedSince": modified_since_iso,
        "excludePersonalWorkspaces": str(not include_personal).lower(),
    }
    _track_api_call()  # Track API usage
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    payload = r.json() or {}
    
    # Handle different response structures
    if isinstance(payload, list):
        workspaces = payload
    elif isinstance(payload, dict):
        workspaces = payload.get("workspaces", [])
    else:
        workspaces = []
    
    # Ensure all items are dicts
    return [ws for ws in workspaces if isinstance(ws, dict)]


def post_workspace_info(workspace_ids: List[str], max_retries: int = 3) -> str:
    if not workspace_ids:
        raise ValueError("workspace_ids cannot be empty.")
    url = f"{PBI_ADMIN_BASE}/workspaces/getInfo"
    body = {
        "workspaces": workspace_ids,
        "lineage": True,
        "users": True
    }
    
    for attempt in range(max_retries):
        try:
            _track_api_call()  # Track API usage
            r = requests.post(url, headers=HEADERS, json=body)
            r.raise_for_status()
            
            response_data = r.json() or {}
            
            # Handle different response structures
            # Per Microsoft docs: response is {"id": "uuid", "createdDateTime": "...", "status": "..."}
            if isinstance(response_data, dict):
                scan_id = response_data.get("id") or response_data.get("scanId")  # Check "id" first (official field name)
            elif isinstance(response_data, str):
                scan_id = response_data
            else:
                scan_id = None
            
            if not scan_id:
                if DEBUG_MODE:
                    print(f"DEBUG: getInfo response type: {type(response_data)}")
                    print(f"DEBUG: getInfo response content: {response_data}")
                raise RuntimeError(f"No scan ID returned by getInfo. Response: {response_data}")
            return scan_id
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limit exceeded
                retry_after = int(e.response.headers.get('Retry-After', 60))  # Default to 60 seconds
                print(f"⚠️ Rate limit exceeded (429). Waiting {retry_after} seconds before retry {attempt + 1}/{max_retries}...")
                if attempt < max_retries - 1:
                    time.sleep(retry_after)
                else:
                    print(f"❌ Rate limit exceeded. Maximum retries reached. Please wait at least 1 hour before trying again.")
                    print(f"   API Limits: 500 requests/hour, 16 simultaneous requests")
                    raise
            else:
                raise


def poll_scan_status(scan_id: str) -> None:
    url = f"{PBI_ADMIN_BASE}/workspaces/scanStatus/{scan_id}"
    start = time.time()
    while True:
        try:
            _track_api_call()  # Track API usage
            r = requests.get(url, headers=HEADERS)
            
            if r.status_code == 202:
                if time.time() - start > SCAN_TIMEOUT_MINUTES * 60:
                    raise TimeoutError(f"Scan {scan_id} timed out after {SCAN_TIMEOUT_MINUTES} minutes.")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            r.raise_for_status()
            status_json = r.json() or {}
            status = status_json.get("status")
            if status == "Succeeded":
                return
            if status in {"Failed", "Cancelled"}:
                raise RuntimeError(f"Scan {scan_id} ended with status: {status}")
            if time.time() - start > SCAN_TIMEOUT_MINUTES * 60:
                raise TimeoutError(f"Scan {scan_id} timed out after {SCAN_TIMEOUT_MINUTES} minutes.")
            time.sleep(POLL_INTERVAL_SECONDS)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get('Retry-After', 60))
                print(f"⚠️  Rate limit hit while polling scan status. Cooling down for {retry_after} seconds...")
                print(f"   This won't count against the timeout. Scan will continue after cooldown.")
                time.sleep(retry_after)
                continue  # Don't count this against timeout, just retry
            raise


def read_scan_result(scan_id: str) -> Dict[str, Any]:
    url = f"{PBI_ADMIN_BASE}/workspaces/scanResult/{scan_id}"
    
    # Retry logic for 429 rate limit errors
    max_retries = 5
    base_wait = 60
    
    for attempt in range(max_retries):
        try:
            _track_api_call()  # Track API usage
            r = requests.get(url, headers=HEADERS)
            r.raise_for_status()
            
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                if attempt < max_retries - 1:
                    retry_after = int(e.response.headers.get('Retry-After', base_wait * (2 ** attempt)))
                    print(f"⚠️  Rate limit hit reading results. Cooling down for {retry_after} seconds...")
                    print(f"   Attempt {attempt + 1}/{max_retries}. Auto-retrying after cooldown...")
                    time.sleep(retry_after)
                    continue
                else:
                    print(f"❌ Rate limit exceeded after {max_retries} retries")
            raise
    return {}


def get_scan_result_by_id(
    scan_id: str,
    curated_dir: str = CURATED_DIR,
    table_name: str = "tenant_cloud_connections",
    merge_with_existing: bool = True
) -> None:
    """
    Retrieves scan result using a scan ID and processes cloud connections.
    This uses the WorkspaceInfo GetScanResult API to fetch previously completed scan results.
    
    The scan result must be from a scan that completed successfully within the last 24 hours.
    Use this function when you have a scan ID from:
    - A previous call to PostWorkspaceInfo API
    - A scan triggered by another process
    - A scan ID stored for later retrieval
    
    Args:
        scan_id: The scan ID (UUID) from a previous scan
        curated_dir: Output directory for curated parquet files
        table_name: Name of the SQL table to create/update
        merge_with_existing: If True, merge with existing data; if False, overwrite
    
    Example:
        # Get result from a scan triggered earlier today
        get_scan_result_by_id(
            scan_id="e7d03602-4873-4760-b37e-1563ef5358e3",
            merge_with_existing=True
        )
    """
    print(f"Fetching scan result for scan ID: {scan_id}")
    
    try:
        payload = read_scan_result(scan_id)
        
        if not payload or not payload.get("workspaces"):
            print(f"Warning: No workspaces found in scan result for {scan_id}")
            return
        
        print(f"Retrieved scan result with {len(payload.get('workspaces', []))} workspace(s)")
        
        # Build workspace sidecar from the scan payload
        sidecar = {}
        for ws in payload.get("workspaces", []):
            ws_id = ws.get("id")
            if not ws_id:
                continue
            
            # Extract workspace admins/owners
            users = ws.get("users") or []
            admins = [u.get("emailAddress") or u.get("identifier") 
                      for u in users if u.get("groupUserAccessRight") in {"Admin", "Member"}]
            
            sidecar[ws_id] = {
                "name": ws.get("name", ""),
                "kind": str(ws.get("type", "")).lower() or "unknown",
                "users": ", ".join(admins[:5]) if admins else None
            }
        
        payload["workspace_sidecar"] = sidecar
        
        # Save to lakehouse if available
        if mssparkutils is not None:
            try:
                raw_path = f"{_to_lakehouse_path(RAW_DIR)}/from_scan_id/{scan_id}.json"
                mssparkutils.fs.put(raw_path, json.dumps(payload))
                print(f"Saved scan result to: {raw_path}")
            except Exception as e:
                print(f"Warning: Could not save to lakehouse: {e}")
        
        # Extract connection rows
        rows = flatten_scan_payload(payload, sidecar)
        
        if not rows:
            print("No connection rows extracted from scan result")
            return
        
        print(f"Extracted {len(rows)} connection row(s)")
        
        # Create DataFrame
        df_new = spark.createDataFrame(rows)
        
        if merge_with_existing:
            try:
                df_existing = spark.read.parquet(curated_dir)
                df_combined = df_existing.union(df_new).dropDuplicates([
                    "workspace_id", "workspace_name", "artifact_type", "artifact_id",
                    "artifact_name", "datasource_type", "target"
                ])
                df_combined.write.mode("overwrite").parquet(curated_dir)
                print(f"Merged with existing data in {curated_dir}")
            except Exception:
                df_new.write.mode("overwrite").parquet(curated_dir)
                print(f"Created new parquet in {curated_dir}")
        else:
            df_new.write.mode("overwrite").parquet(curated_dir)
            print(f"Overwrote data in {curated_dir}")
        
        # Register or refresh SQL table
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(f"CREATE TABLE {table_name} USING PARQUET LOCATION '{curated_dir}'")
        print(f"Registered table: {table_name}")
        
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Error: Scan ID {scan_id} not found. The scan may have expired (>24 hours) or never existed.")
        elif e.response.status_code == 401:
            print(f"Error: Authentication failed. Ensure you have Fabric Admin permissions.")
        else:
            print(f"HTTP Error {e.response.status_code}: {e}")
        raise
    except Exception as e:
        print(f"Error processing scan result: {e}")
        raise

# --- Batch runner ---

def run_one_batch(batch_meta: List[Dict[str, Any]]) -> Dict[str, Any]:
    ids = [w.get("id") for w in batch_meta if w.get("id")]
    scan_id = post_workspace_info(ids)
    poll_scan_status(scan_id)
    payload = read_scan_result(scan_id)
    
    # Extract workspace users/owners from scan result
    ws_users_map = {}
    workspaces_data = payload.get("workspaces") if isinstance(payload, dict) else []
    if not isinstance(workspaces_data, list):
        workspaces_data = []
    
    for ws in workspaces_data:
        if not isinstance(ws, dict):
            continue
        ws_id = ws.get("id")
        users = ws.get("users") or []
        if not isinstance(users, list):
            users = []
        # Get workspace admins/owners
        admins = [u.get("emailAddress") or u.get("identifier") 
                  for u in users if isinstance(u, dict) and u.get("workspaceUserAccessRight") in {"Admin", "Member"}]
        ws_users_map[ws_id] = ", ".join(admins[:5]) if admins else None  # Limit to first 5
    
    # Build sidecar with capacity metadata from scan result
    sidecar = {}
    for ws in workspaces_data:
        if not isinstance(ws, dict):
            continue
        ws_id = ws.get("id")
        if not ws_id:
            continue
        
        # Extract capacity information from workspace
        capacity_id = ws.get("capacityId")
        is_dedicated = ws.get("isOnDedicatedCapacity", False)
        
        # Determine capacity name
        if not is_dedicated or not capacity_id:
            capacity_name = "Shared"
        else:
            # Use capacityName if available, otherwise create identifier from ID
            capacity_name = ws.get("capacityName") or f"Capacity_{capacity_id[:8]}"
        
        sidecar[ws_id] = {
            "name": ws.get("name", ""),
            "kind": (str(ws.get("type")).lower() if ws.get("type") else "unknown"),
            "users": ws_users_map.get(ws_id),
            "capacity_id": capacity_id,
            "capacity_name": capacity_name,
            "is_dedicated_capacity": is_dedicated
        }
    
    # Add metadata for workspaces from batch_meta that weren't in scan result
    for w in batch_meta:
        ws_id = w.get("id")
        if ws_id and ws_id not in sidecar:
            sidecar[ws_id] = {
                "name": w.get("name", ""),
                "kind": (str(w.get("type")).lower() if w.get("type") else "unknown"),
                "users": ws_users_map.get(ws_id),
                "capacity_id": None,
                "capacity_name": "Shared",
                "is_dedicated_capacity": False
            }
    payload["workspace_sidecar"] = sidecar
    if RUNNING_IN_FABRIC and mssparkutils is not None:
        try:
            raw_path = f"{_to_lakehouse_path(RAW_DIR)}/full/{scan_id}.json"
            mssparkutils.fs.put(raw_path, json.dumps(payload))
        except Exception:
            pass
    elif not RUNNING_IN_FABRIC:
        # Save locally and optionally upload to lakehouse
        local_raw_path = Path(RAW_DIR) / "full" / f"{scan_id}.json"
        local_raw_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_raw_path, 'w') as f:
            json.dump(payload, f)
        
        if UPLOAD_TO_LAKEHOUSE and LAKEHOUSE_WORKSPACE_ID and LAKEHOUSE_ID:
            upload_to_fabric_lakehouse(
                str(local_raw_path),
                f"{LAKEHOUSE_UPLOAD_PATH}/raw/full/{scan_id}.json",
                LAKEHOUSE_WORKSPACE_ID,
                LAKEHOUSE_ID
            )
    
    # Show API usage stats periodically
    stats = get_api_call_stats()
    if stats['calls'] % 50 == 0:  # Every 50 calls
        print_api_call_stats()
    
    return payload

# --- Flatten helpers ---

def _lower_or(x, default="unknown"):
    return (str(x).lower() if x is not None else default)


def _build_target(server, database, endpoint):
    """Build a consolidated target string from server, database, and endpoint."""
    parts = []
    if server:
        parts.append(f"Server: {server}")
    if database:
        parts.append(f"Database: {database}")
    if endpoint:
        parts.append(f"Endpoint: {endpoint}")
    return " | ".join(parts) if parts else None


def _save_data(rows, curated_dir, table_name, mode="overwrite"):
    """Save data using Spark (Fabric) or pandas (local)."""
    if RUNNING_IN_FABRIC and SPARK_AVAILABLE:
        # Use Spark
        df = spark.createDataFrame(rows)
        df = (
            df.withColumn("connector", F.lower(F.coalesce(F.col("connector"), F.lit("unknown"))))
              .dropDuplicates(["workspace_id","item_id","connector","server","database","endpoint"])
        )
        df.write.mode(mode).parquet(curated_dir)
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(f"CREATE TABLE {table_name} USING PARQUET LOCATION '{curated_dir}'")
        return df.count()
    
    elif PANDAS_AVAILABLE:
        # Use pandas
        # Convert Row objects to dicts if needed
        if rows and hasattr(rows[0], 'asDict'):
            data = [row.asDict() for row in rows]
        else:
            data = rows
        
        df = pd.DataFrame(data)
        
        # Normalize connector column
        if 'connector' in df.columns:
            df['connector'] = df['connector'].fillna('unknown').str.lower()
        
        # Drop duplicates
        df.drop_duplicates(
            subset=["workspace_id","item_id","connector","server","database","endpoint"],
            inplace=True
        )
        
        # Save to parquet and CSV
        output_path = Path(curated_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        parquet_file = output_path / f"{table_name}.parquet"
        csv_file = output_path / f"{table_name}.csv"
        
        df.to_parquet(parquet_file, index=False)
        df.to_csv(csv_file, index=False)
        
        print(f"Saved to: {parquet_file}")
        print(f"Saved to: {csv_file}")
        
        # Upload to Fabric Lakehouse if configured
        if UPLOAD_TO_LAKEHOUSE and LAKEHOUSE_WORKSPACE_ID and LAKEHOUSE_ID:
            print(f"\nUploading to Fabric Lakehouse...")
            upload_to_fabric_lakehouse(
                str(parquet_file),
                f"{LAKEHOUSE_UPLOAD_PATH}/curated/{table_name}.parquet",
                LAKEHOUSE_WORKSPACE_ID,
                LAKEHOUSE_ID
            )
            upload_to_fabric_lakehouse(
                str(csv_file),
                f"{LAKEHOUSE_UPLOAD_PATH}/curated/{table_name}.csv",
                LAKEHOUSE_WORKSPACE_ID,
                LAKEHOUSE_ID
            )
        
        return len(df)
    
    else:
        raise RuntimeError("Neither Spark nor pandas available. Cannot save data.")


def _create_row(data_dict):
    """Create a Row object (Spark) or dict (pandas) based on environment."""
    if RUNNING_IN_FABRIC and Row is not None:
        return Row(**data_dict)
    else:
        return data_dict


def classify_connection_direction(data):
    """
    Classify connections as inbound, outbound, or internal based on connector and target analysis.
    
    Args:
        data: DataFrame (pandas or Spark) with connection information
    
    Returns:
        DataFrame with added 'direction' column
    """
    def get_direction(connector, server, endpoint):
        connector = str(connector).lower() if connector else ''
        server = str(server).lower() if server else ''
        endpoint = str(endpoint).lower() if endpoint else ''
        
        # Patterns indicating OUTBOUND (Fabric connecting to external)
        outbound_patterns = [
            'snowflake', 'salesforce', 'dynamics365', 'rest', 'web',
            'azuresqldatabase', 's3', 'oracle', 'mysql', 'postgresql',
            'sharepoint', 'kusto', 'synapse'
        ]
        
        # Patterns indicating INTERNAL (Fabric-to-Fabric)
        internal_patterns = [
            'onelake', 'fabriclakehouse', 'lakehouse'
        ]
        
        # Outbound: connecting to external cloud services
        if any(pattern in connector for pattern in outbound_patterns):
            return 'outbound'
        
        # Internal: Fabric-to-Fabric connections
        if any(pattern in connector for pattern in internal_patterns):
            return 'internal'
        
        # Check server/endpoint for external domains
        external_domains = [
            '.windows.net', '.snowflakecomputing.com',
            '.salesforce.com', '.dynamics.com', '.azure.com'
        ]
        
        for domain in external_domains:
            if domain in server or domain in endpoint:
                # External domain but check if it's Fabric-related
                if 'onelake' not in server and 'fabric' not in server:
                    return 'outbound'
        
        return 'unknown'
    
    if RUNNING_IN_FABRIC and SPARK_AVAILABLE:
        # Spark DataFrame
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        
        direction_udf = udf(get_direction, StringType())
        return data.withColumn('direction', direction_udf(
            F.col('connector'),
            F.col('server'),
            F.col('endpoint')
        ))
    else:
        # Pandas DataFrame
        data = data.copy()
        data['direction'] = data.apply(
            lambda row: get_direction(row.get('connector'), row.get('server'), row.get('endpoint')),
            axis=1
        )
        return data


def get_activity_events(days_back: int = 30, activity_filter: str = None) -> List[Dict[str, Any]]:
    """
    Get Power BI activity events to detect inbound connections.
    
    Args:
        days_back: Number of days of history to retrieve
        activity_filter: Optional activity type to filter (e.g., 'GetDataset', 'ExecuteQueries')
    
    Returns:
        List of activity event dictionaries
    """
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)
    
    url = f"{PBI_ADMIN_BASE}/activityevents"
    
    all_events = []
    current_date = start_date
    
    # Activity events API requires requests in 24-hour windows
    while current_date < end_date:
        window_end = min(current_date + timedelta(days=1), end_date)
        
        # Format timestamps to match Microsoft's example with 7 decimal places
        params = {
            "startDateTime": current_date.strftime("%Y-%m-%dT%H:%M:%S.0000000Z"),
            "endDateTime": window_end.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")
        }
        
        continuation_token = None
        
        while True:
            if continuation_token:
                params["continuationToken"] = continuation_token
            
            try:
                r = requests.get(url, headers=HEADERS, params=params)
                r.raise_for_status()
                data = r.json()
                
                events = data.get("activityEventEntities", [])
                
                # Apply filter if specified
                if activity_filter:
                    events = [e for e in events if e.get("Activity") == activity_filter]
                
                all_events.extend(events)
                
                continuation_token = data.get("continuationToken")
                if not continuation_token:
                    break
                    
            except requests.exceptions.HTTPError as e:
                print(f"Warning: Activity event API error: {e}")
                break
        
        current_date = window_end
        time.sleep(0.5)  # Rate limit protection
    
    return all_events


def get_inbound_api_activity(days_back: int = 30) -> List[Dict[str, Any]]:
    """
    Get external API usage events showing applications/users accessing your Power BI resources.
    
    Args:
        days_back: Number of days of history to analyze
    
    Returns:
        List of formatted activity records showing inbound API access
    """
    print(f"Fetching activity events (last {days_back} days)...")
    
    # Activities that indicate external access to your resources
    inbound_activities = [
        'GetDataset', 'ExecuteQueries', 'AnalyzeInExcel',
        'ExportReport', 'ViewReport', 'GetTile',
        'GenerateEmbedToken', 'GetDashboard'
    ]
    
    all_events = get_activity_events(days_back=days_back)
    
    # Filter and format events
    inbound_records = []
    for event in all_events:
        activity = event.get('Activity')
        
        if activity in inbound_activities:
            # Exclude browser-based access (focus on API/programmatic)
            user_agent = event.get('UserAgent', '')
            if user_agent and not user_agent.startswith('Mozilla'):
                record = {
                    'timestamp': event.get('CreationTime'),
                    'activity': activity,
                    'user': event.get('UserId'),
                    'client_ip': event.get('ClientIP'),
                    'user_agent': user_agent,
                    'workspace_id': event.get('WorkspaceId'),
                    'workspace_name': event.get('WorkspaceName'),
                    'dataset_id': event.get('DatasetId'),
                    'dataset_name': event.get('DatasetName'),
                    'report_id': event.get('ReportId'),
                    'report_name': event.get('ReportName'),
                    'is_success': event.get('IsSuccess', True)
                }
                inbound_records.append(record)
    
    print(f"Found {len(inbound_records)} inbound API activity events")
    return inbound_records


def analyze_connection_directionality(
    scanner_results_path: str = None,
    include_activity_logs: bool = False,
    activity_days_back: int = 30,
    output_dir: str = None
):
    """
    Comprehensive analysis of connection directionality (outbound vs inbound).
    
    Args:
        scanner_results_path: Path to scanner results (parquet file or SQL table)
        include_activity_logs: Whether to analyze Activity Event API for inbound connections
        activity_days_back: Days of activity history to analyze
        output_dir: Directory to save analysis results
    
    Returns:
        Tuple of (outbound_df, inbound_df) where inbound_df is None if not requested
    """
    print("="*80)
    print("Connection Directionality Analysis")
    print("="*80)
    
    # 1. Load scanner results
    if RUNNING_IN_FABRIC and SPARK_AVAILABLE:
        if scanner_results_path:
            df_scanner = spark.read.parquet(scanner_results_path)
        else:
            df_scanner = spark.sql("SELECT * FROM tenant_cloud_connections")
        
        # Classify direction
        df_scanner = classify_connection_direction(df_scanner)
        
        # Show distribution
        print("\n📊 Scanner API Results (Connection Direction):")
        df_scanner.groupBy('direction').count().show()
        
        # Convert to pandas for consistent output
        df_outbound = df_scanner.toPandas()
        
    elif PANDAS_AVAILABLE:
        if scanner_results_path:
            df_scanner = pd.read_parquet(scanner_results_path)
        else:
            df_scanner = pd.read_parquet(f"{CURATED_DIR}/tenant_cloud_connections.parquet")
        
        # Classify direction
        df_outbound = classify_connection_direction(df_scanner)
        
        # Show distribution
        print("\n📊 Scanner API Results (Connection Direction):")
        print(df_outbound.groupby('direction')['item_id'].count())
    
    else:
        raise RuntimeError("Neither Spark nor pandas available")
    
    # 2. Analyze inbound activity (if enabled)
    df_inbound = None
    if include_activity_logs:
        print(f"\n🔍 Analyzing Activity Event API (last {activity_days_back} days)...")
        
        try:
            inbound_records = get_inbound_api_activity(days_back=activity_days_back)
            
            if inbound_records:
                df_inbound = pd.DataFrame(inbound_records)
                
                print(f"\n📥 Inbound Activity Summary:")
                print(f"   Total API calls: {len(df_inbound)}")
                print(f"   Unique users: {df_inbound['user'].nunique()}")
                print(f"   Unique IPs: {df_inbound['client_ip'].nunique()}")
                print(f"   Unique workspaces: {df_inbound['workspace_id'].nunique()}")
                
                print(f"\n   Top Activities:")
                print(df_inbound.groupby('activity')['timestamp'].count().sort_values(ascending=False).head(10))
                
                # Save if output directory specified
                if output_dir:
                    output_path = Path(output_dir)
                    output_path.mkdir(parents=True, exist_ok=True)
                    
                    inbound_file = output_path / "inbound_api_activity.parquet"
                    df_inbound.to_parquet(inbound_file, index=False)
                    
                    inbound_csv = output_path / "inbound_api_activity.csv"
                    df_inbound.to_csv(inbound_csv, index=False)
                    
                    print(f"\n💾 Saved inbound activity to: {inbound_file}")
            else:
                print("\n⚠️  No inbound API activity found (non-browser access only)")
                
        except Exception as e:
            print(f"\n⚠️  Activity event analysis failed: {e}")
    
    # 3. Save classified outbound connections
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        outbound_file = output_path / "connections_with_direction.parquet"
        df_outbound.to_parquet(outbound_file, index=False)
        
        outbound_csv = output_path / "connections_with_direction.csv"
        df_outbound.to_csv(outbound_csv, index=False)
        
        print(f"\n💾 Saved classified connections to: {outbound_file}")
    
    print("\n" + "="*80)
    print("Analysis Complete")
    print("="*80)
    
    return df_outbound, df_inbound


def flatten_scan_payload(payload: Dict[str, Any], ws_sidecar: Dict[str, Dict[str, str]]) -> List:
    rows: List = []
    
    # Validate payload is a dictionary
    if not isinstance(payload, dict):
        print(f"Warning: flatten_scan_payload received {type(payload).__name__} instead of dict, skipping")
        return rows
    
    for ws in (payload.get("workspaces") or []):
        ws_id   = ws.get("id")
        itemset = ws.get("items") or []
        wmeta   = ws_sidecar.get(ws_id, {"name": ws.get("name") or "", "kind": _lower_or(ws.get("type")), "users": None, "capacity_id": None, "capacity_name": "Shared", "is_dedicated_capacity": False})
        ws_name = wmeta.get("name", "")
        ws_kind = wmeta.get("kind", "unknown")
        ws_users = wmeta.get("users")
        ws_capacity_id = wmeta.get("capacity_id")
        ws_capacity_name = wmeta.get("capacity_name", "Shared")
        ws_is_dedicated = wmeta.get("is_dedicated_capacity", False)

        for item in itemset:
            item_id   = item.get("id")
            item_name = item.get("name")
            item_type = _lower_or(item.get("type"))

            if item_type in {"semanticmodel", "dataset"}:
                # Extract item-level metadata
                item_creator = item.get("createdBy") or item.get("configuredBy")
                item_modified_by = item.get("modifiedBy")
                item_modified_date = item.get("modifiedDateTime")
                
                for ds in (item.get("datasources") or []):
                    conn      = ds.get("connectionDetails") or {}
                    connector = _lower_or(conn.get("datasourceType"))
                    server    = conn.get("server") or conn.get("host")
                    database  = conn.get("database") or conn.get("db")
                    gateway_id= ds.get("gatewayId")
                    connection_scope = "OnPremViaGateway" if gateway_id else "Cloud"
                    cloud_flag       = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    target    = _build_target(server, database, None)
                    rows.append(_create_row({
                        "workspace_id":   ws_id,
                        "workspace_name": ws_name,
                        "workspace_kind": ws_kind,
                        "workspace_users": ws_users,
                        "capacity_id":    ws_capacity_id,
                        "capacity_name":  ws_capacity_name,
                        "is_dedicated_capacity": ws_is_dedicated,
                        "item_id":        item_id,
                        "item_name":      item_name,
                        "item_type":      "SemanticModel",
                        "item_creator":   item_creator,
                        "item_modified_by": item_modified_by,
                        "item_modified_date": item_modified_date,
                        "connector":      connector,
                        "target":         target,
                        "server":         server,
                        "database":       database,
                        "endpoint":       None,
                        "connection_scope": connection_scope,
                        "cloud":          cloud_flag,
                        "generation":     None
                    }))

            elif item_type == "dataflow":
                generation = item.get("generation") or (item.get("properties") or {}).get("generation")
                item_creator = item.get("createdBy") or item.get("configuredBy")
                item_modified_by = item.get("modifiedBy")
                item_modified_date = item.get("modifiedDateTime")
                
                sources    = item.get("sources") or item.get("entities") or []
                for src in sources:
                    connector = _lower_or(src.get("type") or src.get("provider"))
                    endpoint  = src.get("url") or src.get("path")
                    connection_scope = "Cloud"
                    cloud_flag       = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    target    = _build_target(None, None, endpoint)
                    rows.append(_create_row({
                        "workspace_id":   ws_id,
                        "workspace_name": ws_name,
                        "workspace_kind": ws_kind,
                        "workspace_users": ws_users,
                        "capacity_id":    ws_capacity_id,
                        "capacity_name":  ws_capacity_name,
                        "is_dedicated_capacity": ws_is_dedicated,
                        "item_id":        item_id,
                        "item_name":      item_name,
                        "item_type":      "Dataflow",
                        "item_creator":   item_creator,
                        "item_modified_by": item_modified_by,
                        "item_modified_date": item_modified_date,
                        "connector":      connector,
                        "target":         target,
                        "server":         None,
                        "database":       None,
                        "endpoint":       endpoint,
                        "connection_scope": connection_scope,
                        "cloud":          cloud_flag,
                        "generation":     generation
                    }))

            elif item_type == "pipeline":
                item_creator = item.get("createdBy") or item.get("configuredBy")
                item_modified_by = item.get("modifiedBy")
                item_modified_date = item.get("modifiedDateTime")
                
                for act in (item.get("activities") or []):
                    ref       = act.get("linkedService") or {}
                    connector = _lower_or(ref.get("type") or act.get("type"))
                    endpoint  = ref.get("url") or ref.get("endpoint")
                    gateway_id= ref.get("gatewayId")
                    connection_scope = "OnPremViaGateway" if gateway_id else "Cloud"
                    cloud_flag       = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    target    = _build_target(None, None, endpoint)
                    rows.append(_create_row({
                        "workspace_id":   ws_id,
                        "workspace_name": ws_name,
                        "workspace_kind": ws_kind,
                        "workspace_users": ws_users,
                        "capacity_id":    ws_capacity_id,
                        "capacity_name":  ws_capacity_name,
                        "is_dedicated_capacity": ws_is_dedicated,
                        "item_id":        item_id,
                        "item_name":      item_name,
                        "item_type":      "Pipeline",
                        "item_creator":   item_creator,
                        "item_modified_by": item_modified_by,
                        "item_modified_date": item_modified_date,
                        "connector":      connector,
                        "target":         target,
                        "server":         None,
                        "database":       None,
                        "endpoint":       endpoint,
                        "connection_scope": connection_scope,
                        "cloud":          cloud_flag,
                        "generation":     None
                    }))

            elif item_type in {"lakehouse", "notebook"}:
                item_creator = item.get("createdBy") or item.get("configuredBy")
                item_modified_by = item.get("modifiedBy")
                item_modified_date = item.get("modifiedDateTime")
                
                references = (item.get("connections") or []) + (item.get("lineage") or [])
                for ref in references:
                    connector       = _lower_or(ref.get("type"))
                    endpoint        = ref.get("url") or ref.get("endpoint")
                    is_cloud_flag   = ref.get("isCloud", True)
                    connection_scope= "Cloud" if is_cloud_flag else "OnPremViaGateway"
                    cloud_flag      = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    target          = _build_target(None, None, endpoint)
                    rows.append(_create_row({
                        "workspace_id":   ws_id,
                        "workspace_name": ws_name,
                        "workspace_kind": ws_kind,
                        "workspace_users": ws_users,
                        "capacity_id":    ws_capacity_id,
                        "capacity_name":  ws_capacity_name,
                        "is_dedicated_capacity": ws_is_dedicated,
                        "item_id":        item_id,
                        "item_name":      item_name,
                        "item_type":      item_type.capitalize(),
                        "item_creator":   item_creator,
                        "item_modified_by": item_modified_by,
                        "item_modified_date": item_modified_date,
                        "connector":      connector,
                        "target":         target,
                        "server":         None,
                        "database":       None,
                        "endpoint":       endpoint,
                        "connection_scope": connection_scope,
                        "cloud":          cloud_flag,
                        "generation":     None
                    }))

            else:
                item_creator = item.get("createdBy") or item.get("configuredBy")
                item_modified_by = item.get("modifiedBy")
                item_modified_date = item.get("modifiedDateTime")
                
                rows.append(_create_row({
                    "workspace_id":   ws_id,
                    "workspace_name": ws_name,
                    "workspace_kind": ws_kind,
                    "workspace_users": ws_users,
                    "capacity_id":    ws_capacity_id,
                    "capacity_name":  ws_capacity_name,
                    "is_dedicated_capacity": ws_is_dedicated,
                    "item_id":        item_id,
                    "item_name":      item_name,
                    "item_type":      item_type.capitalize(),
                    "item_creator":   item_creator,
                    "item_modified_by": item_modified_by,
                    "item_modified_date": item_modified_date,
                    "connector":      "unknown",
                    "target":         None,
                    "server":         None,
                    "database":       None,
                    "endpoint":       None,
                    "connection_scope": "Cloud",
                    "cloud":          True,
                    "generation":     None
                }))
    return rows

# --- Capacity Grouping Helper ---

def group_workspaces_by_capacity(workspaces: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group workspaces by their capacity ID for organized batch processing.
    
    This enables:
    - Sequential processing by capacity (better organization)
    - Foundation for parallel capacity scanning (Phase 3)
    - Capacity-specific progress tracking
    
    Args:
        workspaces: List of workspace dictionaries from get_all_workspaces()
    
    Returns:
        Dictionary mapping capacity_id -> list of workspaces
        Special key "shared" for workspaces without dedicated capacity
    
    Example:
        {
            "capacity-abc-123": [{"id": "ws1", ...}, {"id": "ws2", ...}],
            "capacity-def-456": [{"id": "ws3", ...}],
            "shared": [{"id": "ws4", ...}, {"id": "ws5", ...}]
        }
    """
    capacity_groups = {}
    
    for ws in workspaces:
        capacity_id = ws.get("capacityId")
        is_dedicated = ws.get("isOnDedicatedCapacity", False)
        
        # Determine group key
        if capacity_id and is_dedicated:
            group_key = capacity_id
        else:
            group_key = "shared"
        
        # Initialize group if needed
        if group_key not in capacity_groups:
            capacity_groups[group_key] = []
        
        capacity_groups[group_key].append(ws)
    
    return capacity_groups


def scan_capacities_parallel(
    capacity_groups: Dict[str, List[Dict[str, Any]]],
    ws_sidecar: Dict[str, Dict[str, str]],
    max_parallel_capacities: int = 2,
    max_calls_per_hour: int = 450,
    capacity_filter: List[str] = None,
    exclude_capacities: List[str] = None,
    capacity_priority: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Scan multiple capacities in parallel with shared rate limiting.
    
    Phase 3 implementation: Enables concurrent scanning of different capacities
    while respecting global API rate limits through SharedRateLimiter.
    
    Args:
        capacity_groups: Dictionary of capacity_id -> list of workspaces
        ws_sidecar: Workspace metadata dictionary
        max_parallel_capacities: Number of capacities to scan concurrently (default: 2)
        max_calls_per_hour: Total API calls per hour across all workers (default: 450)
        capacity_filter: Only scan these capacity IDs (None = all)
        exclude_capacities: Skip these capacity IDs (None = none)
        capacity_priority: Process capacities in this order (rest processed after)
    
    Returns:
        List of scan payloads from all capacities
    """
    # Apply capacity filters
    filtered_groups = {}
    
    if capacity_filter:
        # Only include specified capacities
        for cap_id in capacity_filter:
            if cap_id in capacity_groups:
                filtered_groups[cap_id] = capacity_groups[cap_id]
            else:
                print(f"⚠️  Warning: Capacity filter '{cap_id}' not found in discovered capacities")
    else:
        filtered_groups = dict(capacity_groups)
    
    # Exclude capacities
    if exclude_capacities:
        for cap_id in exclude_capacities:
            if cap_id in filtered_groups:
                removed_count = len(filtered_groups[cap_id])
                del filtered_groups[cap_id]
                print(f"🚫 Excluded capacity '{cap_id}' ({removed_count} workspaces)")
    
    if not filtered_groups:
        print("❌ No capacities to scan after applying filters")
        return []
    
    # Determine processing order
    capacity_ids = list(filtered_groups.keys())
    
    if capacity_priority:
        # Process priority capacities first
        priority_caps = [cap_id for cap_id in capacity_priority if cap_id in filtered_groups]
        other_caps = [cap_id for cap_id in capacity_ids if cap_id not in priority_caps]
        ordered_capacity_ids = priority_caps + other_caps
        
        if priority_caps:
            print(f"\n🎯 Priority capacities (will be processed first): {len(priority_caps)}")
            for cap_id in priority_caps:
                print(f"   - {cap_id}: {len(filtered_groups[cap_id])} workspaces")
    else:
        # Default: Sort by workspace count (largest first)
        ordered_capacity_ids = sorted(
            capacity_ids,
            key=lambda x: len(filtered_groups[x]),
            reverse=True
        )
    
    print(f"\n📊 Parallel Capacity Scanning Configuration:")
    print(f"   Capacities to scan: {len(filtered_groups)}")
    print(f"   Parallel workers: {max_parallel_capacities}")
    print(f"   Total API quota: {max_calls_per_hour} calls/hour")
    print(f"   Processing order: {'Priority-based' if capacity_priority else 'Largest-first'}")
    print()
    
    # Initialize shared rate limiter
    rate_limiter = SharedRateLimiter(
        max_calls_per_hour=max_calls_per_hour,
        max_parallel_workers=max_parallel_capacities
    )
    
    all_scan_payloads = []
    scan_payloads_lock = threading.Lock()
    
    def scan_single_capacity(cap_id: str, cap_workspaces: List[Dict[str, Any]]) -> None:
        """Scan a single capacity with rate limiting."""
        worker_id = f"capacity_{cap_id}"
        rate_limiter.allocate_worker(worker_id)
        
        cap_display = cap_id if cap_id != "shared" else "Shared Capacity"
        print(f"\n🔄 [{worker_id}] Starting scan: {len(cap_workspaces)} workspaces")
        
        # Create batches for this capacity
        ws_list = [{
            "id": w.get("id"),
            "name": w.get("name", ""),
            "type": (str(w.get("type")).lower() if w.get("type") else "unknown")
        } for w in cap_workspaces if w.get("id")]
        
        batches = [ws_list[i:i+BATCH_SIZE_WORKSPACES] 
                   for i in range(0, len(ws_list), BATCH_SIZE_WORKSPACES)]
        
        print(f"   [{worker_id}] Processing {len(batches)} batches")
        
        capacity_payloads = []
        
        for batch_idx, batch in enumerate(batches, 1):
            # Check rate limit quota
            if not rate_limiter.acquire(worker_id, count=1):
                print(f"   [{worker_id}] ⚠️  Quota exhausted at batch {batch_idx}/{len(batches)}")
                print(f"   [{worker_id}] Processed {batch_idx-1}/{len(batches)} batches before quota limit")
                break
            
            try:
                # Make API call
                ids = [w.get("id") for w in batch if w.get("id")]
                scan_id = post_workspace_info(ids)
                poll_scan_status(scan_id)
                payload = read_scan_result(scan_id)
                payload["workspace_sidecar"] = ws_sidecar
                capacity_payloads.append(payload)
                
                if batch_idx % 10 == 0 or batch_idx == len(batches):
                    print(f"   [{worker_id}] Progress: {batch_idx}/{len(batches)} batches")
                    
            except Exception as e:
                print(f"   [{worker_id}] ❌ Error in batch {batch_idx}: {e}")
                rate_limiter.release(worker_id, count=1)  # Return quota
                continue
        
        # Add to global results
        with scan_payloads_lock:
            all_scan_payloads.extend(capacity_payloads)
        
        print(f"   [{worker_id}] ✅ Completed: {len(capacity_payloads)} batches scanned")
    
    # Execute parallel scanning
    print(f"\n{'='*70}")
    print(f"Starting parallel capacity scanning...")
    print(f"{'='*70}\n")
    
    with ThreadPoolExecutor(max_workers=max_parallel_capacities) as executor:
        futures = {
            executor.submit(scan_single_capacity, cap_id, filtered_groups[cap_id]): cap_id
            for cap_id in ordered_capacity_ids
        }
        
        for future in as_completed(futures):
            cap_id = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Capacity {cap_id} failed: {e}")
    
    # Print final statistics
    stats = rate_limiter.get_stats()
    print(f"\n{'='*70}")
    print(f"📊 Parallel Scan Statistics:")
    print(f"{'='*70}")
    print(f"Total API calls made: {stats['total_calls_made']}")
    print(f"Remaining quota: {stats['remaining_quota']}")
    print(f"Average rate: {stats['calls_per_hour_rate']:.1f} calls/hour")
    print(f"Elapsed time: {stats['elapsed_hours']:.2f} hours")
    print(f"\nPer-capacity quota usage:")
    for worker_id, remaining in stats['worker_quotas'].items():
        used = rate_limiter.calls_per_worker - remaining
        print(f"   {worker_id}: {used}/{rate_limiter.calls_per_worker} calls used")
    print(f"{'='*70}\n")
    
    return all_scan_payloads

# --- Full tenant scan ---

def full_tenant_scan(include_personal: bool = True,
                     curated_dir: str = CURATED_DIR,
                     table_name: str = "tenant_cloud_connections",
                     group_by_capacity: bool = False,
                     parallel_capacities: int = 1,
                     max_calls_per_hour: int = 450,
                     capacity_filter: List[str] = None,
                     exclude_capacities: List[str] = None,
                     capacity_priority: List[str] = None) -> None:
    """
    Full tenant scan with optional capacity grouping and parallel scanning.
    
    Args:
        include_personal: Include personal workspaces
        curated_dir: Output directory
        table_name: SQL table name
        group_by_capacity: Group and process workspaces by capacity (better organization)
        parallel_capacities: Number of capacities to scan in parallel (1=sequential, 2+=parallel)
        max_calls_per_hour: Total API calls per hour for parallel mode (default: 450)
        capacity_filter: Only scan these capacity IDs (e.g., ['cap-123', 'cap-456'])
        exclude_capacities: Skip these capacity IDs (e.g., ['shared'])
        capacity_priority: Process capacities in this order (rest follow)
    """
    ws_min = get_all_workspaces(include_personal=include_personal)
    if not ws_min:
        print("No workspaces discovered.")
        return

    print(f"Discovered {len(ws_min)} workspaces (include_personal={include_personal}).")
    
    # Warn if very large tenant (>10k workspaces)
    if len(ws_min) > 10000:
        print(f"\n⚠️  LARGE TENANT DETECTED ({len(ws_min)} workspaces)")
        print(f"   Estimated API calls: {(len(ws_min) / 100) * 8:.0f}")
        print(f"   Estimated duration: {(len(ws_min) / 100) / 52:.1f} hours at MAX_PARALLEL_SCANS={MAX_PARALLEL_SCANS}")
        print(f"\n💡 RECOMMENDATIONS:")
        print(f"   1. Run check_scanner_api_health() first to detect API contention")
        print(f"   2. Consider using full_tenant_scan_chunked() for rate limit safety")
        print(f"   3. Schedule for off-hours to avoid impacting other users")
        print(f"   4. After baseline scan, use incremental_update() with hash optimization\n")

    # Build workspace sidecar for metadata
    ws_sidecar = {w.get("id"): {
        "name": w.get("name", ""), 
        "kind": "workspace",
        "capacity_id": w.get("capacityId"),
        "capacity_name": w.get("capacityName") or "Shared",
        "is_dedicated_capacity": w.get("isOnDedicatedCapacity", False)
    } for w in ws_min if w.get("id")}
    
    # Optional: Group by capacity for better organization and/or parallel scanning
    if group_by_capacity or parallel_capacities > 1:
        capacity_groups = group_workspaces_by_capacity(ws_min)
        print(f"\n📊 Grouping workspaces by capacity...")
        print(f"Found {len(capacity_groups)} capacity groups:")
        
        for cap_id, cap_workspaces in sorted(capacity_groups.items(), key=lambda x: len(x[1]), reverse=True):
            cap_name = "Shared Capacity" if cap_id == "shared" else f"Capacity {cap_id[:8]}..."
            print(f"  - {cap_name}: {len(cap_workspaces)} workspaces")
        print()
        
        # Phase 3: Parallel capacity scanning
        if parallel_capacities > 1:
            print(f"\n🚀 Phase 3: Parallel Capacity Scanning Enabled")
            print(f"   Parallel workers: {parallel_capacities}")
            print(f"   Total API quota: {max_calls_per_hour} calls/hour\n")
            
            scan_payloads = scan_capacities_parallel(
                capacity_groups=capacity_groups,
                ws_sidecar=ws_sidecar,
                max_parallel_capacities=parallel_capacities,
                max_calls_per_hour=max_calls_per_hour,
                capacity_filter=capacity_filter,
                exclude_capacities=exclude_capacities,
                capacity_priority=capacity_priority
            )
        else:
            # Sequential capacity processing (Phase 2)
            all_scan_payloads = []
            
            for cap_idx, (cap_id, cap_workspaces) in enumerate(capacity_groups.items(), 1):
                cap_name = "Shared Capacity" if cap_id == "shared" else f"Capacity {cap_id[:8]}..."
                print(f"\n{'='*70}")
                print(f"📍 Processing Capacity {cap_idx}/{len(capacity_groups)}: {cap_name}")
                print(f"   Workspaces: {len(cap_workspaces)}")
                print(f"{'='*70}")
                
                # Create workspace list for this capacity
                ws_list = [{
                    "id":   w.get("id"),
                    "name": w.get("name", ""),
                    "type": (str(w.get("type")).lower() if w.get("type") else "unknown")
                } for w in cap_workspaces if w.get("id")]
                
                batches = [ws_list[i:i+BATCH_SIZE_WORKSPACES] for i in range(0, len(ws_list), BATCH_SIZE_WORKSPACES)]
                scan_payloads: List[Dict[str, Any]] = []
                
                print(f"📦 Processing {len(batches)} batches for this capacity...")
                
                with ThreadPoolExecutor(max_workers=MAX_PARALLEL_SCANS) as pool:
                    futures = [pool.submit(run_one_batch, b) for b in batches]
                    
                    future_iterator = as_completed(futures)
                    if TQDM_AVAILABLE:
                        future_iterator = tqdm(as_completed(futures), total=len(futures),
                                              desc=f"{cap_name}", unit="batch")
                    
                    for fut in future_iterator:
                        scan_payloads.append(fut.result())
                
                print(f"✅ Completed {len(scan_payloads)} batches for {cap_name}")
                all_scan_payloads.extend(scan_payloads)
            
            print(f"\n{'='*70}")
            print(f"✅ All {len(capacity_groups)} capacity groups processed")
            print(f"{'='*70}\n")
            
            # Use all collected payloads
            scan_payloads = all_scan_payloads
        
    else:
        # Original non-grouped processing
        ws_list = [{
            "id":   w.get("id"),
            "name": w.get("name", ""),
            "type": (str(w.get("type")).lower() if w.get("type") else "unknown")
        } for w in ws_min if w.get("id")]

        batches = [ws_list[i:i+BATCH_SIZE_WORKSPACES] for i in range(0, len(ws_list), BATCH_SIZE_WORKSPACES)]
        scan_payloads: List[Dict[str, Any]] = []

        print(f"📦 Processing {len(batches)} batches...")
    
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_SCANS) as pool:
        futures = [pool.submit(run_one_batch, b) for b in batches]
        
        # Progress bar if available
        future_iterator = as_completed(futures)
        if TQDM_AVAILABLE:
            future_iterator = tqdm(as_completed(futures), total=len(futures), 
                                  desc="Scanning batches", unit="batch")
        
            for fut in future_iterator:
                scan_payloads.append(fut.result())

        print(f"Completed {len(scan_payloads)} full scan batches.")
    
    # Continue with flattening (same for both grouped and non-grouped)
    print(f"\n📊 Total scan payloads collected: {len(scan_payloads)}")

    all_rows = []
    for payload in scan_payloads:
        sidecar = payload.get("workspace_sidecar", {})
        all_rows.extend(flatten_scan_payload(payload, sidecar))

    if not all_rows:
        print("No connection rows produced by full scan.")
        return

    row_count = _save_data(all_rows, curated_dir, table_name, mode="overwrite")
    print(f"Full tenant scan completed. Rows saved: {row_count} | Curated path: {curated_dir} | SQL table: {table_name}")
    
    # Show final API usage statistics
    print_api_call_stats()

# --- Full tenant scan with rate limit management (for large tenants) ---

def full_tenant_scan_chunked(include_personal: bool = True,
                              max_batches_per_hour: int = 450,  # Leave buffer under 500/hour limit
                              curated_dir: str = CURATED_DIR,
                              table_name: str = "tenant_cloud_connections",
                              enable_checkpointing: bool = None,
                              checkpoint_storage: str = None,
                              group_by_capacity: bool = False,
                              parallel_capacities: int = 1,
                              capacity_filter: List[str] = None,
                              exclude_capacities: List[str] = None,
                              capacity_priority: List[str] = None) -> None:
    """
    Full tenant scan with automatic rate limit management for very large tenants.
    Processes workspaces in hourly chunks, respecting the 500 API calls/hour limit.
    Merges results incrementally to avoid losing progress.
    Supports checkpoint/resume for long-running scans.
    
    Args:
        include_personal: Include personal workspaces
        max_batches_per_hour: Max API calls per hour (default 450 for safety margin)
        curated_dir: Output directory
        table_name: SQL table name
        enable_checkpointing: Enable checkpoint/resume (default: ENABLE_CHECKPOINTING global)
        checkpoint_storage: 'json' or 'lakehouse' (default: CHECKPOINT_STORAGE global)
        group_by_capacity: Group workspaces by capacity (recommended for multi-capacity tenants)
        parallel_capacities: Number of capacities to scan in parallel (1=sequential, 2+=parallel)
        capacity_filter: Only scan these capacity IDs (e.g., ['cap-123', 'cap-456'])
        exclude_capacities: Skip these capacity IDs (e.g., ['shared'])
        capacity_priority: Process capacities in this order (rest follow)
    """
    # Use global settings if not specified
    if enable_checkpointing is None:
        enable_checkpointing = ENABLE_CHECKPOINTING
    if checkpoint_storage is None:
        checkpoint_storage = CHECKPOINT_STORAGE
    
    # Initialize checkpoint manager
    checkpoint_id = f"full_scan_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    checkpoint_mgr = CheckpointManager(checkpoint_id, storage=checkpoint_storage) if enable_checkpointing else None
    
    # Try to load checkpoint
    completed_batch_indices = set()
    start_chunk_idx = 0
    
    if enable_checkpointing and checkpoint_mgr:
        checkpoint = checkpoint_mgr.load_checkpoint()
        if checkpoint:
            completed_batch_indices = set(checkpoint.get('completed_batch_indices', []))
            start_chunk_idx = checkpoint.get('next_chunk_idx', 0)
            print(f"🔄 Resuming from checkpoint: {len(completed_batch_indices)} batches already completed")
    
    ws_min = get_all_workspaces(include_personal=include_personal)
    if not ws_min:
        print("No workspaces discovered.")
        return

    print(f"📊 Discovered {len(ws_min)} workspaces (include_personal={include_personal}).")
    
    # Optional: Show capacity distribution
    if group_by_capacity:
        capacity_groups = group_workspaces_by_capacity(ws_min)
        print(f"\n📊 Capacity Distribution:")
        for cap_id, cap_workspaces in sorted(capacity_groups.items(), key=lambda x: len(x[1]), reverse=True):
            cap_name = "Shared" if cap_id == "shared" else f"Capacity {cap_id[:8]}..."
            print(f"   {cap_name}: {len(cap_workspaces)} workspaces")
        print()
    
    ws_list = [{
        "id":   w.get("id"),
        "name": w.get("name", ""),
        "type": (str(w.get("type")).lower() if w.get("type") else "unknown")
    } for w in ws_min if w.get("id")]

    all_batches = [ws_list[i:i+BATCH_SIZE_WORKSPACES] for i in range(0, len(ws_list), BATCH_SIZE_WORKSPACES)]
    total_batches = len(all_batches)
    
    remaining_batches = total_batches - len(completed_batch_indices)
    print(f"📦 Total batches: {total_batches} | Remaining: {remaining_batches}")
    
    if enable_checkpointing:
        print(f"💾 Checkpointing enabled: Saving every {CHECKPOINT_INTERVAL} batches to {checkpoint_storage}")
    
    print(f"⏱️  Estimated time: {remaining_batches / max_batches_per_hour:.1f} hours")
    print(f"🔄 Processing in chunks of {max_batches_per_hour} batches/hour to respect rate limits...")
    
    # Progress bar for overall scan (if tqdm available)
    pbar_overall = None
    if TQDM_AVAILABLE:
        pbar_overall = tqdm(total=total_batches, initial=len(completed_batch_indices), 
                           desc="Overall Progress", unit="batch", position=0)
    
    # Process in hourly chunks
    chunk_range = range(start_chunk_idx, total_batches, max_batches_per_hour)
    for chunk_idx in chunk_range:
        chunk_start = chunk_idx
        chunk_end = min(chunk_idx + max_batches_per_hour, total_batches)
        
        # Filter out already completed batches
        chunk_batch_indices = [i for i in range(chunk_start, chunk_end) if i not in completed_batch_indices]
        if not chunk_batch_indices:
            print(f"⏭️  Skipping chunk {chunk_idx // max_batches_per_hour + 1} (already completed)")
            continue
        
        chunk_batches = [all_batches[i] for i in chunk_batch_indices]
        
        print(f"\n{'='*60}")
        print(f"🔹 Chunk {chunk_idx // max_batches_per_hour + 1}: Processing {len(chunk_batches)} batches ({chunk_start+1}-{chunk_end} of {total_batches})")
        print(f"{'='*60}")
        
        chunk_start_time = time.time()
        scan_payloads: List[Dict[str, Any]] = []
        
        # Progress bar for this chunk (if tqdm available)
        batch_iterator = enumerate(chunk_batches)
        if TQDM_AVAILABLE:
            batch_iterator = tqdm(batch_iterator, total=len(chunk_batches), 
                                desc=f"Chunk {chunk_idx // max_batches_per_hour + 1}", 
                                unit="batch", position=1, leave=False)
        
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_SCANS) as pool:
            futures = {pool.submit(run_one_batch, b): (chunk_batch_indices[i], b) for i, b in enumerate(chunk_batches)}
            
            for fut in as_completed(futures):
                batch_idx, batch_meta = futures[fut]
                try:
                    payload = fut.result()
                    scan_payloads.append(payload)
                    completed_batch_indices.add(batch_idx)
                    
                    if pbar_overall:
                        pbar_overall.update(1)
                    
                    # Save checkpoint periodically
                    if enable_checkpointing and checkpoint_mgr and len(completed_batch_indices) % CHECKPOINT_INTERVAL == 0:
                        checkpoint_mgr.save_checkpoint({
                            'completed_batch_indices': list(completed_batch_indices),
                            'next_chunk_idx': chunk_idx + max_batches_per_hour,
                            'total_batches': total_batches,
                            'scan_id': checkpoint_id
                        })
                except Exception as e:
                    print(f"⚠️  Batch {batch_idx} failed: {e}")
        
        print(f"✅ Completed {len(scan_payloads)} batches in this chunk.")
        
        # Flatten and save this chunk's results
        all_rows = []
        for payload in scan_payloads:
            sidecar = payload.get("workspace_sidecar", {})
            all_rows.extend(flatten_scan_payload(payload, sidecar))
        
        if all_rows:
            if RUNNING_IN_FABRIC and SPARK_AVAILABLE:
                df_new = spark.createDataFrame(all_rows)
                df_new = (
                    df_new.withColumn("connector", F.lower(F.coalesce(F.col("connector"), F.lit("unknown"))))
                          .dropDuplicates(["workspace_id","item_id","connector","server","database","endpoint"])
                )
                
                # Merge with existing data if table exists
                try:
                    df_existing = spark.read.parquet(curated_dir)
                    df_combined = df_existing.union(df_new).dropDuplicates(
                        ["workspace_id","item_id","connector","server","database","endpoint"]
                    )
                    df_combined.write.mode("overwrite").parquet(curated_dir)
                    print(f"💾 Merged {len(all_rows)} new rows with existing data.")
                except Exception:
                    # First chunk - no existing data
                    df_new.write.mode("overwrite").parquet(curated_dir)
                    print(f"💾 Saved {len(all_rows)} rows (initial write).")
                
                # Update SQL table
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                spark.sql(f"CREATE TABLE {table_name} USING PARQUET LOCATION '{curated_dir}'")
            else:
                # Pandas (local execution)
                _save_data(all_rows, curated_dir, table_name, mode="append")
        
        # Save checkpoint after each chunk
        if enable_checkpointing and checkpoint_mgr:
            checkpoint_mgr.save_checkpoint({
                'completed_batch_indices': list(completed_batch_indices),
                'next_chunk_idx': chunk_idx + max_batches_per_hour,
                'total_batches': total_batches,
                'scan_id': checkpoint_id
            })
        
        # Check if we need to wait before next chunk
        if chunk_end < total_batches:
            chunk_elapsed = time.time() - chunk_start_time
            wait_time = max(0, 3600 - chunk_elapsed)  # Wait until 1 hour has passed
            
            if wait_time > 60:
                print(f"\n⏳ Rate limit protection: Waiting {wait_time/60:.1f} minutes before next chunk...")
                print(f"   (Processed {len(completed_batch_indices)}/{total_batches} batches so far)")
                time.sleep(wait_time)
            elif wait_time > 0:
                print(f"⏳ Brief pause: {wait_time:.0f} seconds...")
                time.sleep(wait_time)
    
    if pbar_overall:
        pbar_overall.close()
    
    # Clear checkpoint after successful completion
    if enable_checkpointing and checkpoint_mgr:
        checkpoint_mgr.clear_checkpoint()
    
    print(f"\n{'='*60}")
    print(f"✅ Full chunked scan completed!")
    print(f"📊 Total batches processed: {total_batches}")
    print(f"💾 SQL table: {table_name}")
    print(f"📁 Curated path: {curated_dir}")
    print(f"{'='*60}")

# --- Incremental scan ---

def run_one_batch_incremental(batch_meta: List[Dict[str, Any]]) -> Dict[str, Any]:
    ids = [w.get("id") for w in batch_meta if w.get("id")]
    scan_id = post_workspace_info(ids)
    poll_scan_status(scan_id)
    payload = read_scan_result(scan_id)
    
    # Extract workspace users/owners from scan result
    ws_users_map = {}
    for ws in (payload.get("workspaces") or []):
        ws_id = ws.get("id")
        users = ws.get("users") or []
        # Get workspace admins/owners
        admins = [u.get("emailAddress") or u.get("identifier") 
                  for u in users if u.get("workspaceUserAccessRight") in {"Admin", "Member"}]
        ws_users_map[ws_id] = ", ".join(admins[:5]) if admins else None  # Limit to first 5
    
    sidecar = {
        w.get("id"): {
            "name": w.get("name", ""),
            "kind": (str(w.get("type")).lower() if w.get("type") else "unknown"),
            "users": ws_users_map.get(w.get("id"))
        } for w in batch_meta if w.get("id")
    }
    payload["workspace_sidecar"] = sidecar
    if RUNNING_IN_FABRIC and mssparkutils is not None:
        try:
            raw_path = f"{_to_lakehouse_path(RAW_DIR)}/incremental/{scan_id}.json"
            mssparkutils.fs.put(raw_path, json.dumps(payload))
        except Exception:
            pass
    elif not RUNNING_IN_FABRIC:
        # Save locally and optionally upload to lakehouse
        local_raw_path = Path(RAW_DIR) / "incremental" / f"{scan_id}.json"
        local_raw_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_raw_path, 'w') as f:
            json.dump(payload, f)
        
        if UPLOAD_TO_LAKEHOUSE and LAKEHOUSE_WORKSPACE_ID and LAKEHOUSE_ID:
            upload_to_fabric_lakehouse(
                str(local_raw_path),
                f"{LAKEHOUSE_UPLOAD_PATH}/raw/incremental/{scan_id}.json",
                LAKEHOUSE_WORKSPACE_ID,
                LAKEHOUSE_ID
            )
    return payload


def incremental_update(modified_since_iso: str,
                       include_personal: bool = True,
                       enable_hash_optimization: bool = True,
                       curated_dir: str = CURATED_DIR,
                       table_name: str = "tenant_cloud_connections") -> None:
    """
    Incremental update with optional hash-based optimization.
    
    Args:
        modified_since_iso: ISO timestamp to filter modified workspaces
        include_personal: Include personal workspaces
        enable_hash_optimization: Skip workspaces with unchanged connections (recommended)
        curated_dir: Output directory
        table_name: SQL table name
    """
    changed_ws = modified_workspace_ids(modified_since_iso, include_personal=include_personal)
    if not changed_ws:
        print(f"No modified workspaces since {modified_since_iso}. Nothing to update.")
        return

    # Extract workspace IDs from the workspace dictionaries
    changed_ws_ids = [ws.get("id") for ws in changed_ws if ws.get("id")]
    
    print(f"Found {len(changed_ws_ids)} modified workspaces since {modified_since_iso} (include_personal={include_personal}).")
    
    # Smart filtering: Use stored hash tracker to skip workspaces with recent scans
    workspaces_to_scan = changed_ws_ids
    if enable_hash_optimization and len(changed_ws_ids) > 5:
        print(f"\n🔍 Using hash-based optimization to reduce API calls...")
        try:
            hash_tracker = ConnectionHashTracker(
                config=type('Config', (), {
                    'curated_dir': curated_dir,
                    'tenant_id': TENANT_ID,
                    'client_id': CLIENT_ID,
                    'client_secret': CLIENT_SECRET
                })(),
                running_in_fabric=RUNNING_IN_FABRIC
            )
            
            # Load stored hashes (no API calls - reads from storage)
            stored_hashes = hash_tracker.get_stored_hashes()
            
            # Filter out workspaces that were scanned recently (within last 24 hours)
            # and haven't been modified since their last scan
            from datetime import datetime, timedelta, timezone
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
            
            workspaces_to_scan = []
            skipped_recently_scanned = 0
            
            for ws_id in changed_ws_ids:
                # Check if workspace has stored hash
                if ws_id in stored_hashes:
                    last_scan = stored_hashes[ws_id].get('last_scan_time')
                    if last_scan:
                        try:
                            last_scan_dt = datetime.fromisoformat(last_scan.replace('Z', '+00:00'))
                            # Skip if scanned within last 24 hours
                            if last_scan_dt > cutoff_time:
                                skipped_recently_scanned += 1
                                continue
                        except (ValueError, AttributeError):
                            pass  # Invalid date format, scan anyway
                
                # Include workspace in scan
                workspaces_to_scan.append(ws_id)
            
            reduction_pct = (skipped_recently_scanned / len(changed_ws_ids) * 100) if changed_ws_ids else 0
            
            print(f"✅ Hash optimization complete (no API calls used):")
            print(f"   Skipped {skipped_recently_scanned} workspaces scanned within last 24 hours")
            print(f"   Processing {len(workspaces_to_scan)} workspaces ({reduction_pct:.1f}% reduction)")
            
        except Exception as e:
            print(f"⚠️  Hash optimization failed: {e}")
            print(f"   Falling back to scanning all {len(changed_ws_ids)} workspaces")
            workspaces_to_scan = changed_ws_ids
    
    if not workspaces_to_scan:
        print(f"✅ All workspaces scanned recently. No processing needed.")
        return
    
    print(f"\n📊 Scanning {len(workspaces_to_scan)} workspaces with changes...")

    print(f"\n📊 Scanning {len(workspaces_to_scan)} workspaces with changes...")

    batches = [workspaces_to_scan[i:i+BATCH_SIZE_WORKSPACES] for i in range(0, len(workspaces_to_scan), BATCH_SIZE_WORKSPACES)]
    scan_payloads: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_SCANS) as pool:
        # Convert workspace IDs to batch metadata
        batch_metadata = [[{"id": ws_id, "name": "", "type": "Workspace"} for ws_id in batch] for batch in batches]
        futures = [pool.submit(run_one_batch_incremental, b) for b in batch_metadata]
        for fut in as_completed(futures):
            scan_payloads.append(fut.result())

    print(f"Completed {len(scan_payloads)} incremental scan batches.")

    all_rows = []
    workspace_connections_for_hash = {}  # Track connections for hash update
    
    for payload in scan_payloads:
        sidecar = payload.get("workspace_sidecar", {})
        rows = flatten_scan_payload(payload, sidecar)
        all_rows.extend(rows)
        
        # Group connections by workspace for hash calculation
        if enable_hash_optimization:
            for row in rows:
                ws_id = row.get("workspace_id") if isinstance(row, dict) else getattr(row, "workspace_id", None)
                if ws_id:
                    if ws_id not in workspace_connections_for_hash:
                        workspace_connections_for_hash[ws_id] = []
                    
                    conn_data = {
                        'connector': row.get("connector") if isinstance(row, dict) else getattr(row, "connector", None),
                        'server': row.get("server") if isinstance(row, dict) else getattr(row, "server", None),
                        'database': row.get("database") if isinstance(row, dict) else getattr(row, "database", None),
                        'endpoint': row.get("endpoint") if isinstance(row, dict) else getattr(row, "endpoint", None),
                        'item_id': row.get("item_id") if isinstance(row, dict) else getattr(row, "item_id", None)
                    }
                    workspace_connections_for_hash[ws_id].append(conn_data)

    if not all_rows:
        print("No connection rows produced by incremental scan.")
        # Still update hashes for workspaces with no connections
        if enable_hash_optimization and workspace_connections_for_hash:
            try:
                hash_tracker = ConnectionHashTracker(
                    config=type('Config', (), {'curated_dir': curated_dir})(),
                    running_in_fabric=RUNNING_IN_FABRIC
                )
                workspace_hashes = hash_tracker.calculate_workspace_hashes(workspace_connections_for_hash)
                hash_tracker.save_hashes(workspace_hashes)
                print(f"Updated hashes for {len(workspace_hashes)} workspaces")
            except Exception as e:
                print(f"Warning: Failed to update hashes: {e}")
        return

    # Merge with existing data
    if RUNNING_IN_FABRIC and SPARK_AVAILABLE:
        df_new = spark.createDataFrame(all_rows)
        df_new = (
            df_new.withColumn("connector", F.lower(F.coalesce(F.col("connector"), F.lit("unknown"))))
                  .dropDuplicates(["workspace_id","item_id","connector","server","database","endpoint"])
        )
        try:
            df_existing = spark.read.parquet(curated_dir)
            df_merged = (
                df_existing.unionByName(df_new, allowMissingColumns=True)
                .dropDuplicates(["workspace_id","item_id","connector","server","database","endpoint"])
            )
        except Exception:
            df_merged = df_new
        df_merged.write.mode("overwrite").parquet(curated_dir)
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(f"CREATE TABLE {table_name} USING PARQUET LOCATION '{curated_dir}'")
        row_count = df_merged.count()
    elif PANDAS_AVAILABLE:
        # Convert to DataFrame
        data = [row.asDict() if hasattr(row, 'asDict') else row for row in all_rows]
        df_new = pd.DataFrame(data)
        if 'connector' in df_new.columns:
            df_new['connector'] = df_new['connector'].fillna('unknown').str.lower()
        
        # Try to merge with existing
        output_path = Path(curated_dir)
        parquet_file = output_path / f"{table_name}.parquet"
        try:
            df_existing = pd.read_parquet(parquet_file)
            df_merged = pd.concat([df_existing, df_new], ignore_index=True)
        except FileNotFoundError:
            df_merged = df_new
        
        df_merged.drop_duplicates(
            subset=["workspace_id","item_id","connector","server","database","endpoint"],
            inplace=True
        )
        df_merged.to_parquet(parquet_file, index=False)
        df_merged.to_csv(output_path / f"{table_name}.csv", index=False)
        row_count = len(df_merged)
    else:
        raise RuntimeError("Neither Spark nor pandas available")

    print(f"Incremental update completed. Rows: {row_count} | Curated path: {curated_dir} | SQL table: {table_name}")
    
    # Update connection hashes for next incremental scan
    if enable_hash_optimization and workspace_connections_for_hash:
        print(f"\n💾 Updating connection hashes for {len(workspace_connections_for_hash)} workspaces...")
        try:
            hash_tracker = ConnectionHashTracker(
                config=type('Config', (), {
                    'curated_dir': curated_dir,
                    'tenant_id': TENANT_ID,
                    'client_id': CLIENT_ID,
                    'client_secret': CLIENT_SECRET
                })(),
                running_in_fabric=RUNNING_IN_FABRIC
            )
            
            # Calculate and save hashes
            workspace_hashes = hash_tracker.calculate_workspace_hashes(workspace_connections_for_hash)
            
            # Extract workspace metadata
            workspace_metadata = {}
            for payload in scan_payloads:
                for ws in (payload.get("workspaces") or []):
                    ws_id = ws.get("id")
                    if ws_id:
                        workspace_metadata[ws_id] = {
                            'name': ws.get("name", ""),
                            'type': str(ws.get("type", "")).lower()
                        }
            
            hash_tracker.save_hashes(workspace_hashes, workspace_metadata)
            print(f"✅ Saved connection hashes for future incremental scans")
            
        except Exception as e:
            print(f"⚠️  Warning: Failed to save connection hashes: {e}")
            print(f"   Hash optimization will not be available for next incremental scan")

# --- JSON Directory Scanner (Lakehouse) ---

def scan_json_directory_for_connections(
    json_dir_path: str,
    curated_dir: str = CURATED_DIR,
    table_name: str = "tenant_cloud_connections",
    merge_with_existing: bool = True
) -> None:
    """
    Scans all JSON files in a lakehouse directory and extracts cloud connection information.
    
    Args:
        json_dir_path: Path to directory containing JSON files (e.g., "lakehouse:/Default/Files/scanner/raw")
        curated_dir: Output directory for curated parquet files
        table_name: Name of the SQL table to create/update
        merge_with_existing: If True, merge with existing data; if False, overwrite
    """
    if mssparkutils is None:
        raise RuntimeError("JSON directory scanning requires mssparkutils (Fabric environment)")
    
    # Check if single file mode is enabled
    if JSON_SINGLE_FILE_MODE:
        print(f"Single file mode enabled - processing: {JSON_TARGET_FILE}")
        lakehouse_json_path = _to_lakehouse_path(JSON_TARGET_FILE)
        
        # Create a file info object for the single file
        try:
            file_info = mssparkutils.fs.head(lakehouse_json_path, 0)  # Just to verify file exists
            # Get file size
            import subprocess
            # Since we can't get file info directly, we'll proceed with reading
            json_files = [type('obj', (object,), {'path': lakehouse_json_path, 'size': 0})]  # Dummy size
        except Exception as e:
            print(f"Error: Could not access file {JSON_TARGET_FILE}: {e}")
            return
    else:
        print(f"Scanning JSON files in directory: {json_dir_path}")
        
        # Convert to lakehouse path if needed
        lakehouse_json_path = json_dir_path if json_dir_path.startswith(("file:", "abfss:", "lakehouse:")) else _to_lakehouse_path(json_dir_path)
        
        try:
            # List all JSON files in directory
            files = mssparkutils.fs.ls(lakehouse_json_path)
            json_files = [f for f in files if f.path.endswith('.json')]
            
            if not json_files:
                print(f"No JSON files found in {json_dir_path}")
                return
            
            print(f"Found {len(json_files)} JSON file(s) to process")
        except Exception as e:
            print(f"Error listing directory {json_dir_path}: {e}")
            return
    
    try:
        all_rows = []
        for file_info in json_files:
            try:
                # Read entire JSON file (supports files up to 2GB)
                json_path = file_info.path
                
                # Only check file size if we have it (not in single file mode with dummy size)
                if hasattr(file_info, 'size') and file_info.size > 0:
                    file_size_mb = file_info.size / 1024 / 1024
                    if file_info.size > 2 * 1024 * 1024 * 1024:  # Skip files larger than 2GB
                        print(f"  Skipping {json_path}: file too large ({file_size_mb:.1f} MB)")
                        continue
                    print(f"  Reading {json_path} ({file_size_mb:.1f} MB)...")
                else:
                    print(f"  Reading {json_path}...")
                
                # Use Spark to read JSON file - handles large files efficiently
                # Convert file: URI back to Spark-relative path
                spark_path = json_path.replace("file:/lakehouse/default/", "")
                json_text = spark.read.text(spark_path, wholetext=True).first()[0]
                
                payload = json.loads(json_text)
                
                # Debug: Show payload structure (only if DEBUG_MODE enabled)
                if DEBUG_MODE:
                    print(f"  Payload type: {type(payload).__name__}")
                    if isinstance(payload, dict):
                        print(f"  Payload keys: {list(payload.keys())}")
                    elif isinstance(payload, list):
                        print(f"  Payload list length: {len(payload)}")
                        if payload and isinstance(payload[0], dict):
                            print(f"  First item keys: {list(payload[0].keys())}")
                
                # Handle different JSON structures
                if isinstance(payload, list):
                    # If payload is a list, process each item
                    if DEBUG_MODE:
                        print(f"  Processing list of {len(payload)} item(s)")
                    for idx, item in enumerate(payload):
                        if isinstance(item, dict):
                            # Each item should have workspace_sidecar at its root
                            sidecar = item.get("workspace_sidecar", {})
                            rows = flatten_scan_payload(item, sidecar)
                            all_rows.extend(rows)
                            if DEBUG_MODE:
                                print(f"    Item {idx+1}: extracted {len(rows)} row(s)")
                        else:
                            print(f"    Item {idx+1}: skipping non-dict item: {type(item).__name__}")
                elif isinstance(payload, dict):
                    # If payload is a dict, process it directly
                    sidecar = payload.get("workspace_sidecar", {})
                    rows = flatten_scan_payload(payload, sidecar)
                    all_rows.extend(rows)
                    if DEBUG_MODE:
                        print(f"  Extracted {len(rows)} row(s)")
                else:
                    print(f"  Skipping: unexpected type {type(payload).__name__}")
                    continue
                
                if DEBUG_MODE:
                    print(f"  Completed processing {json_path}")
                
            except json.JSONDecodeError as e:
                print(f"  Warning: Failed to parse JSON {json_path}: {e}")
                continue
                
            except Exception as e:
                print(f"  Warning: Failed to process {json_path}: {e}")
                continue
        
        if not all_rows:
            print("No connection rows extracted from JSON files.")
            return
        
        # Create DataFrame and deduplicate
        df_new = spark.createDataFrame(all_rows)
        df_new = (
            df_new.withColumn("connector", F.lower(F.coalesce(F.col("connector"), F.lit("unknown"))))
                  .dropDuplicates(["workspace_id","item_id","connector","server","database","endpoint"])
        )
        
        # Merge or overwrite
        if merge_with_existing:
            try:
                df_existing = spark.read.parquet(curated_dir)
                df_merged = (
                    df_existing.unionByName(df_new, allowMissingColumns=True)
                    .dropDuplicates(["workspace_id","item_id","connector","server","database","endpoint"])
                )
                print(f"Merged {df_new.count()} new rows with existing data")
            except Exception:
                df_merged = df_new
                print("No existing data found, creating new table")
        else:
            df_merged = df_new
            print("Overwriting existing data")
        
        # Write output
        df_merged.write.mode("overwrite").parquet(curated_dir)
        
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(f"CREATE TABLE {table_name} USING PARQUET LOCATION '{curated_dir}'")
        
        print(f"JSON directory scan completed. Total rows: {df_merged.count()}")
        print(f"Curated path: {curated_dir} | SQL table: {table_name}")
        
    except Exception as e:
        print(f"Error scanning JSON directory: {e}")
        raise


# --- API Health Check: Detect Contention Before Large Scans ---

def check_scanner_api_health(
    test_duration_minutes: int = 1,
    test_calls: int = 2
) -> dict:
    """
    Test Scanner API availability and detect if other processes are using it.
    
    Run this BEFORE starting large scans to understand current API contention.
    
    How it works:
    1. Makes small test API calls (getting workspace list) over 1 minute
    2. Monitors for 429 rate limit errors
    3. Estimates if others are using the API based on error patterns
    4. Recommends optimal MAX_PARALLEL_SCANS setting
    
    COST: Only 2-3 API calls (0.4-0.6% of hourly quota) - minimal impact for shared tenants
    
    Args:
        test_duration_minutes: How long to run the test (default: 1 minute for shared tenants)
        test_calls: Number of test calls to make (default: 2 for minimal cost)
                    Quick (shared tenant): 2 calls - DEFAULT
                    Standard: 4 calls
                    Thorough: 6 calls
    
    Returns:
        dict with:
        - status: 'clear', 'light', 'moderate', 'heavy'
        - rate_limit_errors: Count of 429 errors encountered
        - recommended_max_parallel: Suggested MAX_PARALLEL_SCANS value
        - estimated_other_usage: Estimated API calls/hour by other processes
        - safe_to_proceed: Boolean whether to proceed with large scan
    
    Example usage:
        # Quick check (2 API calls, 1 minute) - DEFAULT for shared tenants
        health = check_scanner_api_health()
        
        # Standard check (4 API calls, 2 minutes)
        health = check_scanner_api_health(test_duration_minutes=2, test_calls=4)
        
        # Thorough check (6 API calls, 3 minutes)
        health = check_scanner_api_health(test_duration_minutes=3, test_calls=6)
        
        if health['safe_to_proceed']:
            run_cloud_connection_scan(enable_full_scan_chunked=True)
        else:
            print("Heavy contention - reschedule for off-hours")
    """
    print("="*70)
    print("🔍 SCANNER API HEALTH CHECK")
    print("="*70)
    print(f"Running {test_calls} test API calls over {test_duration_minutes} minutes...")
    print(f"Cost: ~{test_calls + 1} API calls (~{((test_calls + 1) / 500 * 100):.1f}% of hourly quota)")
    print("Optimized for shared tenants - minimal API impact")
    print()
    
    token = get_token()
    base_url = f"https://api.powerbi.com/v1.0/myorg"
    headers = {"Authorization": f"Bearer {token}"}
    
    # Calculate interval between test calls
    interval_seconds = (test_duration_minutes * 60) / test_calls
    
    errors_429 = 0
    errors_other = 0
    successful_calls = 0
    start_time = time.time()
    
    for i in range(test_calls):
        print(f"Test call {i+1}/{test_calls}...", end=" ")
        
        try:
            # Make a lightweight API call (get modified workspaces, small result)
            # This uses the Scanner API without actually scanning
            url = f"{base_url}/admin/workspaces/modified"
            params = {
                "modifiedSince": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
                "$top": 5  # Only get 5 workspaces, minimal data
            }
            
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            
            if resp.status_code == 429:
                errors_429 += 1
                print("❌ Rate limited (429)")
                # Wait before next call
                time.sleep(60)
            elif resp.status_code == 200:
                successful_calls += 1
                print("✅ Success")
            else:
                errors_other += 1
                print(f"⚠️  Status {resp.status_code}")
            
        except requests.RequestException as e:
            errors_other += 1
            print(f"⚠️  Error: {e}")
        
        # Wait before next test call (unless it's the last one)
        if i < test_calls - 1:
            time.sleep(interval_seconds)
    
    elapsed_minutes = (time.time() - start_time) / 60
    
    # Analysis
    print()
    print("="*70)
    print("📊 HEALTH CHECK RESULTS")
    print("="*70)
    print(f"Duration: {elapsed_minutes:.1f} minutes")
    print(f"Successful calls: {successful_calls}/{test_calls}")
    print(f"Rate limit errors (429): {errors_429}/{test_calls}")
    print(f"Other errors: {errors_other}/{test_calls}")
    print()
    
    # Determine status and recommendations
    rate_limit_percentage = (errors_429 / test_calls) * 100 if test_calls > 0 else 0
    
    if errors_429 == 0:
        status = 'clear'
        recommended_max_parallel = 3  # Recommend 3 when clear (faster but still safe)
        estimated_other_usage = 0
        safe_to_proceed = True
        message = "✅ NO API CONTENTION DETECTED"
        details = [
            "The Scanner API appears to be available with no other users",
            "Safe to increase from default MAX_PARALLEL_SCANS=1 to 3-5 for faster completion",
            "Can increase to 8 if you want maximum speed (96% quota usage)"
        ]
        
    elif errors_429 == 1 and test_calls >= 4:  # 25% or less rate limited (with 4+ tests)
        status = 'light'
        recommended_max_parallel = 1  # Stay at default
        estimated_other_usage = 100  # ~100 calls/hour estimated
        safe_to_proceed = True
        message = "⚠️  LIGHT API CONTENTION DETECTED"
        details = [
            f"{rate_limit_percentage:.0f}% of test calls were rate limited",
            "Some other process is using the Scanner API (estimated ~100 calls/hour)",
            f"RECOMMENDED: Keep MAX_PARALLEL_SCANS={recommended_max_parallel} (default)",
            "This will use ~120 calls/hour (24% quota), leaving plenty for others"
        ]
        
    elif rate_limit_percentage >= 50:  # 50%+ rate limited
        status = 'heavy'
        recommended_max_parallel = 1
        estimated_other_usage = 400  # ~400+ calls/hour estimated
        safe_to_proceed = False
        message = "❌ HEAVY API CONTENTION DETECTED"
        details = [
            f"{rate_limit_percentage:.0f}% of test calls were rate limited",
            "Scanner API is heavily utilized by other processes",
            "Estimated other usage: ~400+ calls/hour",
            "NOT RECOMMENDED to proceed now - you would be severely throttled",
            "OPTIONS:",
            "  1. Wait for off-hours (nights/weekends)",
            "  2. Coordinate with tenant admins to schedule exclusive time",
            f"  3. If urgent: Set MAX_PARALLEL_SCANS={recommended_max_parallel} (very slow)"
        ]
        
    else:  # Between 25-50% rate limited
        status = 'moderate'
        recommended_max_parallel = 2
        estimated_other_usage = 250  # ~250 calls/hour estimated
        safe_to_proceed = True
        message = "⚠️  MODERATE API CONTENTION DETECTED"
        details = [
            f"{rate_limit_percentage:.0f}% of test calls were rate limited",
            "Multiple processes appear to be using the Scanner API",
            "Estimated other usage: ~250 calls/hour",
            f"RECOMMENDED: Set MAX_PARALLEL_SCANS={recommended_max_parallel}",
            "This will use ~250 calls/hour, sharing quota 50/50",
            "OR schedule your scan for off-hours/weekends"
        ]
    
    print(message)
    print("-" * 70)
    for detail in details:
        print(f"  {detail}")
    print()
    
    # Summary
    result = {
        'status': status,
        'rate_limit_errors': errors_429,
        'rate_limit_percentage': rate_limit_percentage,
        'successful_calls': successful_calls,
        'recommended_max_parallel': recommended_max_parallel,
        'estimated_other_usage': estimated_other_usage,
        'safe_to_proceed': safe_to_proceed,
        'message': message,
        'details': details
    }
    
    # Practical next steps
    print("🎯 NEXT STEPS:")
    print("-" * 70)
    if safe_to_proceed:
        if status == 'clear':
            print("  1. 🎉 Great news! You can increase MAX_PARALLEL_SCANS for faster completion")
            print("  2. Edit line 135 in the script:")
            print("     Change: MAX_PARALLEL_SCANS = 1")
            print("     To:     MAX_PARALLEL_SCANS = 3  (or 5 for even faster)")
            print("  3. This will reduce scan time from ~7 days to ~2 days")
        else:
            print(f"  1. Edit line 107 in the script:")
            print(f"     Change: MAX_PARALLEL_SCANS = 5")
            print(f"     To:     MAX_PARALLEL_SCANS = {recommended_max_parallel}")
            print("  2. Save the file")
            print("  3. Proceed with your scan")
            print("  4. Monitor for continued 429 errors")
    else:
        print("  1. PAUSE - Do not run large scan now")
        print("  2. Check tenant calendar for scheduled maintenance/reports")
        print("  3. Contact tenant admins to identify other API users")
        print("  4. Schedule your scan for off-hours:")
        print("     - Weeknights: 8 PM - 6 AM")
        print("     - Weekends: Friday 6 PM - Monday 6 AM")
        print("  5. Re-run this health check before starting")
    
    print("="*70)
    print()
    
    return result


# --- Orchestrator: Choose Any Combination of Features ---

def run_cloud_connection_scan(
    enable_full_scan: bool = False,
    enable_full_scan_chunked: bool = False,  # NEW: For large tenants with rate limit management
    enable_incremental_scan: bool = True,
    enable_json_directory_scan: bool = False,
    enable_scan_id_retrieval: bool = False,
    enable_directionality_analysis: bool = False,  # NEW: Analyze inbound/outbound connections
    enable_hash_optimization: bool = True,  # NEW: Skip unchanged workspaces in incremental scans
    group_by_capacity: bool = False,  # NEW: Group workspaces by capacity for better organization
    include_personal: bool = True,
    max_batches_per_hour: int = 250,  # Conservative default - leaves 250 calls/hour for other users
    incremental_days_back: float = None,
    incremental_hours_back: float = None,
    json_directory_path: str = None,
    json_merge_with_existing: bool = True,
    scan_id: str = None,
    scan_id_merge_with_existing: bool = True,
    activity_days_back: int = 30,  # NEW: Days of activity events to analyze
    directionality_analysis_output_dir: str = None,  # NEW: Output directory for directionality analysis results
    parallel_capacities: int = 1,  # NEW: Number of capacities to scan in parallel (Phase 3)
    max_calls_per_hour: int = 450,  # NEW: Total API calls per hour for parallel mode (Phase 3)
    capacity_filter: List[str] = None,  # NEW: Only scan these capacity IDs (Phase 3)
    exclude_capacities: List[str] = None,  # NEW: Skip these capacity IDs (Phase 3)
    capacity_priority: List[str] = None,  # NEW: Process capacities in this order (Phase 3)
    curated_dir: str = CURATED_DIR,
    table_name: str = "tenant_cloud_connections"
) -> None:
    """
    Orchestrates cloud connection scanning with configurable features.
    
    Args:
        enable_full_scan: Run full tenant scan (baseline) - may hit rate limits on large tenants
        enable_full_scan_chunked: Run full tenant scan with automatic rate limit management (recommended for 10K+ workspaces)
        enable_incremental_scan: Run incremental scan for modified workspaces
        enable_json_directory_scan: Scan JSON files in a lakehouse directory
        enable_scan_id_retrieval: Retrieve results from a previous scan using scan ID
        enable_directionality_analysis: Analyze connection directionality (inbound vs outbound)
        enable_hash_optimization: Skip workspaces with unchanged connections in incremental scans (80-90% faster)
        group_by_capacity: Group and process workspaces by capacity (better organization, foundation for parallel scanning)
        include_personal: Include personal workspaces in API scans
        max_batches_per_hour: Max API calls per hour for chunked scans (default 450 for safety margin under 500 limit)
        incremental_days_back: Number of days to look back for incremental scan (can be fractional, e.g., 0.5 = 12 hours)
        incremental_hours_back: Number of hours to look back for incremental scan (alternative to days, takes precedence)
        json_directory_path: Path to directory containing JSON files (required if enable_json_directory_scan=True)
        json_merge_with_existing: Merge JSON scan results with existing data
        scan_id: Scan ID to retrieve (required if enable_scan_id_retrieval=True)
        scan_id_merge_with_existing: Merge scan ID results with existing data
        activity_days_back: Days of activity events to analyze for inbound connections (default: 30)
        directionality_analysis_output_dir: Output directory for analysis results (Fabric: uses lakehouse tables, Local: defaults to ./scanner_output/analysis)
        parallel_capacities: Number of capacities to scan in parallel (1=sequential, 2+=parallel) - Phase 3
        max_calls_per_hour: Total API calls per hour for parallel mode (default: 450) - Phase 3
        capacity_filter: Only scan these capacity IDs (e.g., ['cap-123', 'cap-456']) - Phase 3
        exclude_capacities: Skip these capacity IDs (e.g., ['shared']) - Phase 3
        capacity_priority: Process capacities in this order (rest follow) - Phase 3
        curated_dir: Output directory for curated data
        table_name: SQL table name for results
    """
    # Calculate time window
    if incremental_hours_back is not None:
        lookback_hours = incremental_hours_back
        time_display = f"{incremental_hours_back} hour(s)"
    elif incremental_days_back is not None:
        lookback_hours = incremental_days_back * 24
        time_display = f"{incremental_days_back} day(s)"
    else:
        lookback_hours = 24  # Default to 1 day
        time_display = "1 day (default)"
    
    # Initialize authentication if not already done (for programmatic usage)
    if HEADERS is None:
        initialize_authentication()
    
    print("="*80)
    print("Cloud Connection Scanner - Feature Selection")
    print("="*80)
    print(f"Full Tenant Scan:           {'ENABLED' if enable_full_scan else 'DISABLED'}")
    print(f"Full Tenant Scan (Chunked): {'ENABLED' if enable_full_scan_chunked else 'DISABLED'}")
    print(f"Incremental Scan:           {'ENABLED' if enable_incremental_scan else 'DISABLED'}")
    print(f"Hash Optimization:          {'ENABLED' if enable_hash_optimization and enable_incremental_scan else 'DISABLED'}")
    print(f"Capacity Grouping:          {'ENABLED' if group_by_capacity else 'DISABLED'}")
    print(f"Parallel Capacities:        {parallel_capacities} {'(Sequential)' if parallel_capacities == 1 else f'(Parallel - Phase 3)'}")
    if parallel_capacities > 1:
        print(f"  Max Calls/Hour:           {max_calls_per_hour}")
        print(f"  Capacity Filter:          {', '.join(capacity_filter) if capacity_filter else 'None'}")
        print(f"  Exclude Capacities:       {', '.join(exclude_capacities) if exclude_capacities else 'None'}")
        print(f"  Capacity Priority:        {', '.join(capacity_priority) if capacity_priority else 'None'}")
    print(f"JSON Directory Scan:        {'ENABLED' if enable_json_directory_scan else 'DISABLED'}")
    print(f"Scan ID Retrieval:          {'ENABLED' if enable_scan_id_retrieval else 'DISABLED'}")
    print(f"Directionality Analysis:    {'ENABLED' if enable_directionality_analysis else 'DISABLED'}")
    print(f"Include Personal WS:        {include_personal}")
    print(f"Incremental Lookback:       {time_display}")
    print(f"Activity Analysis Days:     {activity_days_back if enable_directionality_analysis else 'N/A'}")
    print(f"Max Batches/Hour:           {max_batches_per_hour}")
    print(f"JSON Directory Path:        {json_directory_path or 'Not specified'}")
    print(f"Scan ID:                    {scan_id or 'Not specified'}")
    print(f"Output Table:               {table_name}")
    print("="*80)
    
    features_enabled = sum([enable_full_scan, enable_full_scan_chunked, enable_incremental_scan, 
                           enable_json_directory_scan, enable_scan_id_retrieval, enable_directionality_analysis])
    if features_enabled == 0:
        print("\nWARNING: No features enabled. Nothing to do.")
        return
    
    # Feature 1: Full Tenant Scan
    if enable_full_scan:
        print("\n[1/5] Running FULL TENANT SCAN...")
        try:
            full_tenant_scan(
                include_personal=include_personal,
                curated_dir=curated_dir,
                table_name=table_name,
                group_by_capacity=group_by_capacity,
                parallel_capacities=parallel_capacities,
                max_calls_per_hour=max_calls_per_hour,
                capacity_filter=capacity_filter,
                exclude_capacities=exclude_capacities,
                capacity_priority=capacity_priority
            )
            print("✓ Full tenant scan completed successfully")
        except Exception as e:
            print(f"✗ Full tenant scan failed: {e}")
            raise
    
    # Feature 1b: Full Tenant Scan (Chunked with Rate Limit Management)
    if enable_full_scan_chunked:
        print("\n[1b/5] Running FULL TENANT SCAN (CHUNKED - Rate Limit Safe)...")
        try:
            full_tenant_scan_chunked(
                include_personal=include_personal,
                max_batches_per_hour=max_batches_per_hour,
                curated_dir=curated_dir,
                table_name=table_name,
                group_by_capacity=group_by_capacity,
                parallel_capacities=parallel_capacities,
                capacity_filter=capacity_filter,
                exclude_capacities=exclude_capacities,
                capacity_priority=capacity_priority
            )
            print("✓ Chunked full tenant scan completed successfully")
        except Exception as e:
            print(f"✗ Chunked full tenant scan failed: {e}")
            raise
    
    # Feature 2: Incremental Scan
    if enable_incremental_scan:
        print("\n[2/5] Running INCREMENTAL SCAN...")
        try:
            modified_since = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
            incremental_update(
                modified_since_iso=modified_since,
                include_personal=include_personal,
                enable_hash_optimization=enable_hash_optimization,
                curated_dir=curated_dir,
                table_name=table_name
            )
            print("✓ Incremental scan completed successfully")
        except Exception as e:
            print(f"✗ Incremental scan failed: {e}")
            raise
    
    # Feature 3: JSON Directory Scan
    if enable_json_directory_scan:
        print("\n[3/4] Running JSON DIRECTORY SCAN...")
        if not json_directory_path:
            raise ValueError("json_directory_path is required when enable_json_directory_scan=True")
        
        try:
            scan_json_directory_for_connections(
                json_dir_path=json_directory_path,
                curated_dir=curated_dir,
                table_name=table_name,
                merge_with_existing=json_merge_with_existing
            )
            print("✓ JSON directory scan completed successfully")
        except Exception as e:
            print(f"✗ JSON directory scan failed: {e}")
            raise
    
    # Feature 4: Scan ID Retrieval
    if enable_scan_id_retrieval:
        print("\n[4/4] Running SCAN ID RETRIEVAL...")
        if not scan_id:
            raise ValueError("scan_id is required when enable_scan_id_retrieval=True")
        
        try:
            get_scan_result_by_id(
                scan_id=scan_id,
                curated_dir=curated_dir,
                table_name=table_name,
                merge_with_existing=scan_id_merge_with_existing
            )
            print("✓ Scan ID retrieval completed successfully")
        except Exception as e:
            print(f"✗ Scan ID retrieval failed: {e}")
            raise
    
    print("\n" + "="*80)
    print("SCAN COMPLETE - All enabled features executed successfully")
    print("="*80)
    
    # Feature 5: Connection Directionality Analysis
    if enable_directionality_analysis:
        print("\n" + "="*80)
        print("[ANALYSIS] Running Connection Directionality Analysis...")
        print("="*80)
        try:
            # Determine output directory
            if directionality_analysis_output_dir:
                analysis_output = directionality_analysis_output_dir
            elif RUNNING_IN_FABRIC:
                analysis_output = None  # Save to lakehouse tables
            else:
                analysis_output = "./scanner_output/analysis"  # Local default
            
            analyze_connection_directionality(
                scanner_results_path=None,  # Reads from lakehouse table or curated_dir/table_name
                include_activity_logs=True,
                activity_days_back=activity_days_back,
                output_dir=analysis_output
            )
            print("✓ Directionality analysis completed successfully")
        except Exception as e:
            print(f"✗ Directionality analysis failed: {e}")
            # Don't raise - this is supplementary analysis


# --- CLI Wrapper ---

def main():
    """
    Command-line interface for Fabric Scanner Cloud Connections.
    
    Usage Examples:
        # Full scan (baseline)
        python fabric_scanner_cloud_connections.py --full-scan
        
        # Full scan with rate limiting (safe for large shared tenants)
        python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants
        
        # Incremental scan (last 24 hours, with hash optimization)
        python fabric_scanner_cloud_connections.py --incremental
        
        # Incremental scan (last 7 days)
        python fabric_scanner_cloud_connections.py --incremental --days 7
        
        # Incremental scan (last 6 hours)
        python fabric_scanner_cloud_connections.py --incremental --hours 6
        
        # Incremental without hash optimization
        python fabric_scanner_cloud_connections.py --incremental --no-hash-optimization
        
        # Get scan result by ID
        python fabric_scanner_cloud_connections.py --scan-id e7d03602-4873-4760-b37e-1563ef5358e3
        
        # Health check
        python fabric_scanner_cloud_connections.py --health-check
        
        # Analyze connection directionality
        python fabric_scanner_cloud_connections.py --analyze-direction --with-activity --activity-days 30
        
        # Process JSON directory
        python fabric_scanner_cloud_connections.py --json-dir Files/scanner/raw/full
        
        # Exclude personal workspaces
        python fabric_scanner_cloud_connections.py --full-scan --no-personal
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Microsoft Fabric Scanner API - Cloud Connections Inventory',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Full scan (baseline):
    python %(prog)s --full-scan
  
  Full scan with rate limiting (safe for large shared tenants):
    python %(prog)s --full-scan --large-shared-tenants
  
  Incremental scan (last 24 hours):
    python %(prog)s --incremental
  
  Incremental scan (last 7 days):
    python %(prog)s --incremental --days 7
  
  Health check:
    python %(prog)s --health-check
  
  Get specific scan result:
    python %(prog)s --scan-id YOUR_SCAN_ID
        """
    )
    
    # Scan mode options (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        '--full-scan',
        action='store_true',
        help='Run full tenant scan (all workspaces)'
    )
    mode_group.add_argument(
        '--incremental',
        action='store_true',
        help='Run incremental scan (modified workspaces only)'
    )
    mode_group.add_argument(
        '--scan-id',
        type=str,
        metavar='SCAN_ID',
        help='Retrieve results from a specific scan ID (UUID)'
    )
    mode_group.add_argument(
        '--health-check',
        action='store_true',
        help='Check Scanner API health and quota availability'
    )
    mode_group.add_argument(
        '--analyze-direction',
        action='store_true',
        help='Analyze connection directionality (inbound vs outbound)'
    )
    mode_group.add_argument(
        '--json-dir',
        type=str,
        metavar='PATH',
        help='Process JSON files from directory (e.g., Files/scanner/raw)'
    )
    
    # Full scan options
    full_group = parser.add_argument_group('full scan options')
    full_group.add_argument(
        '--large-shared-tenants',
        action='store_true',
        help='Use rate-limited chunked mode for large shared tenants (processes in hourly chunks, respects 500 calls/hour limit)'
    )
    full_group.add_argument(
        '--group-by-capacity',
        action='store_true',
        help='Group and process workspaces by capacity for better organization (recommended for multi-capacity tenants)'
    )
    full_group.add_argument(
        '--max-batches-per-hour',
        type=int,
        default=450,
        metavar='N',
        help='Max API calls per hour in chunked mode (default: 450, leaves 10%% buffer)'
    )
    full_group.add_argument(
        '--parallel-capacities',
        type=int,
        default=1,
        metavar='N',
        help='Number of capacities to scan in parallel (1=sequential, 2-3=recommended, default: 1) - Phase 3'
    )
    full_group.add_argument(
        '--max-calls-per-hour',
        type=int,
        default=450,
        metavar='N',
        help='Total API calls per hour for parallel mode (default: 450) - Phase 3'
    )
    full_group.add_argument(
        '--capacity-filter',
        type=str,
        metavar='IDS',
        help='Comma-separated capacity IDs to scan (e.g., cap-123,cap-456) - Phase 3'
    )
    full_group.add_argument(
        '--exclude-capacities',
        type=str,
        metavar='IDS',
        help='Comma-separated capacity IDs to exclude (e.g., shared,cap-789) - Phase 3'
    )
    full_group.add_argument(
        '--capacity-priority',
        type=str,
        metavar='IDS',
        help='Comma-separated capacity IDs to process first (e.g., cap-123,cap-456) - Phase 3'
    )
    
    # Incremental scan options
    incr_group = parser.add_argument_group('incremental scan options')
    incr_group.add_argument(
        '--days',
        type=int,
        metavar='N',
        help='Days to look back for modified workspaces (default: 1)'
    )
    incr_group.add_argument(
        '--hours',
        type=float,
        metavar='N',
        help='Hours to look back (overrides --days if specified)'
    )
    incr_group.add_argument(
        '--no-hash-optimization',
        action='store_true',
        help='Disable hash optimization (scans all modified workspaces, not just changed ones)'
    )
    
    # Direction analysis options
    dir_group = parser.add_argument_group('direction analysis options')
    dir_group.add_argument(
        '--with-activity',
        action='store_true',
        help='Include Activity Event API analysis for inbound connections'
    )
    dir_group.add_argument(
        '--activity-days',
        type=int,
        default=30,
        metavar='N',
        help='Days of activity history to analyze (default: 30)'
    )
    dir_group.add_argument(
        '--output-dir',
        type=str,
        metavar='PATH',
        help='Output directory for analysis results'
    )
    
    # General options
    parser.add_argument(
        '--no-personal',
        action='store_true',
        help='Exclude personal workspaces from scan'
    )
    parser.add_argument(
        '--table-name',
        type=str,
        default='tenant_cloud_connections',
        metavar='NAME',
        help='SQL table name for results (default: tenant_cloud_connections)'
    )
    parser.add_argument(
        '--curated-dir',
        type=str,
        default=CURATED_DIR,
        metavar='PATH',
        help=f'Output directory for curated data (default: {CURATED_DIR})'
    )
    parser.add_argument(
        '--no-merge',
        action='store_true',
        help='Overwrite existing data instead of merging'
    )
    
    # Lakehouse upload options (for local execution)
    lakehouse_group = parser.add_argument_group('lakehouse upload options (local execution)')
    lakehouse_group.add_argument(
        '--upload-to-lakehouse',
        action='store_true',
        help='Upload results to Fabric lakehouse when running locally'
    )
    lakehouse_group.add_argument(
        '--lakehouse-workspace-id',
        type=str,
        metavar='WORKSPACE_ID',
        help='Workspace ID containing the target lakehouse'
    )
    lakehouse_group.add_argument(
        '--lakehouse-id',
        type=str,
        metavar='LAKEHOUSE_ID',
        help='Lakehouse ID to upload results to'
    )
    lakehouse_group.add_argument(
        '--lakehouse-upload-path',
        type=str,
        default='Files/scanner',
        metavar='PATH',
        help='Path within lakehouse to upload files (default: Files/scanner)'
    )
    
    # Configuration and checkpoint options
    config_group = parser.add_argument_group('configuration & checkpoint options')
    config_group.add_argument(
        '--config',
        type=str,
        metavar='PATH',
        help='Path to configuration file (YAML or JSON). Example: scanner_config.yaml'
    )
    config_group.add_argument(
        '--enable-checkpoints',
        action='store_true',
        help='Enable checkpoint/resume for long-running scans (overrides config file)'
    )
    config_group.add_argument(
        '--disable-checkpoints',
        action='store_true',
        help='Disable checkpoint/resume (overrides config file)'
    )
    config_group.add_argument(
        '--lakehouse-upload-debug',
        action='store_true',
        help='Show lakehouse upload configuration details for debugging'
    )
    config_group.add_argument(
        '--checkpoint-storage',
        type=str,
        choices=['json', 'lakehouse'],
        metavar='TYPE',
        help='Checkpoint storage type: json (local files) or lakehouse (Fabric storage)'
    )
    config_group.add_argument(
        '--clear-checkpoint',
        type=str,
        metavar='CHECKPOINT_ID',
        help='Clear a specific checkpoint file and exit (utility command)'
    )
    
    args = parser.parse_args()
    
    # Load configuration file if provided
    if args.config:
        print(f"📄 Loading configuration from: {args.config}")
        config = load_config_file(args.config)
        if config:
            apply_config(config)
            print(f"✅ Configuration applied successfully")
    
    # Handle checkpoint clearing utility command
    if args.clear_checkpoint:
        print(f"🗑️  Clearing checkpoint: {args.clear_checkpoint}")
        checkpoint_storage = args.checkpoint_storage or CHECKPOINT_STORAGE
        mgr = CheckpointManager(args.clear_checkpoint, storage=checkpoint_storage)
        mgr.clear_checkpoint()
        print(f"✅ Checkpoint cleared successfully")
        return 0
    
    # Override checkpoint settings from command line
    enable_checkpointing_override = None
    if args.enable_checkpoints:
        enable_checkpointing_override = True
    elif args.disable_checkpoints:
        enable_checkpointing_override = False
    
    checkpoint_storage_override = args.checkpoint_storage
    
    # Override lakehouse upload settings from command line
    global UPLOAD_TO_LAKEHOUSE, LAKEHOUSE_WORKSPACE_ID, LAKEHOUSE_ID, LAKEHOUSE_UPLOAD_PATH
    if args.upload_to_lakehouse:
        UPLOAD_TO_LAKEHOUSE = True
    if args.lakehouse_workspace_id:
        LAKEHOUSE_WORKSPACE_ID = args.lakehouse_workspace_id
    if args.lakehouse_id:
        LAKEHOUSE_ID = args.lakehouse_id
    if args.lakehouse_upload_path:
        LAKEHOUSE_UPLOAD_PATH = args.lakehouse_upload_path
    
    # Initialize authentication (only when running as script, not during test imports)
    initialize_authentication()
    
    # Execute based on mode
    try:
        if args.full_scan:
            print("="*80)
            print("FULL TENANT SCAN")
            print("="*80)
            include_personal = not args.no_personal
            
            # Parse Phase 3 parallel capacity parameters
            capacity_filter = args.capacity_filter.split(',') if args.capacity_filter else None
            exclude_capacities = args.exclude_capacities.split(',') if args.exclude_capacities else None
            capacity_priority = args.capacity_priority.split(',') if args.capacity_priority else None
            
            # Display Phase 3 settings if parallel mode enabled
            if args.parallel_capacities > 1:
                print(f"Phase 3: Parallel Capacity Scanning ENABLED")
                print(f"  Parallel capacities: {args.parallel_capacities}")
                print(f"  Max calls/hour: {args.max_calls_per_hour}")
                if capacity_filter:
                    print(f"  Capacity filter: {', '.join(capacity_filter)}")
                if exclude_capacities:
                    print(f"  Exclude capacities: {', '.join(exclude_capacities)}")
                if capacity_priority:
                    print(f"  Capacity priority: {', '.join(capacity_priority)}")
            
            if args.large_shared_tenants:
                print(f"Mode: Large Shared Tenant (rate-limited, chunked processing)")
                print(f"Max batches/hour: {args.max_batches_per_hour}")
                if args.group_by_capacity:
                    print(f"Capacity grouping: ENABLED")
                full_tenant_scan_chunked(
                    include_personal=include_personal,
                    max_batches_per_hour=args.max_batches_per_hour,
                    curated_dir=args.curated_dir,
                    table_name=args.table_name,
                    enable_checkpointing=enable_checkpointing_override,
                    checkpoint_storage=checkpoint_storage_override,
                    group_by_capacity=args.group_by_capacity,
                    parallel_capacities=args.parallel_capacities,
                    capacity_filter=capacity_filter,
                    exclude_capacities=exclude_capacities,
                    capacity_priority=capacity_priority
                )
            else:
                print(f"Mode: Standard (MAX_PARALLEL_SCANS={MAX_PARALLEL_SCANS})")
                if args.group_by_capacity:
                    print(f"Capacity grouping: ENABLED")
                full_tenant_scan(
                    include_personal=include_personal,
                    curated_dir=args.curated_dir,
                    table_name=args.table_name,
                    group_by_capacity=args.group_by_capacity,
                    parallel_capacities=args.parallel_capacities,
                    max_calls_per_hour=args.max_calls_per_hour,
                    capacity_filter=capacity_filter,
                    exclude_capacities=exclude_capacities,
                    capacity_priority=capacity_priority
                )
        
        elif args.incremental:
            print("="*80)
            print("INCREMENTAL SCAN")
            print("="*80)
            
            # Calculate modified_since timestamp
            if args.hours:
                hours_back = args.hours
                modified_since = datetime.now(timezone.utc) - timedelta(hours=hours_back)
                print(f"Looking back: {hours_back} hours")
            else:
                days_back = args.days if args.days else 1
                modified_since = datetime.now(timezone.utc) - timedelta(days=days_back)
                print(f"Looking back: {days_back} days")
            
            # Format timestamp to match Microsoft's example: 2020-10-02T05:51:30.0000000Z (7 decimal places)
            modified_since_iso = modified_since.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")
            enable_hash = not args.no_hash_optimization
            
            print(f"Hash optimization: {'enabled' if enable_hash else 'disabled'}")
            print(f"Modified since: {modified_since_iso}")
            
            # Show lakehouse upload configuration if debug flag enabled (--debug includes this automatically)
            if args.lakehouse_upload_debug or args.debug:
                if UPLOAD_TO_LAKEHOUSE:
                    if LAKEHOUSE_WORKSPACE_ID and LAKEHOUSE_ID:
                        print(f"\n[DEBUG] Lakehouse upload: ENABLED")
                        print(f"[DEBUG]   Workspace ID: {LAKEHOUSE_WORKSPACE_ID}")
                        print(f"[DEBUG]   Lakehouse ID: {LAKEHOUSE_ID}")
                        print(f"[DEBUG]   Upload path: {LAKEHOUSE_UPLOAD_PATH}")
                    else:
                        print(f"\n[DEBUG] ⚠️  Lakehouse upload configured but missing workspace_id or lakehouse_id")
                        print(f"[DEBUG]   UPLOAD_TO_LAKEHOUSE: {UPLOAD_TO_LAKEHOUSE}")
                        print(f"[DEBUG]   LAKEHOUSE_WORKSPACE_ID: {LAKEHOUSE_WORKSPACE_ID or 'NOT SET'}")
                        print(f"[DEBUG]   LAKEHOUSE_ID: {LAKEHOUSE_ID or 'NOT SET'}")
                else:
                    print(f"\n[DEBUG] Lakehouse upload: DISABLED")
                print()  # Blank line after debug output
            
            incremental_update(
                modified_since_iso=modified_since_iso,
                include_personal=not args.no_personal,
                enable_hash_optimization=enable_hash,
                curated_dir=args.curated_dir,
                table_name=args.table_name
            )
        
        elif args.scan_id:
            print("="*80)
            print("RETRIEVE SCAN RESULT BY ID")
            print("="*80)
            print(f"Scan ID: {args.scan_id}")
            
            get_scan_result_by_id(
                scan_id=args.scan_id,
                curated_dir=args.curated_dir,
                table_name=args.table_name,
                merge_with_existing=not args.no_merge
            )
        
        elif args.health_check:
            print("="*80)
            print("SCANNER API HEALTH CHECK")
            print("="*80)
            
            try:
                health = check_scanner_api_health()
                
                print(f"\nStatus: {health['status'].upper()}")
                print(f"Safe to proceed: {'✅ YES' if health['safe_to_proceed'] else '⚠️  NO'}")
                print(f"\nDetails:")
                print(f"  Active scans: {health['active_scans']}")
                print(f"  Recommended MAX_PARALLEL_SCANS: {health['recommended_max_parallel']}")
                
                if health['status'] == 'heavy':
                    print(f"\n⚠️  WARNING: Heavy API usage detected!")
                    print(f"  Wait {health.get('cooldown_minutes', 10)} minutes before scanning")
                elif health['status'] == 'moderate':
                    print(f"\n⚠️  Moderate API usage - proceed with caution")
                    print(f"  Recommended: MAX_PARALLEL_SCANS=1-2")
                else:
                    print(f"\n✅ API clear - safe to scan")
                    print(f"  You can increase MAX_PARALLEL_SCANS to 3-5 if needed")
                
            except NameError:
                print("ERROR: check_scanner_api_health() function not found in this version")
                print("This feature may not be available yet.")
        
        elif args.analyze_direction:
            print("="*80)
            print("CONNECTION DIRECTIONALITY ANALYSIS")
            print("="*80)
            
            analyze_connection_directionality(
                include_activity_logs=args.with_activity,
                activity_days_back=args.activity_days,
                output_dir=args.output_dir
            )
        
        elif args.json_dir:
            print("="*80)
            print("PROCESS JSON DIRECTORY")
            print("="*80)
            print(f"Directory: {args.json_dir}")
            
            run_cloud_connection_scan(
                enable_full_scan=False,
                enable_incremental_scan=False,
                enable_json_directory_scan=True,
                json_directory_path=args.json_dir,
                json_merge_with_existing=not args.no_merge,
                curated_dir=args.curated_dir,
                table_name=args.table_name
            )
        
        print("\n" + "="*80)
        print("✅ OPERATION COMPLETED SUCCESSFULLY")
        print("="*80)
        
        # Show final API stats if any calls were made
        stats = get_api_call_stats()
        if stats['calls'] > 0:
            print_api_call_stats()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user")
        stats = get_api_call_stats()
        if stats['calls'] > 0:
            print_api_call_stats()
        return 1
    except Exception as e:
        print(f"\n\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
