# Large Tenant Optimization Guide (247k Workspaces)

> **Important Note**: This guide focuses on **chunked scan mode** (`--large-shared-tenants`) with `max_batches_per_hour` rate limiting, which is the **recommended approach for large shared tenants**. Some sections below may reference older `MAX_PARALLEL_SCANS` settings for historical context or small-tenant scenarios. For shared tenants with 50K+ workspaces, always use chunked mode as shown in this guide.

## Executive Summary

**For large shared tenants with 247,000 workspaces:**

- **Conservative approach (recommended)**: `max_batches_per_hour=250` completes full baseline in **3-4 days**
- **Aggressive approach (off-hours only)**: `max_batches_per_hour=450` completes full baseline in **20-24 hours**

**Daily incremental updates** with hash optimization scan only **~5-15% of workspaces** in **5-15 minutes**.

**Key principle**: For shared tenants, leave at least 50% of API capacity for other users during business hours.

## Understanding the API Limits

### Common Misconception ❌
"Scanner API allows 500 workspaces/hour"

### Reality ✅
- **API Limit**: 500 **calls** per hour
- **Workspace Capacity**: ~6,000-8,000 **workspaces** per hour

### How It Works

Each batch of 100 workspaces requires:
1. `post_workspace_info()` - **1 API call** (initiate scan)
2. `poll_scan_status()` - **~6 API calls** (polls every 20 seconds for ~2 minutes)
3. `read_scan_result()` - **1 API call** (retrieve results)

**Total**: ~8 API calls per batch of 100 workspaces

**Throughput Calculation**:
- 500 API calls/hour ÷ 8 calls/batch = **62.5 batches/hour**
- 62.5 batches × 100 workspaces = **6,250 workspaces/hour**
- With 8 parallel scans: **~7,000-8,000 workspaces/hour** (polling overlaps)

## Recommended Configuration

### For Large Shared Tenants (247k Workspaces)

**Use chunked scan mode with conservative settings:**

```python
# Conservative settings for shared tenants (RECOMMENDED)
BATCH_SIZE_WORKSPACES  = 100  # Max allowed by API  
max_batches_per_hour   = 250  # Conservative - leaves 50% for others
POLL_INTERVAL_SECONDS  = 20   # Balance speed vs API usage
SCAN_TIMEOUT_MINUTES   = 30   # Generous timeout
```

**CLI Usage:**
```powershell
# Recommended for shared tenants (business hours)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 250

# Aggressive for off-hours (Friday evening/weekend)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 400
```

### Expected Performance (247k Workspaces)

| max_batches_per_hour | % of API Limit | Duration | When to Use |
|---------------------|----------------|----------|-------------|
| **250 (Recommended)** | **50%** | **3-4 days** | Shared tenant, business hours |
| 300 | 60% | 2.5-3 days | Off-hours, some sharing |
| 400 | 80% | 1.5-2 days | Late night, low usage |
| 450 | 90% | 20-24 hours | Weekends only, emergency |

## Recommended Strategy

### ⚠️ IMPORTANT: Shared Tenant Considerations

**For large shared tenants (247K workspaces), use conservative settings to avoid impacting other users.**

**Recommended default configuration:**
- `MAX_PARALLEL_SCANS = 1` (for chunked scans)
- `max_batches_per_hour = 250` (leaves 50% of API capacity for others)

**This uses ~250 API calls/hour, which is 50% of the 500/hour tenant limit**, leaving 250 API calls/hour (50%) for other users and processes.

**Before running large scans, consider:**
1. **Are you the only Scanner API user?** → Can increase to max_batches_per_hour=400-450
2. **Shared tenant with other teams?** → Use default 250 or check with `--health-check`
3. **Production environment?** → Coordinate with admins, use default 250
4. **Ad-hoc reports running?** → Run `--health-check` to see current API usage

**Settings for Different Scenarios:**

| Scenario | max_batches_per_hour | % of Limit | Impact | When to Use |
|----------|---------------------|-----------|--------|-------------|
| **Conservative (Default)** | **250** | **50%** | Leaves 50% for others | Shared tenant, business hours |
| Moderate | 300 | 60% | Leaves 40% for others | Off-hours, some sharing |
| Aggressive | 400 | 80% | Leaves 20% for others | Late night, confirmed low usage |
| Emergency | 450 | 90% | Leaves 10% for others | Weekends only, emergency |

**Recommended approach for shared tenants:**

```powershell
# Option 1: Conservative (default - safe for business hours)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 250
# Uses ~250 API calls/hour, leaves 250 for others

# Option 2: Off-hours (Friday evening/weekend)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 400
# Uses ~400 API calls/hour, completes faster

# Always check first
python fabric_scanner_cloud_connections.py --health-check
```

### Scheduling Strategy for Shared Tenants

**Business Hours (8 AM - 6 PM)**: Use conservative settings
```powershell
# Daily incremental scans - run at 7 AM
python fabric_scanner_cloud_connections.py --incremental --hours 24
# Completes in 5-15 minutes, minimal API impact
```

**Off-Hours (Evenings/Weekends)**: Can use higher throughput
```powershell
# Friday evening: Start baseline scan
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 400 --enable-checkpoints
# Completes over weekend (~1.5-2 days)
```

**Always check first:**
```powershell
python fabric_scanner_cloud_connections.py --health-check
```

## Phase 3: Parallel Capacity Scanning (NEW)

**Speed up full tenant scans by scanning multiple capacities concurrently** with thread-safe rate limiting.

### Overview

Phase 3 introduces parallel capacity scanning, which can reduce full tenant scan time by **2-3x** depending on your capacity distribution:
- **Sequential (default)**: 1 capacity at a time = baseline speed
- **Conservative parallel**: 2 capacities with reduced quota = ~1.5x faster
- **Balanced parallel**: 2 capacities with standard quota = ~2x faster
- **Faster parallel**: 3 capacities with full quota = ~2.8x faster

### How It Works

Instead of scanning all workspaces sequentially, Phase 3:
1. **Groups workspaces by capacity** (using Phase 2 metadata)
2. **Scans multiple capacity groups concurrently** using thread-safe workers
3. **Distributes API quota evenly** across workers (e.g., 450 calls/hour ÷ 3 workers = 150 calls/worker)
4. **Maintains rate limits** with centralized quota management
5. **Supports filtering** to scan/skip specific capacities

### Performance Examples (247k Workspaces)

Assuming workspaces are distributed across **10-15 capacities**:

| Configuration | Speed Improvement | Duration (Conservative) | When to Use |
|--------------|-------------------|------------------------|-------------|
| Sequential (default) | Baseline | 3-4 days | Heavily shared tenants |
| 2 workers, 300 calls/hour | ~1.5x faster | 2-2.5 days | Conservative parallel |
| 2 workers, 450 calls/hour | ~2x faster | 1.5-2 days | Most tenants |
| 3 workers, 450 calls/hour | ~2.8x faster | 1-1.5 days | Dedicated or off-hours |

### Conservative Parallel Scanning (Recommended)

**For shared tenants, start with conservative parallel settings:**

```powershell
# Very Conservative - Sequential (safest for heavily shared tenants)
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --large-shared-tenants \
  --parallel-capacities 1 \
  --max-calls-per-hour 450

# Conservative Parallel - 2 workers with reduced quota
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --large-shared-tenants \
  --parallel-capacities 2 \
  --max-calls-per-hour 300

# Balanced Parallel - 2 workers with standard quota (recommended)
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --large-shared-tenants \
  --parallel-capacities 2 \
  --max-calls-per-hour 450

# Faster Parallel - 3 workers (off-hours or dedicated tenants)
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --large-shared-tenants \
  --parallel-capacities 3 \
  --max-calls-per-hour 450
```

### Advanced Filtering

**Scan only production capacities:**
```powershell
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --parallel-capacities 2 \
  --capacity-filter "prod-capacity-1,prod-capacity-2,prod-capacity-3"
```

**Skip test/dev environments:**
```powershell
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --parallel-capacities 2 \
  --exclude-capacities "test-capacity,dev-capacity,sandbox"
```

**Prioritize critical capacities:**
```powershell
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --parallel-capacities 3 \
  --capacity-priority "production,critical" \
  --exclude-capacities "test,dev"
```

### When to Use Parallel Scanning

✅ **Use Sequential** (`--parallel-capacities 1`):
- Heavily shared tenants with many concurrent Scanner API users
- Production environments during business hours
- When API availability is uncertain
- First-time baseline scans (test with sequential first)

✅ **Use Conservative Parallel** (`--parallel-capacities 2 --max-calls-per-hour 300`):
- Shared tenants with moderate API usage
- Business hours with confirmed low API usage
- Teams coordinating Scanner API access

✅ **Use Balanced Parallel** (`--parallel-capacities 2 --max-calls-per-hour 450`):
- Most tenants (good speed/safety balance)
- Off-hours scanning (evenings/weekends)
- When you've tested with conservative settings

✅ **Use Faster Parallel** (`--parallel-capacities 3 --max-calls-per-hour 450`):
- Dedicated tenants or exclusive Scanner API access
- Weekend/off-hours scanning
- Emergency baseline updates

### Combining Phase 3 with Chunked Mode

**For the fastest large tenant scans, combine parallel capacity scanning with chunked mode:**

```powershell
# Balanced parallel + chunked mode (recommended for large tenants)
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --large-shared-tenants \
  --max-batches-per-hour 250 \
  --parallel-capacities 2 \
  --max-calls-per-hour 450 \
  --enable-checkpoints

# Faster parallel + chunked mode (off-hours/weekends)
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --large-shared-tenants \
  --max-batches-per-hour 400 \
  --parallel-capacities 3 \
  --max-calls-per-hour 450 \
  --enable-checkpoints
```

### Configuration File Example

Add Phase 3 settings to `scanner_config.yaml`:

```yaml
# Phase 3: Parallel Capacity Scanning
phase3:
  parallel_capacities: 2        # CONSERVATIVE: 1, BALANCED: 2, FASTER: 3
  max_calls_per_hour: 450       # CONSERVATIVE: 300, STANDARD: 450
  capacity_filter: []           # Optional: only scan these IDs
  exclude_capacities: []        # Optional: skip these IDs
  capacity_priority: []         # Optional: scan these first

# Performance settings
performance:
  max_batches_per_hour: 250     # Chunked mode rate limit
```

Then run:
```powershell
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --config scanner_config.yaml
```

### Benefits for Large Tenants

1. **Reduced scan time**: 2-3x faster for multi-capacity tenants
2. **Flexible speed control**: Start conservative, increase as needed
3. **Capacity-aware**: Leverage your capacity distribution
4. **Thread-safe**: No risk of quota violations
5. **Backward compatible**: Defaults to Phase 2 sequential behavior

### Limitations

- **Capacity distribution matters**: Maximum benefit when workspaces are distributed across multiple capacities
- **Single capacity tenants**: No benefit (all workspaces in one capacity)
- **API quota is shared**: Workers split the total quota (not additive)

---

## Scan Workflows for Large Tenants

### 1. Initial Baseline (One-Time)

**For Shared Tenants - Conservative Settings Recommended:**

**Option 1: Sequential with Chunked Scan** - Safest for shared tenants:

```powershell
# Conservative approach - sequential scanning
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --large-shared-tenants \
  --max-batches-per-hour 250 \
  --parallel-capacities 1 \
  --enable-checkpoints

# Duration: 3-4 days
# Uses only 250 API calls/hour (50% of limit), leaves 50% for others
```

**Option 2: Balanced Parallel with Chunked Scan** - Recommended for most tenants:

```powershell
# Balanced approach - 2 workers with standard quota
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --large-shared-tenants \
  --max-batches-per-hour 250 \
  --parallel-capacities 2 \
  --max-calls-per-hour 450 \
  --enable-checkpoints

# Duration: 1.5-2 days (2x faster than sequential)
# Uses 250 batches/hour in chunked mode, distributed across 2 capacity workers
```

**Option 3: Faster Parallel** - For dedicated tenants or off-hours:

```powershell
# Aggressive approach - 3 workers (ONLY if you're the sole user)
python fabric_scanner_cloud_connections.py \
  --full-scan \
  --large-shared-tenants \
  --max-batches-per-hour 450 \
  --parallel-capacities 3 \
  --max-calls-per-hour 450 \
  --enable-checkpoints

# Duration: 12-18 hours (2.8x faster than sequential)
# Uses 450 API calls/hour (90% of limit), leaves only 10% for others
```

**Timeline Comparison (247,000 workspaces)**:
- **Sequential**: 3-4 days (safest)
- **2 workers**: 1.5-2 days (balanced)
- **3 workers**: 12-18 hours (fastest, off-hours only)

### 2. Daily Incremental Updates

**With Hash Optimization** - Only scan changes:

```powershell
# Run DAILY at 7 AM
python fabric_scanner_cloud_connections.py --incremental --hours 24

# With hash optimization (enabled by default)
# - Scans only workspaces modified in last 24 hours
# - Skips workspaces already scanned recently
# - Reduces API usage by 80-90%
```

**Typical Daily Run**:
- Modified workspaces: ~10,000-20,000 (4-8% of total)
- Hash optimization filters to: ~1,500-3,000 (85% already scanned)
- Scan time: **5-15 minutes**
- API calls: **30-50** (minimal impact)

### 3. Weekly Full Refresh (Optional)

```powershell
# Run WEEKLY - Sunday 2 AM
python fabric_scanner_cloud_connections.py --incremental --days 7 --skip-hash-check

# --skip-hash-check forces scan of all modified workspaces
# (disables hash optimization for this run)
```

**Weekly Run**:
- Modified in 7 days: ~40,000-60,000 workspaces
- Scan time: **2-4 hours**

## Advanced Optimizations

### Understanding Chunked Mode API Usage

**Chunked mode (`--large-shared-tenants`) provides automatic rate limiting:**

```
How it works:
1. Splits workspaces into batches of 100
2. Processes up to max_batches_per_hour batches each hour
3. Automatically pauses between hourly chunks
4. Saves progress after each chunk (checkpoint/resume)
```

**Each batch uses approximately 8 API calls:**
```
1. post_workspace_info()  - 1 call
2. poll_scan_status()     - ~6 calls (polls every 20 seconds for ~2 min)
3. read_scan_result()     - 1 call
```

### Choosing max_batches_per_hour Setting

**Recommended Settings for Shared Tenants:**

| max_batches_per_hour | API Calls/Hour | % of Limit | For Other Users | When to Use | 247k Duration |
|---------------------|----------------|------------|-----------------|-------------|---------------|
| **250 (Recommended)** | **250** | **50%** | **250 (50%)** | **Shared tenant, business hours** | **3-4 days** |
| 300 | 300 | 60% | 200 (40%) | Off-hours, some sharing | 2.5-3 days |
| 400 | 400 | 80% | 100 (20%) | Late night, confirmed low usage | 1.5-2 days |
| 450 | 450 | 90% | 50 (10%) | Weekends only, emergency | 20-24 hours |

**Calculation Examples:**

**250 batches/hour (Conservative - RECOMMENDED):**
- API calls: 250 batches × 1 call to submit = **250 calls/hour**
- Percentage: 250 ÷ 500 = **50% of limit**
- Leaves: **250 calls/hour (50%) for other users** ✅
- Workspaces: 250 batches × 100 = **25,000 workspaces/hour**
- Time for 247k: 247,000 ÷ 25,000 = ~10 hours active + pauses = **3-4 days**

**450 batches/hour (Aggressive - OFF-HOURS ONLY):**
- API calls: 450 batches × 1 call to submit = **450 calls/hour**
- Percentage: 450 ÷ 500 = **90% of limit**
- Leaves: **Only 50 calls/hour (10%) for other users** ⚠️
- Workspaces: 450 batches × 100 = **45,000 workspaces/hour**  
- Time for 247k: 247,000 ÷ 45,000 = ~5.5 hours active + pauses = **20-24 hours**

### When to Use Different Settings

#### Use max_batches_per_hour=250 (Conservative):
✅ **Shared tenant** with multiple API consumers  
✅ **Production environment** during business hours  
✅ Running during **business hours** (8 AM - 6 PM)  
✅ Daily scheduled scans (reliability > speed)  
✅ **Other teams running reports** or using Scanner API  
✅ **Unsure about tenant usage** - be courteous

#### Use max_batches_per_hour=400-450 (Aggressive):
✅ Running during **off-hours** (nights/weekends)  
✅ **Dedicated tenant** (you're the only Scanner API user)  
✅ **Coordinated with tenant admins** - others agree to pause  
✅ One-time baseline scan with time pressure  
✅ Weekend scan confirmed via `--health-check`

### Shared Tenant Best Practices

#### Detecting Other Scanner API Users

**Before running large scans, check for other API consumers:**

1. **Run health check**:
   ```powershell
   python fabric_scanner_cloud_connections.py --health-check
   ```

2. **Ask tenant admins**: "Who else uses Scanner API?"

3. **Check for 429 errors**: If you get rate limit errors, others are using API

4. **Common Scanner API consumers**:
   - Power BI usage reports/audits
   - Automated governance scans
   - Third-party monitoring tools (Datadog, Splunk connectors)
   - Other data engineering teams

#### Quota Sharing Strategy

**If multiple processes use Scanner API:**

| Scenario | Your Quota | Recommended Setting | 247k Time |
|----------|------------|---------------------|-----------|
| **You alone** | 500 calls/hr | max_batches_per_hour=450 | 20-24 hours |
| **You + 1 other** | 250 calls/hr | max_batches_per_hour=250 | 3-4 days |
| **You + 2 others** | 167 calls/hr | max_batches_per_hour=150 | 6-7 days |
| **3+ processes** | <125 calls/hr | **Coordinate schedules!** | 8+ days |

**Coordination Strategy:**
```
Monday-Wednesday: Team A runs scans (max_batches_per_hour=400)
Thursday-Friday: Team B runs scans (max_batches_per_hour=400)
Weekends: Team C runs scans (max_batches_per_hour=450)
```

#### Respectful API Usage

**Conservative-first approach:**
1. Start with `max_batches_per_hour=250` (50% usage)
2. Monitor for 429 errors
3. If no errors for 1-2 hours, can increase to 300-400
4. Always leave at least 100 calls/hour (20%) for others

#### Coordination Strategies

**Option 1: Time-Based Scheduling (Recommended)**

Divide the day into time slots:

```powershell
# Team A: Midnight - 8 AM (8 hours)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 450

# Team B: 8 AM - 4 PM (8 hours)  
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 450

# Team C: 4 PM - Midnight (8 hours)
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 450
```

**Benefit**: Each team gets full API quota during their window

**Option 2: Priority-Based Scheduling**

```
High Priority (Production Reports): 8 AM - 10 AM
  - max_batches_per_hour=450 (90% quota)

Medium Priority (Your scans): 10 AM - 6 PM
  - max_batches_per_hour=200 (shares with ad-hoc users)

Low Priority (Large baseline scans): 6 PM - 8 AM + Weekends
  - max_batches_per_hour=450 (off-hours, full quota)
```

**Option 3: Continuous Sharing**

All teams run simultaneously with reduced settings:

```
2 teams continuously:
  Team A: max_batches_per_hour=250
  Team B: max_batches_per_hour=250
  Total: 500 calls/hour (perfect balance)

3 teams continuously:
  Team A: max_batches_per_hour=150
  Team B: max_batches_per_hour=150  
  Team C: max_batches_per_hour=150
  Total: 450 calls/hour (safe)
```

#### Signs You Need to Coordinate

**Red flags that others are using Scanner API:**

❌ Getting **429 "Too Many Requests"** errors frequently  
❌ Scans taking **longer than expected** to complete  
❌ API calls **timing out** or retrying often  
❌ Your API usage shows **less than expected** throughput  
❌ **Inconsistent performance** (fast sometimes, slow other times)

**If you see these, reduce your setting immediately:**
```powershell
# Emergency throttle
python fabric_scanner_cloud_connections.py --full-scan --large-shared-tenants --max-batches-per-hour 150
```

#### Communication Template

**Send to tenant admins before large scans:**

```
Subject: Planned Scanner API Usage - [Date/Time]

Team: [Your Team]
Purpose: Baseline scan of 247k workspaces for cloud connection inventory
Estimated Duration: 3-4 days (Friday 6 PM - Tuesday 6 PM)
API Usage: 250 calls/hour (50% of tenant limit)
Impact: Leaves 50% API capacity for other teams

Please confirm:
1. Are any other teams planning Scanner API usage this period?
2. Are there scheduled reports/jobs that use Scanner API?
3. OK to proceed with this schedule?

Alternative: We can run at higher priority during off-hours (max_batches_per_hour=400)
This reduces duration to 1.5-2 days but uses 80% of API capacity.
```

#### Quick Decision Matrix

**Choosing your max_batches_per_hour setting:**

| Situation | Recommended Setting | Why |
|-----------|-------------------|-----|
| You're the only user | 450 | Full quota available |
| 1 other user (known schedule) | 250 | Share quota equally |
| 2+ other users | 150-200 | Divide quota by N |
| Getting 429 errors | 100-150 | Others need capacity NOW |
| Business hours (uncertain) | 250 | Be courteous |
| Off-hours/weekends | 400-450 | Likely no competition |
| Production environment | 250 | Safety first |
| Ad-hoc/testing | 100-150 | Don't block production |

**DO:**
- ✅ Use `max_batches_per_hour=250` during business hours (leaves 50% capacity)
- ✅ Schedule large scans for nights/weekends
- ✅ Coordinate with other teams before baseline scans
- ✅ Monitor for 429 errors (means others are using API)
- ✅ Use hash optimization to reduce daily API usage by 80-90%
- ✅ Send notification: "Running large scan Friday 6 PM - Tuesday 6 PM"

**DON'T:**
- ❌ Use `max_batches_per_hour=450` during business hours in shared tenants
- ❌ Run unscheduled baseline scans without notification
- ❌ Ignore 429 rate limit errors (means you're blocking others)
- ❌ Assume you're the only Scanner API user
- ❌ Disable hash optimization for routine daily scans
- ❌ Run multiple processes simultaneously from same team (combine into one!)

## Health Check Feature

The script includes `--health-check` to detect API contention before starting large scans.

**Usage:**
```powershell
python fabric_scanner_cloud_connections.py --health-check
```

This makes a few test API calls to determine if other users are consuming the Scanner API quota, helping you choose appropriate `max_batches_per_hour` settings.

## Summary

For large shared tenants with 247,000+ workspaces:

1. **Use chunked mode**: `--large-shared-tenants` flag
2. **Conservative default**: `max_batches_per_hour=250` (3-4 days, 50% API usage)
3. **Aggressive for off-hours**: `max_batches_per_hour=400-450` (1.5-2 days, 80-90% API usage)
4. **Daily incremental**: `--incremental --hours 24` (5-15 minutes with hash optimization)
5. **Always check first**: `--health-check` before large scans
6. **Coordinate with teams**: Notify admins before baseline scans
7. **Monitor for 429 errors**: Reduce settings if you see rate limiting

The conservative approach ensures you're a good tenant citizen while still completing necessary scans efficiently.

### Scenario 1: First-Time Setup (247k Workspaces)

**Friday Evening Start**:
```
Friday 6:00 PM  - Start chunked scan
Saturday 11:00 AM - ~50% complete (123k workspaces)
Sunday 11:00 AM   - Complete (247k workspaces)
```

**Monday**: Begin daily incremental scans (15-30 minutes each)

### Scenario 2: Daily Operations (After Baseline)

**Typical Daily Scan** (7 AM):
```
7:00 AM - Start incremental scan
7:15 AM - Hash optimization filters 18,000 → 2,500 workspaces
7:25 AM - Complete (2,500 workspaces scanned)
```

**API Usage**: ~200 calls (60% below limit)

### Scenario 3: Weekly Refresh

**Sunday 2 AM**:
```
2:00 AM  - Start weekly scan (7 days lookback, no hash optimization)
2:01 AM  - Found 52,000 modified workspaces
9:30 AM  - Complete (52,000 workspaces scanned)
```

**Total time**: 7.5 hours

## Cost-Benefit Analysis

### Full Scan Strategy

| Frequency | Workspaces Scanned/Month | API Calls/Month | Time/Month |
|-----------|--------------------------|-----------------|------------|
| Weekly full scan | 988,000 | ~124,000 | ~164 hours |
| Monthly full scan | 247,000 | ~31,000 | ~41 hours |

### Incremental Strategy (Recommended)

| Frequency | Workspaces Scanned/Month | API Calls/Month | Time/Month |
|-----------|--------------------------|-----------------|------------|
| Daily incremental (with hash opt) | ~75,000 | ~9,400 | ~6 hours |
| Daily incremental (no hash opt) | ~450,000 | ~56,000 | ~75 hours |

**Savings with Hash Optimization**:
- **83% fewer workspaces scanned**
- **83% fewer API calls**
- **92% less time** (6 hours vs 75 hours)

## Recommended Workflow for 247k Workspaces

### Setup Phase (One-Time)

1. **Configure authentication** (Service Principal recommended for automation)
2. **Test with small subset**: Use 1,000 workspaces first
3. **Run baseline chunked scan**: Friday evening start
4. **Verify results**: Monday morning check data quality
5. **Schedule daily incremental**: 7 AM daily with hash optimization

### Operational Phase (Daily)

```python
# Scheduled daily at 7 AM
run_cloud_connection_scan(
    enable_incremental_scan=True,
    enable_hash_optimization=True,  # 80-90% API savings!
    incremental_hours_back=24,
    include_personal=True
)
```

**Expected**:
- Runtime: 15-30 minutes
- API calls: 150-300
- Workspaces scanned: 1,500-3,000

### Monthly Audit (Optional)

```python
# Scheduled monthly (first Sunday, 2 AM)
run_cloud_connection_scan(
    enable_incremental_scan=True,
    enable_hash_optimization=False,  # Full scan of month's changes
    incremental_days_back=30,
    include_personal=True
)
```

**Expected**:
- Runtime: 12-18 hours
- API calls: ~12,000-18,000
- Workspaces scanned: ~100,000-150,000

## Fabric Cluster Recommendations

### For Baseline Scan (30-40 hours)
- **Cluster**: High Concurrency
- **Cores**: 16-32
- **RAM**: 32-64 GB
- **Cost**: ~$50-100 for full scan (depends on region)

### For Daily Incremental (15-30 minutes)
- **Cluster**: Standard
- **Cores**: 4-8
- **RAM**: 8-16 GB
- **Cost**: <$5/month

## Summary

For **247,000 workspaces**:

**One-time baseline**: 
- Use `enable_full_scan_chunked=True`
- Run over weekend (~40 hours)
- Cost: ~$75 in Fabric compute

**Daily operations**:
- Use `enable_incremental_scan=True` + `enable_hash_optimization=True`
- Runtime: 15-30 minutes
- API calls: 150-300/day (95% under limit)
- Cost: ~$5/month in Fabric compute

**Total first month**:
- Setup: 40 hours
- Daily updates: 7.5 hours (30 days × 15 minutes)
- **Total**: ~48 hours operational time
- **Cost**: ~$80 total

**Subsequent months**:
- Daily updates only: 7.5 hours/month
- Cost: ~$5/month

The chunked scan + hash optimization makes managing 247k workspaces **completely feasible** with minimal ongoing effort!
