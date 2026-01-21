# Test Suite - Fabric Scanner Cloud Connections

This directory contains comprehensive tests for the Fabric Scanner Cloud Connections project, covering all major features including capacity metadata (Phase 1), capacity grouping (Phase 2), and parallel capacity scanning (Phase 3).

## Test Files

### test_capacity_metadata.py
**Phase 1: Capacity Metadata Validation**

Tests the capacity metadata extraction functionality that adds three columns to the scanner output:
- `capacity_id`: Unique identifier for the capacity
- `capacity_name`: Display name of the capacity
- `is_dedicated_capacity`: Boolean indicating if capacity is dedicated or shared

**Test Coverage:**
- ✅ Capacity metadata extraction from workspace objects
- ✅ Dedicated vs shared capacity detection
- ✅ Missing capacity field handling
- ✅ Null value handling
- ✅ All 5 test scenarios pass

**Run this test:**
```powershell
cd tests
python test_capacity_metadata.py
```

**Expected Output:**
```
TEST SUMMARY
Total tests: 5
Passed: 5
Failed: 0
Success rate: 100.0%
```

---

### test_phase3_parallel_scanning.py
**Phase 3: Parallel Capacity Scanning**

Tests the parallel capacity scanning functionality that speeds up full tenant scans by scanning multiple capacities concurrently with thread-safe rate limiting.

**Test Coverage:**
- ✅ SharedRateLimiter quota distribution (450 calls → 150/worker for 3 workers)
- ✅ Acquire/release functionality
- ✅ Statistics reporting
- ✅ Capacity filtering (include only specific IDs)
- ✅ Capacity exclusion (skip specific IDs)
- ✅ Capacity prioritization (process critical capacities first)
- ✅ Thread safety (30 concurrent operations)
- ✅ CLI argument parsing (5 new Phase 3 arguments)
- ✅ Capacity grouping integration
- ✅ Error handling (invalid capacity IDs)

**Run this test:**
```powershell
cd tests
python test_phase3_parallel_scanning.py
```

**Expected Output:**
```
TEST SUMMARY
Total tests: 10
Passed: 10
Failed: 0
Success rate: 100.0%
```

---

### test_core_functions.py
**Core Scanner Functions**

Tests the fundamental scanning operations including full scans, incremental scans, scan ID retrieval, health checks, JSON processing, and CLI parsing.

**Test Coverage:**
- ✅ Health check - API availability and quota validation
- ✅ Scan result retrieval - Get results by scan ID
- ✅ Incremental scan filtering - Workspace modification time filtering
- ✅ Hash optimization - Smart workspace filtering (80-90% reduction)
- ✅ JSON directory scanning - Processing scanner API JSON files
- ✅ Chunked scan calculations - Batch sizing for large tenants (247k workspaces)
- ✅ Workspace batching - Correct batch chunk creation
- ✅ Checkpoint functionality - Save/resume progress tracking
- ✅ Personal workspace filtering - Include/exclude personal workspaces
- ✅ CLI argument parsing - All scan modes (full, incremental, scan-id, chunked, health-check)

**Run this test:**
```powershell
cd tests
python test_core_functions.py
```

**Expected Output:**
```
TEST SUMMARY
Total tests: 10
Passed: 10
Failed: 0
Success rate: 100.0%
```

---

## Running All Tests

Run all tests sequentially:

```powershell
cd tests
python test_capacity_metadata.py
python test_phase3_parallel_scanning.py
python test_core_functions.py
```

Or create a simple test runner:

```powershell
# Run all tests and summarize results
Get-ChildItem *.py | Where-Object { $_.Name -like "test_*.py" } | ForEach-Object {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "Running: $($_.Name)" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    python $_.Name
}
```

---

## Test Design Philosophy

### Mock-Based Testing
All tests use **mock objects** instead of making actual API calls to:
- ✅ Avoid authentication requirements
- ✅ Ensure tests run quickly (no network delays)
- ✅ Provide deterministic results
- ✅ Test edge cases that are hard to reproduce with real APIs
- ✅ Prevent accidental API quota consumption

### Test Structure
Each test file follows this pattern:
```python
import sys
from unittest.mock import Mock, patch, MagicMock

# Mock Fabric modules
sys.modules['notebookutils'] = MagicMock()
sys.modules['pyspark'] = MagicMock()

# Import scanner
import fabric_scanner_cloud_connections as scanner

# Test functions
def test_feature():
    # Setup mocks
    # Execute function
    # Verify results
    # Print status
```

---

## FAQ

### Q: Do I need to authenticate to run the tests?
**A:** No. The tests use mock objects and don't make actual API calls. You'll see authentication messages during module import, but these can be ignored.

### Q: How long do the tests take to run?
**A:** All 25 tests (across 3 files) complete in **under 5 seconds** since they use mocks instead of real API calls.

### Q: Can I run tests from the main project directory?
**A:** Yes, but it's recommended to run from the `tests/` directory:
```powershell
# From project root
python tests/test_capacity_metadata.py

# Better: From tests directory
cd tests
python test_capacity_metadata.py
```

### Q: What if a test fails?
**A:** Test failures indicate a regression in functionality. Check:
1. **Error message**: Shows which assertion failed and why
2. **Test name**: Identifies which feature is broken
3. **Recent changes**: Review recent code modifications to `fabric_scanner_cloud_connections.py`

Example failure output:
```
✗ FAIL: test_quota_distribution
   Error: Each worker should get 150 calls, got 100
```

### Q: Do the tests cover Phase 2 (capacity grouping)?
**A:** Yes. Phase 2 functionality is validated in `test_phase3_parallel_scanning.py` (Test 9: Capacity Grouping Integration), which tests that workspaces are correctly grouped by capacity.

### Q: Can I add new tests?
**A:** Absolutely! Follow the existing pattern:
```python
def test_new_feature():
    """Test X: Description of what you're testing"""
    print("\n" + "="*70)
    print("TEST X: Feature Name")
    print("="*70)
    
    # Your test logic here
    
    assert condition, "Failure message"
    print(f"✓ PASS: Feature works correctly")
```

### Q: Why don't you use pytest?
**A:** These tests use simple assertions for:
- ✅ **No dependencies**: Works without installing pytest
- ✅ **Simplicity**: Easy to understand and modify
- ✅ **Portability**: Run anywhere with just Python
- ✅ **Clear output**: Custom formatting for better readability

### Q: How do I debug a failing test?
**A:** Add print statements or use Python's debugger:
```python
# Add debug output
print(f"DEBUG: Variable value = {some_var}")

# Or use pdb
import pdb; pdb.set_trace()
```

### Q: Are these unit tests or integration tests?
**A:** They're **unit tests** with some integration testing:
- **Unit tests**: Test individual functions in isolation (most tests)
- **Integration tests**: Test how components work together (capacity grouping + parallel scanning)

### Q: Do tests validate the actual Scanner API?
**A:** No. These tests validate the **client code logic**, not the Microsoft Scanner API itself. They test:
- ✅ Correct request formation
- ✅ Response parsing
- ✅ Error handling
- ✅ Data transformation
- ✅ Business logic

### Q: What's not tested?
**A:** The following require manual testing or real API calls:
- ❌ Actual authentication with Azure/Fabric
- ❌ Real Scanner API rate limiting behavior
- ❌ Network connectivity issues
- ❌ Large-scale performance (247k workspaces)
- ❌ Lakehouse upload functionality

### Q: How often should I run the tests?
**A:** Run tests:
- ✅ Before committing code changes
- ✅ After modifying `fabric_scanner_cloud_connections.py`
- ✅ When adding new features
- ✅ When fixing bugs
- ✅ Before releasing to production

### Q: Can tests be run in CI/CD?
**A:** Yes! Since they don't require authentication or API access, they're perfect for CI/CD:
```yaml
# Example GitHub Actions
- name: Run Tests
  run: |
    cd tests
    python test_capacity_metadata.py
    python test_phase3_parallel_scanning.py
    python test_core_functions.py
```

### Q: What's the coverage of the test suite?
**A:** Current coverage:
- ✅ **Phase 1** (Capacity Metadata): 5 tests - 100% coverage
- ✅ **Phase 2** (Capacity Grouping): Validated in Phase 3 tests
- ✅ **Phase 3** (Parallel Scanning): 10 tests - 100% coverage
- ✅ **Core Functions**: 10 tests covering scanning, checkpoints, CLI
- ✅ **Total**: 25 tests, 100% pass rate

### Q: Why are there warnings about datetime.utcnow()?
**A:** This is a Python 3.12+ deprecation warning. It's informational only and doesn't affect test results. To fix, update to:
```python
# Old (deprecated in Python 3.12+)
now = datetime.utcnow()

# New (timezone-aware)
from datetime import datetime, timezone
now = datetime.now(timezone.utc)
```

### Q: What if I see "module X does not have attribute Y" errors?
**A:** This means the test is trying to mock a function that doesn't exist. Check:
1. Function name matches exactly (case-sensitive)
2. Function exists in `fabric_scanner_cloud_connections.py`
3. Import path is correct

---

## Test Maintenance

### Adding New Features
When adding new features to `fabric_scanner_cloud_connections.py`:

1. **Create test function** in appropriate test file
2. **Use mocks** to simulate API responses
3. **Validate logic** with assertions
4. **Print results** for visibility
5. **Update this README** with new test details

### Updating Tests
When modifying existing features:

1. **Update relevant test** to match new behavior
2. **Ensure backward compatibility** where possible
3. **Run all tests** to check for regressions
4. **Update documentation** if test changes

### Deprecating Tests
If a feature is removed:

1. **Delete the test function**
2. **Update test count** in file header
3. **Update this README**
4. **Document in commit message**

---

## Success Criteria

All tests should show **100% pass rate**:

```
✓ test_capacity_metadata.py: 5/5 passed (100%)
✓ test_phase3_parallel_scanning.py: 10/10 passed (100%)
✓ test_core_functions.py: 10/10 passed (100%)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total: 25/25 tests passed (100%)
```

Any failures indicate a regression that should be investigated immediately.

---

## Contributing

To contribute new tests:

1. Follow the existing pattern (mock-based, clear output)
2. Test one feature per function
3. Include descriptive print statements
4. Add assertions with clear failure messages
5. Update this README with new test documentation
6. Ensure 100% pass rate before committing

---

## Support

For questions about the tests:
1. Review this README and FAQ first
2. Check test file comments for implementation details
3. Review `../README.md` for feature documentation
4. Examine existing tests for patterns

---

**Last Updated:** January 2026  
**Test Suite Version:** 1.0  
**Total Tests:** 25  
**Pass Rate:** 100%
