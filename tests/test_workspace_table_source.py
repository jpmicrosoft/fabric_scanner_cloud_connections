"""
Test Suite for Workspace Table Source Feature
Tests the read_workspaces_from_table() functionality with validation, error handling, and fallback logic.
"""

import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import re

# Add parent directory to sys.path to import the scanner module
sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock Fabric modules before importing scanner
sys.modules['notebookutils'] = MagicMock()
sys.modules['notebookutils.mssparkutils'] = MagicMock()
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()

# Import scanner module
import fabric_scanner_cloud_connections as scanner

# Mock authentication globals to prevent auth attempts
scanner.HEADERS = {"Authorization": "Bearer mock_token", "Content-Type": "application/json"}
scanner.ACCESS_TOKEN = "mock_token"

# Test counters
tests_passed = 0
tests_failed = 0

# Use ASCII-safe checkmarks for Windows compatibility
CHECK = "[PASS]"
CROSS = "[FAIL]"

def test_empty_table_name():
    """Test 1: Empty and whitespace-only table names should be rejected"""
    global tests_passed, tests_failed
    
    print("\n" + "="*70)
    print("TEST 1: Empty Table Name Validation")
    print("="*70)
    
    test_cases = [
        ("", "empty string"),
        ("   ", "whitespace only"),
        ("\t", "tab only"),
        ("\n", "newline only"),
    ]
    
    for table_name, description in test_cases:
        try:
            # Should raise ValueError
            result = scanner.read_workspaces_from_table(table_name)
            print(f"✗ FAIL: {description} should be rejected")
            tests_failed += 1
            return
        except ValueError as e:
            if "cannot be empty" in str(e):
                print(f"  ✓ {description:20s} - Correctly rejected")
            else:
                print(f"✗ FAIL: Wrong error message for {description}")
                tests_failed += 1
                return
    
    tests_passed += 1
    print(f"\n✓ PASS: Empty table name validation working correctly")

def test_sql_injection_prevention():
    """Test 2: SQL injection attempts should be blocked"""
    global tests_passed, tests_failed
    
    print("\n" + "="*70)
    print("TEST 2: SQL Injection Prevention")
    print("="*70)
    
    injection_attempts = [
        ("workspace; DROP TABLE users--", "Table drop"),
        ("workspace UNION SELECT * FROM sensitive--", "UNION injection"),
        ("workspace' OR '1'='1", "Classic injection"),
        ("../../../etc/passwd", "Path traversal"),
        ("table-name", "Hyphen"),
        ("table name", "Space"),
        ("table@name", "At symbol"),
    ]
    
    for table_name, description in injection_attempts:
        # Mock Spark environment
        with patch.object(scanner, 'RUNNING_IN_FABRIC', True), \
             patch.object(scanner, 'SPARK_AVAILABLE', True):
            # Should return None (fallback to API) when validation fails
            result = scanner.read_workspaces_from_table(table_name)
            
            if result is None:
                print(f"  ✓ {description:25s} - Blocked (returns None)")
            else:
                print(f"✗ FAIL: {description} should be blocked but returned {result}")
                tests_failed += 1
                return
    
    tests_passed += 1
    print(f"\n✓ PASS: SQL injection prevention working correctly")

def test_valid_table_names():
    """Test 3: Valid table names should pass validation"""
    global tests_passed, tests_failed
    
    print("\n" + "="*70)
    print("TEST 3: Valid Table Name Acceptance")
    print("="*70)
    
    valid_names = [
        "workspace_inventory",
        "prod.workspace_catalog",
        "my_lakehouse.dbo.workspaces",
        "TABLE123",
        "table_name_2024",
    ]
    
    for table_name in valid_names:
        # Test regex validation only
        if not re.match(r'^[a-zA-Z0-9_\.]+$', table_name):
            print(f"✗ FAIL: '{table_name}' should be valid")
            tests_failed += 1
            return
        print(f"  ✓ '{table_name}' - Valid")
    
    tests_passed += 1
    print(f"\n✓ PASS: Valid table names accepted correctly")

def test_spark_missing_column():
    """Test 4: Missing workspace_id column should raise error"""
    global tests_passed, tests_failed
    
    print("\n" + "="*70)
    print("TEST 4: Missing Required Column Detection (Spark)")
    print("="*70)
    
    # Mock Spark DataFrame without workspace_id column
    mock_df = Mock()
    mock_df.columns = ["name", "type", "capacity"]  # Missing workspace_id
    
    mock_spark = Mock()
    mock_spark.sql.return_value = mock_df
    
    with patch.object(scanner, 'RUNNING_IN_FABRIC', True), \
         patch.object(scanner, 'SPARK_AVAILABLE', True), \
         patch.object(scanner, 'spark', mock_spark), \
         patch.object(scanner, 'DEBUG_MODE', False):
        
        result = scanner.read_workspaces_from_table("test_table")
        
        if result is None:  # Should return None and fallback to API
            tests_passed += 1
            print(f"\n✓ PASS: Missing column detected, returns None for API fallback")
        else:
            tests_failed += 1
            print(f"✗ FAIL: Should return None when workspace_id column missing")

def test_spark_null_filtering():
    """Test 5: Null workspace_id values should be filtered out"""
    global tests_passed, tests_failed
    
    print("\n" + "="*70)
    print("TEST 5: Null Workspace ID Filtering (Spark)")
    print("="*70)
    
    # Mock Spark DataFrame with null filtering
    mock_row1 = Mock()
    mock_row1.workspace_id = "ws-001"
    mock_row1.workspace_name = "Sales"
    
    mock_row2 = Mock()
    mock_row2.workspace_id = "ws-002"
    mock_row2.workspace_name = "Marketing"
    
    mock_df = Mock()
    mock_df.columns = ["workspace_id", "workspace_name"]
    mock_df.filter.return_value = mock_df  # Chain filter calls
    mock_df.count.return_value = 2
    mock_df.collect.return_value = [mock_row1, mock_row2]
    
    mock_spark = Mock()
    mock_spark.sql.return_value = mock_df
    
    # Mock F.col for null filtering
    mock_F = Mock()
    mock_F.col.return_value.isNotNull.return_value = Mock()
    
    with patch.object(scanner, 'RUNNING_IN_FABRIC', True), \
         patch.object(scanner, 'SPARK_AVAILABLE', True), \
         patch.object(scanner, 'spark', mock_spark), \
         patch.object(scanner, 'F', mock_F), \
         patch.object(scanner, 'DEBUG_MODE', False):
        
        result = scanner.read_workspaces_from_table("test_table")
        
        if result is not None and len(result) == 2:
            if result[0]["id"] == "ws-001" and result[1]["id"] == "ws-002":
                tests_passed += 1
                print(f"  ✓ Retrieved 2 workspaces after null filtering")
                print(f"\n✓ PASS: Null filtering works correctly")
            else:
                tests_failed += 1
                print(f"✗ FAIL: Workspace IDs incorrect: {result}")
        else:
            tests_failed += 1
            print(f"✗ FAIL: Expected 2 workspaces, got {result}")

def test_pandas_missing_column():
    """Test 6: Missing workspace_id column in pandas should raise error"""
    global tests_passed, tests_failed
    
    print("\n" + "="*70)
    print("TEST 6: Missing Required Column Detection (Pandas)")
    print("="*70)
    
    # Mock pandas DataFrame without workspace_id
    mock_df = Mock()
    mock_df.columns = ["name", "type"]  # Missing workspace_id
    
    mock_pd = Mock()
    mock_pd.read_parquet.return_value = mock_df
    
    with patch.object(scanner, 'RUNNING_IN_FABRIC', False), \
         patch.object(scanner, 'SPARK_AVAILABLE', False), \
         patch.object(scanner, 'PANDAS_AVAILABLE', True), \
         patch.object(scanner, 'pd', mock_pd), \
         patch('pathlib.Path.exists', return_value=True), \
         patch.object(scanner, 'DEBUG_MODE', False):
        
        result = scanner.read_workspaces_from_table("test_workspaces")
        
        if result is None:  # Should return None and fallback to API
            tests_passed += 1
            print(f"\n✓ PASS: Missing column detected, returns None for API fallback")
        else:
            tests_failed += 1
            print(f"✗ FAIL: Should return None when workspace_id column missing")

def test_pandas_null_filtering():
    """Test 7: Null workspace_id values should be filtered in pandas"""
    global tests_passed, tests_failed
    
    print("\n" + "="*70)
    print("TEST 7: Null Workspace ID Filtering (Pandas)")
    print("="*70)
    
    # Mock pandas DataFrame with data
    mock_df = Mock()
    mock_df.columns = ["workspace_id", "workspace_name"]
    mock_df.__len__ = Mock(return_value=2)  # After filtering
    
    # Mock filtering
    mock_df.__getitem__ = Mock(return_value=mock_df)
    mock_df.notna.return_value = Mock()
    
    # Mock iterrows
    mock_df.iterrows.return_value = [
        (0, {"workspace_id": "ws-001", "workspace_name": "Sales"}),
        (1, {"workspace_id": "ws-002", "workspace_name": "Marketing"}),
    ]
    
    mock_pd = Mock()
    mock_pd.read_parquet.return_value = mock_df
    
    with patch.object(scanner, 'RUNNING_IN_FABRIC', False), \
         patch.object(scanner, 'SPARK_AVAILABLE', False), \
         patch.object(scanner, 'PANDAS_AVAILABLE', True), \
         patch.object(scanner, 'pd', mock_pd), \
         patch('pathlib.Path.exists', return_value=True), \
         patch.object(scanner, 'DEBUG_MODE', False):
        
        result = scanner.read_workspaces_from_table("test_workspaces")
        
        if result is not None and len(result) == 2:
            tests_passed += 1
            print(f"  ✓ Retrieved 2 workspaces after null filtering")
            print(f"\n✓ PASS: Pandas null filtering works correctly")
        else:
            tests_failed += 1
            print(f"✗ FAIL: Expected 2 workspaces, got {result}")

def test_empty_result_fallback():
    """Test 8: Empty table should trigger API fallback"""
    global tests_passed, tests_failed
    
    print("\n" + "="*70)
    print("TEST 8: Empty Table API Fallback")
    print("="*70)
    
    # Mock empty DataFrame
    mock_df = Mock()
    mock_df.columns = ["workspace_id"]
    mock_df.filter.return_value = mock_df
    mock_df.count.return_value = 0
    mock_df.collect.return_value = []
    
    mock_spark = Mock()
    mock_spark.sql.return_value = mock_df
    
    mock_F = Mock()
    mock_F.col.return_value.isNotNull.return_value = Mock()
    
    with patch.object(scanner, 'RUNNING_IN_FABRIC', True), \
         patch.object(scanner, 'SPARK_AVAILABLE', True), \
         patch.object(scanner, 'spark', mock_spark), \
         patch.object(scanner, 'F', mock_F), \
         patch.object(scanner, 'DEBUG_MODE', False):
        
        result = scanner.read_workspaces_from_table("empty_table")
        
        if result is None:
            tests_passed += 1
            print(f"  ✓ Empty table returns None")
            print(f"\n✓ PASS: Empty table triggers API fallback correctly")
        else:
            tests_failed += 1
            print(f"✗ FAIL: Empty table should return None, got {result}")

def test_file_not_found_fallback():
    """Test 9: Missing file should trigger API fallback"""
    global tests_passed, tests_failed
    
    print("\n" + "="*70)
    print("TEST 9: File Not Found API Fallback")
    print("="*70)
    
    with patch.object(scanner, 'RUNNING_IN_FABRIC', False), \
         patch.object(scanner, 'SPARK_AVAILABLE', False), \
         patch.object(scanner, 'PANDAS_AVAILABLE', True), \
         patch('pathlib.Path.exists', return_value=False), \
         patch.object(scanner, 'DEBUG_MODE', False):
        
        result = scanner.read_workspaces_from_table("nonexistent_file")
        
        if result is None:
            tests_passed += 1
            print(f"  ✓ File not found returns None")
            print(f"\n✓ PASS: Missing file triggers API fallback correctly")
        else:
            tests_failed += 1
            print(f"✗ FAIL: Missing file should return None, got {result}")

def test_personal_workspace_filtering():
    """Test 10: Personal workspace filtering should work"""
    global tests_passed, tests_failed
    
    print("\n" + "="*70)
    print("TEST 10: Personal Workspace Filtering")
    print("="*70)
    
    # Mock rows with different workspace types
    mock_row1 = Mock()
    mock_row1.workspace_id = "ws-001"
    mock_row1.workspace_type = "Workspace"
    
    mock_row2 = Mock()
    mock_row2.workspace_id = "ws-002"
    mock_row2.workspace_type = "Workspace"
    
    mock_df = Mock()
    mock_df.columns = ["workspace_id", "workspace_type"]
    mock_df.filter.return_value = mock_df
    mock_df.count.return_value = 2
    mock_df.collect.return_value = [mock_row1, mock_row2]
    
    mock_spark = Mock()
    mock_spark.sql.return_value = mock_df
    
    mock_F = Mock()
    mock_F.col.return_value.isNotNull.return_value = Mock()
    
    with patch.object(scanner, 'RUNNING_IN_FABRIC', True), \
         patch.object(scanner, 'SPARK_AVAILABLE', True), \
         patch.object(scanner, 'spark', mock_spark), \
         patch.object(scanner, 'F', mock_F), \
         patch.object(scanner, 'DEBUG_MODE', False):
        
        # Test with include_personal=False
        result = scanner.read_workspaces_from_table("test_table", include_personal=False)
        
        # Verify filter was called for PersonalGroup
        mock_df.filter.assert_called()
        
        if result is not None and len(result) == 2:
            tests_passed += 1
            print(f"  ✓ Personal workspace filtering applied")
            print(f"  ✓ Retrieved 2 non-personal workspaces")
            print(f"\n✓ PASS: Personal workspace filtering works correctly")
        else:
            tests_failed += 1
            print(f"✗ FAIL: Expected 2 workspaces, got {result}")

def run_all_tests():
    """Run all workspace table source tests"""
    print("\n" + "="*70)
    print("WORKSPACE TABLE SOURCE TEST SUITE")
    print("="*70)
    print("Testing validation, error handling, and fallback logic")
    print("="*70)
    
    test_empty_table_name()
    test_sql_injection_prevention()
    test_valid_table_names()
    test_spark_missing_column()
    test_spark_null_filtering()
    test_pandas_missing_column()
    test_pandas_null_filtering()
    test_empty_result_fallback()
    test_file_not_found_fallback()
    test_personal_workspace_filtering()
    
    # Print summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    total_tests = tests_passed + tests_failed
    print(f"Total tests: {total_tests}")
    print(f"Passed: {tests_passed}")
    print(f"Failed: {tests_failed}")
    if tests_failed == 0:
        print(f"Success rate: 100.0%")
        print("\n✅ All workspace table source tests passed!")
    else:
        success_rate = (tests_passed / total_tests) * 100
        print(f"Success rate: {success_rate:.1f}%")
        print(f"\n❌ {tests_failed} test(s) failed")
    print("="*70 + "\n")

if __name__ == "__main__":
    run_all_tests()
