"""
Test script for Phase 1: Capacity Metadata Implementation

This script verifies that capacity_id, capacity_name, and is_dedicated_capacity
columns are properly captured and added to the output.

Usage:
    python test_capacity_metadata.py
"""

import sys
import os
from pathlib import Path

# Add scanner_api to path
sys.path.insert(0, str(Path(__file__).parent / "scanner_api"))

# Import the main script functions
from fabric_scanner_cloud_connections import (
    get_all_workspaces,
    run_one_batch,
    flatten_scan_payload,
    _create_row
)

def test_capacity_metadata_extraction():
    """Test that capacity metadata is properly extracted from workspace objects."""
    
    print("="*80)
    print("PHASE 1 TEST: Capacity Metadata Extraction")
    print("="*80)
    print()
    
    # Step 1: Get a small sample of workspaces
    print("Step 1: Fetching workspace list (first 5 workspaces)...")
    try:
        all_workspaces = get_all_workspaces(include_personal=True)
        if not all_workspaces:
            print("❌ No workspaces found. Check authentication.")
            return False
        
        # Take only first 5 for testing
        sample_workspaces = all_workspaces[:5]
        print(f"✅ Retrieved {len(sample_workspaces)} workspaces for testing")
        print()
        
        # Step 2: Check if capacity metadata exists in API response
        print("Step 2: Checking capacity metadata in API response...")
        print("-" * 80)
        
        capacity_found = 0
        shared_found = 0
        
        for ws in sample_workspaces:
            ws_name = ws.get('name', 'Unknown')
            capacity_id = ws.get('capacityId')
            is_dedicated = ws.get('isOnDedicatedCapacity', False)
            capacity_name = ws.get('capacityName', 'N/A')
            
            if capacity_id:
                capacity_found += 1
                print(f"📍 {ws_name}")
                print(f"   Capacity ID: {capacity_id}")
                print(f"   Capacity Name: {capacity_name}")
                print(f"   Dedicated: {is_dedicated}")
            else:
                shared_found += 1
                print(f"🌐 {ws_name}")
                print(f"   Capacity: Shared (no dedicated capacity)")
            print()
        
        print(f"Summary: {capacity_found} dedicated capacity, {shared_found} shared/personal")
        print()
        
        # Step 3: Test run_one_batch with small sample
        print("Step 3: Testing batch processing with capacity extraction...")
        print("-" * 80)
        
        # Prepare batch metadata
        batch_meta = [{
            "id": ws.get("id"),
            "name": ws.get("name", ""),
            "type": ws.get("type", "Workspace")
        } for ws in sample_workspaces[:2]]  # Only process 2 to minimize API calls
        
        print(f"Processing batch with {len(batch_meta)} workspaces...")
        payload = run_one_batch(batch_meta)
        
        # Check sidecar has capacity metadata
        sidecar = payload.get("workspace_sidecar", {})
        print(f"✅ Batch processed, sidecar has {len(sidecar)} entries")
        print()
        
        print("Checking sidecar capacity metadata:")
        print("-" * 80)
        
        for ws_id, meta in list(sidecar.items())[:3]:  # Show first 3
            print(f"Workspace: {meta.get('name', 'Unknown')}")
            print(f"  capacity_id: {meta.get('capacity_id', 'NULL')}")
            print(f"  capacity_name: {meta.get('capacity_name', 'NULL')}")
            print(f"  is_dedicated_capacity: {meta.get('is_dedicated_capacity', False)}")
            print()
        
        # Step 4: Test flatten function with capacity columns
        print("Step 4: Testing row flattening with capacity columns...")
        print("-" * 80)
        
        rows = flatten_scan_payload(payload, sidecar)
        
        if not rows:
            print("⚠️  No rows generated (no cloud connections found)")
            print("   This is normal if workspaces have no external connections")
            print()
        else:
            print(f"✅ Generated {len(rows)} rows")
            print()
            
            # Show first row structure
            print("Sample row structure (first connection):")
            print("-" * 80)
            
            first_row = rows[0]
            if hasattr(first_row, 'asDict'):
                row_dict = first_row.asDict()
            else:
                row_dict = first_row
            
            # Show capacity-related fields
            print("Capacity Metadata Fields:")
            print(f"  capacity_id: {row_dict.get('capacity_id', 'MISSING')}")
            print(f"  capacity_name: {row_dict.get('capacity_name', 'MISSING')}")
            print(f"  is_dedicated_capacity: {row_dict.get('is_dedicated_capacity', 'MISSING')}")
            print()
            
            print("Other Workspace Fields:")
            print(f"  workspace_id: {row_dict.get('workspace_id', 'MISSING')}")
            print(f"  workspace_name: {row_dict.get('workspace_name', 'MISSING')}")
            print(f"  workspace_kind: {row_dict.get('workspace_kind', 'MISSING')}")
            print()
            
            print("Connection Fields:")
            print(f"  item_name: {row_dict.get('item_name', 'MISSING')}")
            print(f"  item_type: {row_dict.get('item_type', 'MISSING')}")
            print(f"  connector: {row_dict.get('connector', 'MISSING')}")
            print(f"  target: {row_dict.get('target', 'MISSING')}")
            print()
            
            # Verify all 3 capacity fields are present
            required_fields = ['capacity_id', 'capacity_name', 'is_dedicated_capacity']
            missing_fields = [f for f in required_fields if f not in row_dict]
            
            if missing_fields:
                print(f"❌ MISSING FIELDS: {', '.join(missing_fields)}")
                return False
            else:
                print(f"✅ All capacity fields present in output!")
        
        print()
        print("="*80)
        print("✅ PHASE 1 TEST PASSED!")
        print("="*80)
        print()
        print("Next Steps:")
        print("1. Review the capacity metadata above")
        print("2. Run a small incremental scan to verify end-to-end flow")
        print("3. Check output files for capacity columns")
        print()
        print("Example incremental scan command:")
        print("  python fabric_scanner_cloud_connections.py --incremental --hours 24")
        print()
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_row_creation():
    """Test _create_row function with capacity fields."""
    
    print()
    print("="*80)
    print("BONUS TEST: Row Creation with Capacity Fields")
    print("="*80)
    print()
    
    # Create a test row dictionary
    test_data = {
        "workspace_id": "test-ws-123",
        "workspace_name": "Test Workspace",
        "workspace_kind": "workspace",
        "workspace_users": "admin@example.com",
        "capacity_id": "test-cap-456",
        "capacity_name": "Premium-P1",
        "is_dedicated_capacity": True,
        "item_id": "test-item-789",
        "item_name": "Test Dataset",
        "item_type": "SemanticModel",
        "item_creator": "creator@example.com",
        "item_modified_by": "modifier@example.com",
        "item_modified_date": "2026-01-21T10:00:00Z",
        "connector": "Sql",
        "target": "sql.example.com/db",
        "server": "sql.example.com",
        "database": "db",
        "endpoint": None,
        "connection_scope": "Cloud",
        "cloud": True,
        "generation": None
    }
    
    print("Creating test row with capacity metadata...")
    row = _create_row(test_data)
    
    if hasattr(row, 'asDict'):
        row_dict = row.asDict()
    else:
        row_dict = row
    
    print("✅ Row created successfully!")
    print()
    print("Capacity fields in created row:")
    print(f"  capacity_id: {row_dict.get('capacity_id')}")
    print(f"  capacity_name: {row_dict.get('capacity_name')}")
    print(f"  is_dedicated_capacity: {row_dict.get('is_dedicated_capacity')}")
    print()
    
    # Verify fields match
    if (row_dict.get('capacity_id') == test_data['capacity_id'] and
        row_dict.get('capacity_name') == test_data['capacity_name'] and
        row_dict.get('is_dedicated_capacity') == test_data['is_dedicated_capacity']):
        print("✅ All capacity fields match expected values!")
        return True
    else:
        print("❌ Capacity fields do not match!")
        return False


if __name__ == "__main__":
    print()
    print("🧪 CAPACITY METADATA PHASE 1 TEST SUITE")
    print()
    
    # Run tests
    test1_passed = test_row_creation()
    test2_passed = test_capacity_metadata_extraction()
    
    print()
    print("="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Row Creation Test: {'✅ PASSED' if test1_passed else '❌ FAILED'}")
    print(f"End-to-End Test: {'✅ PASSED' if test2_passed else '❌ FAILED'}")
    print()
    
    if test1_passed and test2_passed:
        print("🎉 ALL TESTS PASSED! Phase 1 implementation is working correctly.")
        print()
        print("You can now proceed with:")
        print("  • Running production scans with capacity metadata")
        print("  • Phase 2: Capacity grouping (optional)")
        print("  • Phase 3: Parallel capacity scanning (optional)")
        sys.exit(0)
    else:
        print("⚠️  Some tests failed. Review the errors above.")
        sys.exit(1)
