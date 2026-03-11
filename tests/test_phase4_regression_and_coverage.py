"""
Phase 4 — Regression tests for bug fixes, security controls, refactored code, and coverage gaps.

Tests organized by area:
  P0 Regression:  get_all_workspaces(modified_since=...), credential validation
  Security:       _validate_path_for_sql(), _validate_sql_identifier()
  Refactored:     run_one_batch(scan_mode=...), _build_connection_row(), _save_data(mode="merge")
  Coverage:       flatten_scan_payload(), _get_http_session(), ConnectionHashTracker vectorized hashes
"""

import sys
import hashlib
import threading
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock Fabric-specific modules before importing scanner
sys.modules["notebookutils"] = MagicMock()
sys.modules["notebookutils.mssparkutils"] = MagicMock()
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()

import fabric_scanner_cloud_connections as scanner

# Ensure we run in local/pandas mode for most tests
scanner.HEADERS = {
    "Authorization": "Bearer mock_token",
    "Content-Type": "application/json",
}
scanner.ACCESS_TOKEN = "mock_token"


# ═══════════════════════════════════════════════════════════════════
#  P0 REGRESSION: get_all_workspaces(modified_since=...) — bug fix
# ═══════════════════════════════════════════════════════════════════


class TestGetAllWorkspacesModifiedSince:
    """Regression tests for the P0 bug fix: modified_since param was never wired up."""

    def _mock_response(self, json_data, status_code=200):
        resp = Mock()
        resp.json.return_value = json_data
        resp.status_code = status_code
        resp.raise_for_status = Mock()
        return resp

    @patch.object(scanner, "refresh_access_token")
    @patch.object(scanner, "_track_api_call")
    @patch.object(scanner, "_get_http_session")
    def test_modified_since_passed_to_api_params(
        self, mock_session, mock_track, mock_refresh
    ):
        """modified_since should appear as modifiedSince query parameter."""
        mock_get = Mock(return_value=self._mock_response([{"id": "ws-1"}]))
        mock_session.return_value.get = mock_get

        scanner.get_all_workspaces(modified_since="2025-07-01T00:00:00Z")

        call_kwargs = mock_get.call_args
        assert "params" in call_kwargs.kwargs or len(call_kwargs.args) > 1
        params = call_kwargs.kwargs.get("params") or call_kwargs[1]
        assert params.get("modifiedSince") == "2025-07-01T00:00:00Z"

    @patch.object(scanner, "refresh_access_token")
    @patch.object(scanner, "_track_api_call")
    @patch.object(scanner, "_get_http_session")
    def test_modified_since_none_omits_param(
        self, mock_session, mock_track, mock_refresh
    ):
        """When modified_since is None, modifiedSince should not be in params."""
        mock_get = Mock(return_value=self._mock_response([]))
        mock_session.return_value.get = mock_get

        scanner.get_all_workspaces(modified_since=None)

        params = mock_get.call_args.kwargs.get("params", {})
        assert "modifiedSince" not in params

    @patch.object(scanner, "refresh_access_token")
    @patch.object(scanner, "_track_api_call")
    @patch.object(scanner, "_get_http_session")
    def test_exclude_personal_workspaces_param(
        self, mock_session, mock_track, mock_refresh
    ):
        """include_personal=False → excludePersonalWorkspaces=true."""
        mock_get = Mock(return_value=self._mock_response([]))
        mock_session.return_value.get = mock_get

        scanner.get_all_workspaces(
            include_personal=False, modified_since="2025-07-01T00:00:00Z"
        )

        params = mock_get.call_args.kwargs.get("params", {})
        assert params["excludePersonalWorkspaces"] == "true"
        assert params["modifiedSince"] == "2025-07-01T00:00:00Z"

    @patch.object(scanner, "refresh_access_token")
    @patch.object(scanner, "_track_api_call")
    @patch.object(scanner, "_get_http_session")
    def test_returns_list_response(self, mock_session, mock_track, mock_refresh):
        """API returning a list directly should be handled."""
        mock_get = Mock(
            return_value=self._mock_response([{"id": "ws-1"}, {"id": "ws-2"}])
        )
        mock_session.return_value.get = mock_get

        result = scanner.get_all_workspaces()
        assert len(result) == 2
        assert result[0]["id"] == "ws-1"

    @patch.object(scanner, "refresh_access_token")
    @patch.object(scanner, "_track_api_call")
    @patch.object(scanner, "_get_http_session")
    def test_returns_dict_wrapper_response(
        self, mock_session, mock_track, mock_refresh
    ):
        """API returning {workspaces: [...]} should be handled."""
        mock_get = Mock(
            return_value=self._mock_response({"workspaces": [{"id": "ws-1"}]})
        )
        mock_session.return_value.get = mock_get

        result = scanner.get_all_workspaces()
        assert len(result) == 1


# ═══════════════════════════════════════════════════════════════════
#  P0 REGRESSION: Credential validation — empty-string rejection
# ═══════════════════════════════════════════════════════════════════


class TestCredentialValidation:
    """Regression tests for the P0 security fix: empty credentials now raise ValueError."""

    @patch.object(scanner, "TENANT_ID", "")
    @patch.object(scanner, "CLIENT_ID", "test-client")
    @patch.object(scanner, "CLIENT_SECRET", "test-secret")
    def test_raises_when_tenant_id_empty(self):
        with pytest.raises(ValueError, match="FABRIC_SP_TENANT_ID"):
            scanner.get_access_token_spn()

    @patch.object(scanner, "TENANT_ID", "test-tenant")
    @patch.object(scanner, "CLIENT_ID", "")
    @patch.object(scanner, "CLIENT_SECRET", "test-secret")
    def test_raises_when_client_id_empty(self):
        with pytest.raises(ValueError, match="FABRIC_SP_CLIENT_ID"):
            scanner.get_access_token_spn()

    @patch.object(scanner, "TENANT_ID", "test-tenant")
    @patch.object(scanner, "CLIENT_ID", "test-client")
    @patch.object(scanner, "CLIENT_SECRET", "")
    def test_raises_when_client_secret_empty(self):
        with pytest.raises(ValueError, match="FABRIC_SP_CLIENT_SECRET"):
            scanner.get_access_token_spn()

    @patch.object(scanner, "TENANT_ID", "")
    @patch.object(scanner, "CLIENT_ID", "")
    @patch.object(scanner, "CLIENT_SECRET", "")
    def test_raises_lists_all_missing(self):
        with pytest.raises(ValueError) as exc_info:
            scanner.get_access_token_spn()
        msg = str(exc_info.value)
        assert "FABRIC_SP_TENANT_ID" in msg
        assert "FABRIC_SP_CLIENT_ID" in msg
        assert "FABRIC_SP_CLIENT_SECRET" in msg


# ═══════════════════════════════════════════════════════════════════
#  SECURITY: _validate_path_for_sql()
# ═══════════════════════════════════════════════════════════════════


class TestValidatePathForSql:
    """Tests for the new SQL-LOCATION path validator added in Phase 2."""

    def test_rejects_single_quote(self):
        with pytest.raises(ValueError, match="single quotes"):
            scanner._validate_path_for_sql("abc/x'y/z")

    def test_rejects_double_quote(self):
        with pytest.raises(ValueError, match="double quotes"):
            scanner._validate_path_for_sql('abc/"bad"/z')

    def test_rejects_semicolon(self):
        with pytest.raises(ValueError, match="semicolons"):
            scanner._validate_path_for_sql("abc;DROP TABLE x")

    def test_rejects_backtick(self):
        with pytest.raises(ValueError, match="backticks"):
            scanner._validate_path_for_sql("abc/`bad`/z")

    def test_rejects_backslash(self):
        with pytest.raises(ValueError, match="backslashes"):
            scanner._validate_path_for_sql("abc\\bad\\z")

    def test_rejects_sql_keyword_drop(self):
        with pytest.raises(ValueError, match="SQL keywords"):
            scanner._validate_path_for_sql("/mnt/data/DROP TABLE test")

    def test_rejects_sql_keyword_select(self):
        with pytest.raises(ValueError, match="SQL keywords"):
            scanner._validate_path_for_sql("/mnt/data/SELECT * FROM")

    def test_rejects_sql_keyword_union(self):
        with pytest.raises(ValueError, match="SQL keywords"):
            scanner._validate_path_for_sql("/data/ UNION attack")

    def test_rejects_sql_keyword_case_insensitive(self):
        with pytest.raises(ValueError, match="SQL keywords"):
            scanner._validate_path_for_sql("/data/drop table hack")

    def test_rejects_sql_comment_double_dash(self):
        with pytest.raises(ValueError, match="comment sequences"):
            scanner._validate_path_for_sql("/data/path--comment")

    def test_rejects_sql_comment_block(self):
        with pytest.raises(ValueError, match="comment sequences"):
            scanner._validate_path_for_sql("/data/path/*comment*/")

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError, match="non-empty string"):
            scanner._validate_path_for_sql("")

    def test_rejects_none(self):
        with pytest.raises(ValueError, match="non-empty string"):
            scanner._validate_path_for_sql(None)

    def test_accepts_valid_unix_path(self):
        scanner._validate_path_for_sql("/mnt/lakehouse/curated/connections")

    def test_accepts_valid_abfss_path(self):
        scanner._validate_path_for_sql(
            "abfss://container@storage.dfs.core.windows.net/data"
        )

    def test_accepts_path_with_hyphens_and_dots(self):
        scanner._validate_path_for_sql("/mnt/lake-house/data.v2/output")


# ═══════════════════════════════════════════════════════════════════
#  SECURITY: _validate_sql_identifier()
# ═══════════════════════════════════════════════════════════════════


class TestValidateSqlIdentifier:
    """Tests for SQL identifier validator protecting table names in Spark SQL."""

    def test_rejects_semicolon(self):
        with pytest.raises(ValueError):
            scanner._validate_sql_identifier("table; DROP TABLE x--")

    def test_rejects_space(self):
        with pytest.raises(ValueError):
            scanner._validate_sql_identifier("table name")

    def test_rejects_hyphen(self):
        with pytest.raises(ValueError):
            scanner._validate_sql_identifier("table-name")

    def test_rejects_at_symbol(self):
        with pytest.raises(ValueError):
            scanner._validate_sql_identifier("table@name")

    def test_rejects_quote(self):
        with pytest.raises(ValueError):
            scanner._validate_sql_identifier("table'name")

    def test_rejects_path_traversal(self):
        with pytest.raises(ValueError):
            scanner._validate_sql_identifier("../../../etc/passwd")

    def test_accepts_simple_name(self):
        scanner._validate_sql_identifier("workspace_inventory")

    def test_accepts_dotted_name(self):
        scanner._validate_sql_identifier("dbo.workspace_inventory")

    def test_accepts_fully_qualified(self):
        scanner._validate_sql_identifier("my_lakehouse.dbo.connections")

    def test_accepts_numeric_suffix(self):
        scanner._validate_sql_identifier("tenant_cloud_connections_2025")


# ═══════════════════════════════════════════════════════════════════
#  REFACTORED: run_one_batch(scan_mode="full" | "incremental")
# ═══════════════════════════════════════════════════════════════════


class TestRunOneBatch:
    """Tests for the consolidated run_one_batch after Phase 3 refactor."""

    def _make_payload(self, *, has_capacity=True):
        """Build a realistic scan result payload."""
        ws = {
            "id": "ws-001",
            "name": "Test Workspace",
            "type": "Workspace",
            "capacityId": "cap-abc" if has_capacity else None,
            "capacityName": "Premium P1" if has_capacity else None,
            "isOnDedicatedCapacity": has_capacity,
            "users": [
                {
                    "emailAddress": "admin@contoso.com",
                    "workspaceUserAccessRight": "Admin",
                },
                {
                    "emailAddress": "viewer@contoso.com",
                    "workspaceUserAccessRight": "Viewer",
                },
            ],
            "items": [],
        }
        return {"workspaces": [ws]}

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "mssparkutils", None)
    @patch.object(scanner, "UPLOAD_TO_LAKEHOUSE", False)
    @patch.object(scanner, "read_scan_result")
    @patch.object(scanner, "poll_scan_status")
    @patch.object(scanner, "post_workspace_info", return_value="scan-001")
    def test_full_mode_includes_capacity_metadata(
        self, mock_post, mock_poll, mock_read
    ):
        """Full mode sidecar should contain capacity_id, capacity_name, is_dedicated_capacity."""
        mock_read.return_value = self._make_payload(has_capacity=True)

        batch_meta = [{"id": "ws-001", "name": "Test Workspace", "type": "Workspace"}]
        result = scanner.run_one_batch(batch_meta, scan_mode="full")

        sidecar = result.get("workspace_sidecar", {})
        ws_info = sidecar.get("ws-001", {})
        assert "capacity_id" in ws_info
        assert ws_info["capacity_id"] == "cap-abc"
        assert ws_info["capacity_name"] == "Premium P1"
        assert ws_info["is_dedicated_capacity"] is True

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "mssparkutils", None)
    @patch.object(scanner, "UPLOAD_TO_LAKEHOUSE", False)
    @patch.object(scanner, "read_scan_result")
    @patch.object(scanner, "poll_scan_status")
    @patch.object(scanner, "post_workspace_info", return_value="scan-002")
    def test_incremental_mode_lighter_sidecar(self, mock_post, mock_poll, mock_read):
        """Incremental mode sidecar should NOT have capacity metadata from scan result."""
        mock_read.return_value = self._make_payload(has_capacity=True)

        batch_meta = [{"id": "ws-001", "name": "Test Workspace", "type": "Workspace"}]
        result = scanner.run_one_batch(batch_meta, scan_mode="incremental")

        sidecar = result.get("workspace_sidecar", {})
        ws_info = sidecar.get("ws-001", {})
        # Incremental should still have name/kind/users from batch_meta fallback
        assert "name" in ws_info
        # But capacity fields should not be populated from the scan result
        assert ws_info.get("capacity_id") is None or "capacity_id" not in ws_info

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "mssparkutils", None)
    @patch.object(scanner, "UPLOAD_TO_LAKEHOUSE", False)
    @patch.object(scanner, "read_scan_result")
    @patch.object(scanner, "poll_scan_status")
    @patch.object(scanner, "post_workspace_info", return_value="scan-003")
    def test_backward_compatible_wrapper(self, mock_post, mock_poll, mock_read):
        """run_one_batch_incremental() should delegate to run_one_batch(scan_mode='incremental')."""
        mock_read.return_value = self._make_payload()

        batch_meta = [{"id": "ws-001", "name": "Test", "type": "Workspace"}]
        result = scanner.run_one_batch_incremental(batch_meta)

        assert isinstance(result, dict)
        assert "workspaces" in result

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "mssparkutils", None)
    @patch.object(scanner, "UPLOAD_TO_LAKEHOUSE", False)
    @patch.object(scanner, "read_scan_result")
    @patch.object(scanner, "poll_scan_status")
    @patch.object(scanner, "post_workspace_info", return_value="scan-004")
    def test_user_extraction(self, mock_post, mock_poll, mock_read):
        """run_one_batch should extract admin/member emails into sidecar."""
        mock_read.return_value = self._make_payload()

        batch_meta = [{"id": "ws-001", "name": "Test", "type": "Workspace"}]
        result = scanner.run_one_batch(batch_meta, scan_mode="full")

        sidecar = result.get("workspace_sidecar", {})
        users_str = sidecar.get("ws-001", {}).get("users", "")
        assert "admin@contoso.com" in (users_str or "")


# ═══════════════════════════════════════════════════════════════════
#  REFACTORED: _build_connection_row()
# ═══════════════════════════════════════════════════════════════════


class TestBuildConnectionRow:
    """Tests for the DRY helper extracted in Phase 3 refactor."""

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    def test_produces_correct_dict_keys(self):
        row = scanner._build_connection_row(
            "ws-1",
            "WS Name",
            "workspace",
            "admin@co.com",
            "cap-1",
            "P1",
            True,
            "item-1",
            "My Model",
            "creator@co.com",
            "editor@co.com",
            "2025-07-01",
            item_type="SemanticModel",
            connector="azuresqldatabase",
            server="sql.database.windows.net",
            database="TestDB",
        )
        assert isinstance(row, dict)
        expected_keys = {
            "workspace_id",
            "workspace_name",
            "workspace_kind",
            "workspace_users",
            "capacity_id",
            "capacity_name",
            "is_dedicated_capacity",
            "item_id",
            "item_name",
            "item_type",
            "item_creator",
            "item_modified_by",
            "item_modified_date",
            "connector",
            "target",
            "server",
            "database",
            "endpoint",
            "connection_scope",
            "cloud",
            "generation",
        }
        assert set(row.keys()) == expected_keys

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    def test_cloud_flag_from_cloud_connector(self):
        """Connector in CLOUD_CONNECTORS → cloud=True."""
        row = scanner._build_connection_row(
            "ws-1",
            "WS",
            "workspace",
            None,
            None,
            "Shared",
            False,
            "item-1",
            "Model",
            None,
            None,
            None,
            connector="azuresqldatabase",
        )
        assert row["cloud"] is True

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    def test_cloud_flag_from_connection_scope(self):
        """connection_scope='Cloud' → cloud=True."""
        row = scanner._build_connection_row(
            "ws-1",
            "WS",
            "workspace",
            None,
            None,
            "Shared",
            False,
            "item-1",
            "Model",
            None,
            None,
            None,
            connector="customconnector",
            connection_scope="Cloud",
        )
        assert row["cloud"] is True

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    def test_on_prem_scope_not_cloud(self):
        """On-prem scope + unknown connector → cloud depends on CLOUD_CONNECTORS."""
        row = scanner._build_connection_row(
            "ws-1",
            "WS",
            "workspace",
            None,
            None,
            "Shared",
            False,
            "item-1",
            "Model",
            None,
            None,
            None,
            connector="sqlserver",
            connection_scope="OnPremViaGateway",
        )
        # sqlserver is NOT in CLOUD_CONNECTORS and scope is not Cloud
        assert row["cloud"] is False

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    def test_target_string_built_correctly(self):
        """target should combine server, database, endpoint."""
        row = scanner._build_connection_row(
            "ws-1",
            "WS",
            "workspace",
            None,
            None,
            "Shared",
            False,
            "item-1",
            "Model",
            None,
            None,
            None,
            server="myserver.database.windows.net",
            database="mydb",
        )
        assert "Server: myserver.database.windows.net" in row["target"]
        assert "Database: mydb" in row["target"]


# ═══════════════════════════════════════════════════════════════════
#  REFACTORED: _save_data(mode="merge") — dedup consolidation
# ═══════════════════════════════════════════════════════════════════


class TestSaveDataMerge:
    """Tests for the new merge mode added in Phase 3 refactor."""

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "SPARK_AVAILABLE", False)
    @patch.object(scanner, "PANDAS_AVAILABLE", True)
    @patch.object(scanner, "UPLOAD_TO_LAKEHOUSE", False)
    def test_merge_deduplicates_rows(self, tmp_path):
        """Merge mode should combine old+new data and deduplicate on _DEDUP_COLS."""
        import pandas as pd

        scanner.pd = pd

        curated_dir = str(tmp_path)
        table_name = "test_connections"

        # First write — overwrite mode
        rows_v1 = [
            {
                "workspace_id": "ws-1",
                "item_id": "i-1",
                "connector": "azuresqldatabase",
                "server": "srv1",
                "database": "db1",
                "endpoint": None,
                "extra": "old_value",
            },
            {
                "workspace_id": "ws-2",
                "item_id": "i-2",
                "connector": "snowflake",
                "server": "srv2",
                "database": "db2",
                "endpoint": None,
                "extra": "keep",
            },
        ]
        count1 = scanner._save_data(rows_v1, curated_dir, table_name, mode="overwrite")
        assert count1 == 2

        # Second write — merge mode with one duplicate and one new row
        rows_v2 = [
            {
                "workspace_id": "ws-1",
                "item_id": "i-1",
                "connector": "azuresqldatabase",
                "server": "srv1",
                "database": "db1",
                "endpoint": None,
                "extra": "new_value",
            },
            {
                "workspace_id": "ws-3",
                "item_id": "i-3",
                "connector": "rest",
                "server": None,
                "database": None,
                "endpoint": "https://api.example.com",
                "extra": "new",
            },
        ]
        count2 = scanner._save_data(rows_v2, curated_dir, table_name, mode="merge")

        # Should have 3 unique rows after dedup (ws-1 deduped, ws-2 from old, ws-3 new)
        assert count2 == 3

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "SPARK_AVAILABLE", False)
    @patch.object(scanner, "PANDAS_AVAILABLE", True)
    @patch.object(scanner, "UPLOAD_TO_LAKEHOUSE", False)
    def test_merge_when_no_existing_data(self, tmp_path):
        """Merge should work even if no prior data exists (first run)."""
        import pandas as pd

        scanner.pd = pd

        curated_dir = str(tmp_path)
        rows = [
            {
                "workspace_id": "ws-1",
                "item_id": "i-1",
                "connector": "sql",
                "server": "s",
                "database": "d",
                "endpoint": None,
            },
        ]
        count = scanner._save_data(rows, curated_dir, "test_conn", mode="merge")
        assert count == 1

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "SPARK_AVAILABLE", False)
    @patch.object(scanner, "PANDAS_AVAILABLE", True)
    @patch.object(scanner, "UPLOAD_TO_LAKEHOUSE", False)
    def test_connector_normalized_to_lowercase(self, tmp_path):
        """connector column should be lowercased."""
        import pandas as pd

        scanner.pd = pd

        curated_dir = str(tmp_path)
        rows = [
            {
                "workspace_id": "ws-1",
                "item_id": "i-1",
                "connector": "AzureSqlDatabase",
                "server": "s",
                "database": "d",
                "endpoint": None,
            },
        ]
        scanner._save_data(rows, curated_dir, "test_conn", mode="overwrite")

        parquet_file = Path(curated_dir) / "test_conn.parquet"
        df = pd.read_parquet(parquet_file)
        assert df.iloc[0]["connector"] == "azuresqldatabase"


# ═══════════════════════════════════════════════════════════════════
#  COVERAGE: flatten_scan_payload() — realistic payloads
# ═══════════════════════════════════════════════════════════════════


class TestFlattenScanPayload:
    """Tests for flatten_scan_payload with realistic mock payloads per item type."""

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "DEBUG_MODE", False)
    def test_semantic_model_datasources(self):
        """SemanticModel items should extract datasource connections."""
        payload = {
            "workspaces": [
                {
                    "id": "ws-1",
                    "items": [
                        {
                            "id": "ds-1",
                            "name": "Sales Model",
                            "type": "SemanticModel",
                            "createdBy": "user@co.com",
                            "modifiedBy": "user@co.com",
                            "modifiedDateTime": "2025-07-01T12:00:00Z",
                            "datasources": [
                                {
                                    "connectionDetails": {
                                        "datasourceType": "Sql",
                                        "server": "sql-server.database.windows.net",
                                        "database": "SalesDB",
                                    }
                                },
                                {
                                    "connectionDetails": {
                                        "datasourceType": "AzureSqlDatabase",
                                        "server": "analytics.database.windows.net",
                                        "database": "AnalyticsDB",
                                    }
                                },
                            ],
                        }
                    ],
                }
            ]
        }
        sidecar = {
            "ws-1": {
                "name": "Sales WS",
                "kind": "workspace",
                "users": "admin@co.com",
                "capacity_id": "cap-1",
                "capacity_name": "P1",
                "is_dedicated_capacity": True,
            }
        }

        rows = scanner.flatten_scan_payload(payload, sidecar)
        assert len(rows) == 2
        assert rows[0]["item_type"] == "SemanticModel"
        assert rows[0]["server"] == "sql-server.database.windows.net"
        assert rows[1]["database"] == "AnalyticsDB"

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "DEBUG_MODE", False)
    def test_dataflow_sources(self):
        """Dataflow items should extract sources/entities."""
        payload = {
            "workspaces": [
                {
                    "id": "ws-1",
                    "items": [
                        {
                            "id": "df-1",
                            "name": "ETL Flow",
                            "type": "Dataflow",
                            "createdBy": "user@co.com",
                            "modifiedBy": None,
                            "modifiedDateTime": None,
                            "generation": 2,
                            "sources": [
                                {
                                    "type": "Snowflake",
                                    "url": "https://account.snowflakecomputing.com",
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        sidecar = {
            "ws-1": {
                "name": "WS",
                "kind": "workspace",
                "users": None,
                "capacity_id": None,
                "capacity_name": "Shared",
                "is_dedicated_capacity": False,
            }
        }

        rows = scanner.flatten_scan_payload(payload, sidecar)
        assert len(rows) == 1
        assert rows[0]["item_type"] == "Dataflow"
        assert rows[0]["connector"] == "snowflake"
        assert rows[0]["endpoint"] == "https://account.snowflakecomputing.com"
        assert rows[0]["generation"] == 2

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "DEBUG_MODE", False)
    def test_pipeline_activities(self):
        """Pipeline items should extract activities as connections."""
        payload = {
            "workspaces": [
                {
                    "id": "ws-1",
                    "items": [
                        {
                            "id": "pl-1",
                            "name": "Data Pipeline",
                            "type": "Pipeline",
                            "createdBy": "dev@co.com",
                            "modifiedBy": "dev@co.com",
                            "modifiedDateTime": "2025-06-15",
                            "activities": [
                                {
                                    "type": "CopyActivity",
                                    "linkedService": {
                                        "type": "AzureSqlDatabase",
                                        "url": "https://sql.example.com",
                                    },
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        sidecar = {
            "ws-1": {
                "name": "WS",
                "kind": "workspace",
                "users": None,
                "capacity_id": None,
                "capacity_name": "Shared",
                "is_dedicated_capacity": False,
            }
        }

        rows = scanner.flatten_scan_payload(payload, sidecar)
        assert len(rows) == 1
        assert rows[0]["item_type"] == "Pipeline"
        assert rows[0]["connector"] == "azuresqldatabase"

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "DEBUG_MODE", False)
    def test_lakehouse_connections(self):
        """Lakehouse items should extract connections + lineage references."""
        payload = {
            "workspaces": [
                {
                    "id": "ws-1",
                    "items": [
                        {
                            "id": "lh-1",
                            "name": "My Lakehouse",
                            "type": "Lakehouse",
                            "createdBy": "creator@co.com",
                            "modifiedBy": None,
                            "modifiedDateTime": None,
                            "connections": [
                                {
                                    "type": "OneLake",
                                    "url": "https://onelake.dfs.fabric.microsoft.com",
                                    "isCloud": True,
                                }
                            ],
                            "lineage": [],
                        }
                    ],
                }
            ]
        }
        sidecar = {
            "ws-1": {
                "name": "WS",
                "kind": "workspace",
                "users": None,
                "capacity_id": None,
                "capacity_name": "Shared",
                "is_dedicated_capacity": False,
            }
        }

        rows = scanner.flatten_scan_payload(payload, sidecar)
        assert len(rows) == 1
        assert rows[0]["item_type"] == "Lakehouse"
        assert rows[0]["connector"] == "onelake"

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "DEBUG_MODE", False)
    def test_unknown_item_type_fallback(self):
        """Unrecognized item types should produce a single fallback row."""
        payload = {
            "workspaces": [
                {
                    "id": "ws-1",
                    "items": [
                        {
                            "id": "uk-1",
                            "name": "Unknown Thing",
                            "type": "FutureFeature",
                            "createdBy": None,
                            "modifiedBy": None,
                            "modifiedDateTime": None,
                        }
                    ],
                }
            ]
        }
        sidecar = {
            "ws-1": {
                "name": "WS",
                "kind": "workspace",
                "users": None,
                "capacity_id": None,
                "capacity_name": "Shared",
                "is_dedicated_capacity": False,
            }
        }

        rows = scanner.flatten_scan_payload(payload, sidecar)
        assert len(rows) == 1
        assert rows[0]["item_type"] == "Futurefeature"
        assert rows[0]["cloud"] is True

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "DEBUG_MODE", False)
    def test_non_dict_payload_returns_empty(self):
        """Non-dict payload should return empty list without crashing."""
        rows = scanner.flatten_scan_payload("not a dict", {})
        assert rows == []

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "DEBUG_MODE", False)
    def test_gateway_connection_scope(self):
        """SemanticModel with gatewayId → OnPremViaGateway scope."""
        payload = {
            "workspaces": [
                {
                    "id": "ws-1",
                    "items": [
                        {
                            "id": "ds-1",
                            "name": "OnPrem Model",
                            "type": "SemanticModel",
                            "createdBy": None,
                            "modifiedBy": None,
                            "modifiedDateTime": None,
                            "datasources": [
                                {
                                    "gatewayId": "gw-001",
                                    "connectionDetails": {
                                        "datasourceType": "SqlServer",
                                        "server": "onprem-sql.local",
                                        "database": "InternalDB",
                                    },
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        sidecar = {
            "ws-1": {
                "name": "WS",
                "kind": "workspace",
                "users": None,
                "capacity_id": None,
                "capacity_name": "Shared",
                "is_dedicated_capacity": False,
            }
        }

        rows = scanner.flatten_scan_payload(payload, sidecar)
        assert len(rows) == 1
        assert rows[0]["connection_scope"] == "OnPremViaGateway"

    @patch.object(scanner, "RUNNING_IN_FABRIC", False)
    @patch.object(scanner, "DEBUG_MODE", False)
    def test_multi_workspace_payload(self):
        """Payload with multiple workspaces should flatten all."""
        payload = {
            "workspaces": [
                {
                    "id": "ws-1",
                    "items": [
                        {
                            "id": "ds-1",
                            "name": "M1",
                            "type": "SemanticModel",
                            "createdBy": None,
                            "modifiedBy": None,
                            "modifiedDateTime": None,
                            "datasources": [
                                {
                                    "connectionDetails": {
                                        "datasourceType": "Sql",
                                        "server": "s1",
                                    }
                                }
                            ],
                        }
                    ],
                },
                {
                    "id": "ws-2",
                    "items": [
                        {
                            "id": "ds-2",
                            "name": "M2",
                            "type": "SemanticModel",
                            "createdBy": None,
                            "modifiedBy": None,
                            "modifiedDateTime": None,
                            "datasources": [
                                {
                                    "connectionDetails": {
                                        "datasourceType": "Sql",
                                        "server": "s2",
                                    }
                                }
                            ],
                        }
                    ],
                },
            ]
        }
        sidecar = {
            "ws-1": {
                "name": "WS1",
                "kind": "workspace",
                "users": None,
                "capacity_id": None,
                "capacity_name": "Shared",
                "is_dedicated_capacity": False,
            },
            "ws-2": {
                "name": "WS2",
                "kind": "workspace",
                "users": None,
                "capacity_id": None,
                "capacity_name": "Shared",
                "is_dedicated_capacity": False,
            },
        }

        rows = scanner.flatten_scan_payload(payload, sidecar)
        assert len(rows) == 2
        ws_ids = {r["workspace_id"] for r in rows}
        assert ws_ids == {"ws-1", "ws-2"}


# ═══════════════════════════════════════════════════════════════════
#  COVERAGE: _get_http_session() — singleton + thread safety
# ═══════════════════════════════════════════════════════════════════


class TestGetHttpSession:
    """Tests for the shared HTTP session added in Phase 2 code-reviewer."""

    def test_returns_requests_session(self):
        import requests

        session = scanner._get_http_session()
        assert isinstance(session, requests.Session)

    def test_returns_same_instance(self):
        """Should return the same session object on repeated calls (singleton)."""
        s1 = scanner._get_http_session()
        s2 = scanner._get_http_session()
        assert s1 is s2

    def test_thread_safe_same_instance(self):
        """Multiple threads should all get the same session instance."""
        results = []

        def get_session():
            results.append(id(scanner._get_http_session()))

        threads = [threading.Thread(target=get_session) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(set(results)) == 1, (
            "All threads should get the same session instance"
        )


# ═══════════════════════════════════════════════════════════════════
#  COVERAGE: ConnectionHashTracker vectorized get_stored_hashes()
# ═══════════════════════════════════════════════════════════════════


class TestConnectionHashTrackerVectorized:
    """Tests for the vectorized get_stored_hashes() replacing iterrows()."""

    def test_vectorized_produces_correct_dict(self, tmp_path):
        """Vectorized dict conversion should produce {ws_id: {hash, last_scan_time}}."""
        import pandas as pd

        # Create a mock parquet file with hash data
        data = {
            "workspace_id": ["ws-001", "ws-002", "ws-003"],
            "connection_hash": ["abc123", "def456", "ghi789"],
            "last_scan_time": [
                "2025-07-01T00:00:00Z",
                "2025-07-02T00:00:00Z",
                "2025-07-03T00:00:00Z",
            ],
        }
        df = pd.DataFrame(data)
        hash_file = tmp_path / "workspace_connection_hashes.parquet"
        df.to_parquet(hash_file, index=False)

        # Create tracker pointing at tmp_path
        config = type(
            "Config", (), {"curated_dir": str(tmp_path), "tenant_id": "test"}
        )()
        tracker = scanner.ConnectionHashTracker(config, running_in_fabric=False)

        stored = tracker.get_stored_hashes()

        assert len(stored) == 3
        assert stored["ws-001"]["hash"] == "abc123"
        assert stored["ws-001"]["last_scan_time"] == "2025-07-01T00:00:00Z"
        assert stored["ws-002"]["hash"] == "def456"
        assert stored["ws-003"]["hash"] == "ghi789"

    def test_vectorized_matches_iterrows_approach(self, tmp_path):
        """Vectorized result should be identical to old iterrows approach."""
        import pandas as pd

        data = {
            "workspace_id": [f"ws-{i}" for i in range(100)],
            "connection_hash": [
                hashlib.sha256(f"hash-{i}".encode()).hexdigest() for i in range(100)
            ],
            "last_scan_time": [
                f"2025-07-{(i % 28) + 1:02d}T00:00:00Z" for i in range(100)
            ],
        }
        df = pd.DataFrame(data)
        hash_file = tmp_path / "workspace_connection_hashes.parquet"
        df.to_parquet(hash_file, index=False)

        # Old approach (iterrows)
        df_read = pd.read_parquet(hash_file)
        old_result = {}
        for _, row in df_read.iterrows():
            old_result[row["workspace_id"]] = {
                "hash": row["connection_hash"],
                "last_scan_time": row["last_scan_time"],
            }

        # New approach (vectorized via tracker)
        config = type(
            "Config", (), {"curated_dir": str(tmp_path), "tenant_id": "test"}
        )()
        tracker = scanner.ConnectionHashTracker(config, running_in_fabric=False)
        new_result = tracker.get_stored_hashes()

        assert old_result == new_result

    def test_empty_file_returns_empty_dict(self, tmp_path):
        """If no hash file exists, should return empty dict."""
        config = type(
            "Config", (), {"curated_dir": str(tmp_path), "tenant_id": "test"}
        )()
        tracker = scanner.ConnectionHashTracker(config, running_in_fabric=False)

        stored = tracker.get_stored_hashes()
        assert stored == {}

    def test_calculate_workspace_hash_deterministic(self):
        """Same connections in any order should produce the same hash."""
        config = type("Config", (), {"curated_dir": "/tmp", "tenant_id": "test"})()
        tracker = scanner.ConnectionHashTracker(config)

        connections_a = [
            {"connector": "sql", "server": "srv1", "database": "db1"},
            {"connector": "rest", "server": "srv2", "database": "db2"},
        ]
        connections_b = [
            {"connector": "rest", "server": "srv2", "database": "db2"},
            {"connector": "sql", "server": "srv1", "database": "db1"},
        ]
        assert tracker.calculate_workspace_hash(
            connections_a
        ) == tracker.calculate_workspace_hash(connections_b)

    def test_empty_connections_produces_valid_hash(self):
        """Empty connections list should produce a valid hash (not crash)."""
        config = type("Config", (), {"curated_dir": "/tmp", "tenant_id": "test"})()
        tracker = scanner.ConnectionHashTracker(config)

        h = tracker.calculate_workspace_hash([])
        assert len(h) == 64  # SHA256 hex length
