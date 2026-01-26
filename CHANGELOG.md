# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-beta.1] - 2026-01-26

### Added
- **Automatic Token Refresh**: Scanner now automatically refreshes authentication tokens before expiration
  - Prevents scan failures during long-running operations (multi-day scans supported)
  - 5-minute expiry buffer ensures tokens never expire mid-operation
  - Supports all auth modes: Service Principal, Interactive, and Delegated (Fabric)
  - Automatic 401 error recovery with token refresh and retry
- **Fabric Notebook Support**: Complete guide for running scanner in Microsoft Fabric notebooks
  - Delegated authentication mode for seamless Fabric integration
  - Direct lakehouse table output (no file uploads needed)
  - SQL query examples for connection analysis
  - Troubleshooting guide and feature comparison table
- **Version Information**: Added `--version` CLI flag to display current version

### Fixed
- **Health Check Bug**: Fixed capacity filtering logic in `check_scanner_api_health()`
  - Now correctly identifies capacity-based scan contention
  - Improved recommendations for shared vs. dedicated capacity scenarios

### Changed
- **Authentication Flow**: Enhanced token caching for all auth modes
  - Token cache now shared across all Scanner API calls
  - Reduces authentication overhead for large scans
  - Better integration with Azure CLI and interactive browser auth

### Technical Details
- Token refresh tested across Python 3.8-3.12
- Comprehensive code review completed with zero critical issues
- All import errors are expected optional dependencies (properly handled)

### Beta Release Notes
This is a beta release for early testing and feedback. Core features have been validated and are ready for production use. Please report any issues on GitHub.

---

**Full Changelog**: https://github.com/jpmicrosoft/fabric_scanner_cloud_connections/compare/v0.0.0...v1.0.0-beta.1
