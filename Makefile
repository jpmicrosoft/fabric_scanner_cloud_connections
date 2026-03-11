# Makefile — common development tasks
# Usage: make <target>
#
# Requires GNU Make. On Windows, use WSL, Git Bash, or `make` from choco/scoop.

.DEFAULT_GOAL := help
PYTHON       ?= python
PIP          ?= pip

.PHONY: help install lint format test coverage security ci clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Install project and dev dependencies
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install ruff pip-audit

lint: ## Run ruff linter and format check
	ruff check .
	ruff format --check --diff .

format: ## Auto-format code with ruff
	ruff format .
	ruff check --fix .

test: ## Run pytest with coverage
	pytest tests/ -v --tb=short

coverage: ## Run tests and open HTML coverage report
	pytest tests/ -v --tb=short \
		--cov=fabric_scanner_cloud_connections \
		--cov-report=term-missing \
		--cov-report=html
	@echo "Coverage report: htmlcov/index.html"

security: ## Audit dependencies for known vulnerabilities
	pip-audit --strict --desc

ci: lint test security ## Run the full CI suite locally
	@echo ""
	@echo "✅ CI suite passed."

clean: ## Remove build artifacts and caches
	rm -rf __pycache__ .pytest_cache .ruff_cache htmlcov .coverage coverage.xml test-results.xml
	find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
	@echo "Cleaned."
