.PHONY: help install sync test clean build publish stage-test

# Set default goal to display help menu
.DEFAULT_GOAL := help

sync: ## Install and sync dependencies from uv.lock
	uv sync

help: ## Show this help menu
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: ## Run tests with pytest
	uv run pytest

clean: ## Clean build distributions and cache directories
	rm -rf dist/ .pytest_cache/ .venv/ *.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} +

build: clean ## Build source and wheel distributions using uv
	uv build

publish: build ## Publish package distributions to PyPI
	uv publish

stage-test: build ## Spin up a clean venv, install the built wheel, and test it
	uv venv .test-venv
	# Find the built wheel file in dist/ and install it into the test venv
	.test-venv/bin/uv pip install dist/*.whl
	# Run pytest utilizing the test venv's installed package
	.test-venv/bin/pytest
	@echo "SUCCESS: Built wheel installed and verified cleanly!"