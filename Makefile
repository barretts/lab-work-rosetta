.PHONY: all install install-dev format lint test clean help api setup-db fetch-all status

default: help

help:
	@echo "Clinical Rosetta Stone - Make targets"
	@echo ""
	@echo "  API & Package:"
	@echo "    install           Install the package"
	@echo "    install-dev       Install with dev dependencies"
	@echo "    api               Start the Flask API server"
	@echo "    test              Run tests"
	@echo ""
	@echo "  Data Generation (scripts/):"
	@echo "    setup-db          Initialize database and ingest data"
	@echo "    fetch-all         Run all data fetchers (parallel)"
	@echo "    status            Show data fetch status"
	@echo ""
	@echo "  Development:"
	@echo "    format            Format code with black and isort"
	@echo "    lint              Check code style"
	@echo "    clean             Remove build artifacts"

# === Package targets ===
install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"
	pre-commit install

format:
	@echo "Running black..."
	black src/ tests/ scripts/
	@echo "\nRunning isort..."
	isort src/ tests/ scripts/

lint:
	@echo "Running flake8..."
	flake8 src/ tests/
	@echo "\nRunning ruff..."
	ruff check src/ tests/

test:
	pytest -v

test-cov:
	pytest --cov=rosetta --cov-report=term-missing -v

clean:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	rm -fr build/ dist/ *.egg-info/ .pytest_cache/ .coverage htmlcov/

# === API targets ===
api:
	python -m rosetta.api

# === Data generation targets (scripts/) ===
setup-db:
	python scripts/setup/schema.py
	python scripts/setup/downloader.py
	python scripts/setup/ingest.py
	@echo "Note: Run 'python scripts/setup/ingest_loinc.py' after downloading LOINC manually"
	python scripts/setup/enrichment.py

fetch-all:
	@echo "Fetching MedlinePlus descriptions..."
	python scripts/fetch/fetch_descriptions.py --workers 5
	@echo "\nFetching UMLS/SNOMED mappings..."
	python scripts/fetch/fetch_umls.py --snomed --workers 4
	@echo "\nFetching DailyMed drug interactions..."
	python scripts/fetch/fetch_dailymed.py --fetch --workers 4

status:
	@echo "=== Data Fetch Status ==="
	@python scripts/fetch/fetch_descriptions.py --status
	@echo ""
	@python scripts/fetch/fetch_umls.py --status
	@echo ""
	@python scripts/fetch/fetch_dailymed.py --status
