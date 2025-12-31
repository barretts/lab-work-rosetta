# Clinical Rosetta Stone - Workflow Guide

## Project Structure

```
blood-test-database/
├── src/rosetta/           # API package (pip install -e .)
│   ├── api/               # Flask REST API
│   ├── core/              # Resolver, database utilities
│   └── cli.py             # CLI entry point
├── scripts/               # Data generation (standalone)
│   ├── setup/             # Database setup & ingestion
│   ├── fetch/             # API-based data fetchers
│   └── tools/             # Utility scripts
├── tests/                 # API tests
├── raw_data/              # Downloaded datasets
├── downloads/             # Manual downloads (LOINC)
└── clinical_rosetta.db    # SQLite database
```

---

## Quick Reference

```bash
# Install the package
pip install -e .

# CLI commands (after install)
rosetta translate "ALT"
rosetta lookup 718-7
rosetta search glucose
rosetta stats

# Start the API server
make api
# or: python -m rosetta.api

# Check data fetch status
make status
```

---

## Workflow 1: Using the API

### Start the server:
```bash
make api
# Server runs at http://localhost:5000
```

### API Endpoints:
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/translate?q=ALT` | GET | Translate test name → LOINC |
| `/translate/batch` | POST | Batch translate (JSON array) |
| `/loinc/718-7` | GET | Get LOINC code details |
| `/search?q=glucose` | GET | Search for tests |
| `/reference-range/718-7` | GET | Get reference ranges |
| `/critical-values` | GET | Get critical values |
| `/drugs?name=warfarin` | GET | Drug-lab interactions |
| `/health` | GET | Health check |
| `/stats` | GET | Database statistics |

### Example API calls:
```bash
# Translate a test name
curl "http://localhost:5000/translate?q=hemoglobin"

# Batch translate
curl -X POST http://localhost:5000/translate/batch \
  -H "Content-Type: application/json" \
  -d '["ALT", "AST", "Glucose"]'

# Get LOINC details
curl "http://localhost:5000/loinc/718-7"

# Search
curl "http://localhost:5000/search?q=glucose&limit=10"
```

---

## Workflow 2: CLI Lookups

### Translate shorthand to LOINC:
```bash
rosetta translate "WBC"
rosetta t "Alk Phos"  # shorthand
```

### Lookup LOINC code details:
```bash
rosetta lookup 718-7
rosetta l 2345-7  # shorthand
```

### Search by name:
```bash
rosetta search glucose
rosetta search hemoglobin --limit 20
```

### Get reference ranges:
```bash
rosetta range 718-7
rosetta range 2345-7 --age 45 --sex M
```

### Batch translate:
```bash
rosetta batch "WBC,RBC,HGB,PLT"
```

### Database stats:
```bash
rosetta stats
```

### JSON output:
```bash
rosetta translate "ALT" --json
rosetta lookup 718-7 --json
```

---

## Workflow 3: Rebuild Database from Scratch

Use the scripts in `scripts/setup/`:

```bash
# Or use make target:
make setup-db

# Manual steps:
# Step 1: Create schema
python scripts/setup/schema.py

# Step 2: Download public data
python scripts/setup/downloader.py

# Step 3: Ingest public data
python scripts/setup/ingest.py

# Step 4: Add LOINC (requires manual download)
# 1. Go to https://loinc.org/downloads/
# 2. Register (free) and download LOINC Table
# 3. Extract to downloads/Loinc_2.81/
python scripts/setup/ingest_loinc.py

# Step 5: Add enrichments
python scripts/setup/enrichment.py
```

---

## Workflow 4: Fetch Additional Data

Use the scripts in `scripts/fetch/` for long-running API fetches:

```bash
# Or use make target:
make fetch-all

# Individual fetchers with parallel workers:

# MedlinePlus consumer descriptions
python scripts/fetch/fetch_descriptions.py --workers 5

# UMLS/SNOMED mappings (requires API key in .env)
python scripts/fetch/fetch_umls.py --snomed --workers 4

# DailyMed drug-lab interactions
python scripts/fetch/fetch_dailymed.py --fetch --workers 4

# Check status of all fetchers
make status
```

---

## Workflow 5: Add Custom Mappings

### Edit enrichment.py

Add to the `CURATED_LIS_MAPPINGS` dictionary in `scripts/setup/enrichment.py`:

```python
"My_Hospital_LIS": [
    ("LOCAL_CODE", "LOINC-CODE", "Description"),
    ("Glu Fst", "1558-6", "Fasting Glucose"),
    ("HgbA1C", "4548-4", "Hemoglobin A1c"),
],
```

Then run:
```bash
python scripts/setup/enrichment.py
```

---

## Script Reference

### API Package (`src/rosetta/`)
| Module | Description |
|--------|-------------|
| `rosetta.api` | Flask REST API |
| `rosetta.cli` | Command-line interface |
| `rosetta.core.resolver` | Lab test resolver |
| `rosetta.core.database` | Database utilities |

### Setup Scripts (`scripts/setup/`)
| Script | Description |
|--------|-------------|
| `schema.py` | Creates database schema |
| `downloader.py` | Downloads NHANES, NCI, etc. |
| `ingest.py` | Ingests downloaded data |
| `ingest_loinc.py` | Ingests LOINC table |
| `enrichment.py` | Adds curated mappings |

### Fetch Scripts (`scripts/fetch/`)
| Script | Description |
|--------|-------------|
| `fetch_descriptions.py` | MedlinePlus descriptions |
| `fetch_umls.py` | UMLS/SNOMED mappings |
| `fetch_dailymed.py` | Drug-lab interactions |

### Utility Scripts (`scripts/tools/`)
| Script | Description |
|--------|-------------|
| `translate.py` | Batch file translation |
| `auto_resolver.py` | Smart resolver library |

---

## Make Targets

```bash
make help          # Show all targets

# API & Package
make install       # Install package
make install-dev   # Install with dev dependencies
make api           # Start Flask API server
make test          # Run tests

# Data Generation
make setup-db      # Initialize database
make fetch-all     # Run all fetchers (parallel)
make status        # Show fetch status

# Development
make format        # Format code (black, isort)
make lint          # Check code style
make clean         # Remove build artifacts
```

---

## Troubleshooting

### "No mapping found"
- Try search: `rosetta search "partial name"`
- Add custom mapping to `scripts/setup/enrichment.py`
- Check spelling/abbreviations

### Low confidence matches
- Resolver expands abbreviations (hgb → hemoglobin)
- Confidence < 0.7 may be incorrect
- Use `--confidence 0.7` for stricter matching

### Missing reference ranges
- NHANES only covers common lab tests
- Ranges are for ages 18-79, stratified by sex

### Missing descriptions
- Run fetchers: `make fetch-all`
- Check status: `make status`

---

## File Outputs

| Path | Contents |
|------|----------|
| `clinical_rosetta.db` | SQLite database |
| `raw_data/` | Downloaded source files |
| `downloads/` | Manual downloads (LOINC) |
| `.fetch_progress.json` | MedlinePlus fetch progress |
| `.umls_progress.json` | UMLS fetch progress |
| `.dailymed_progress.json` | DailyMed fetch progress |
