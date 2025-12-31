# Clinical Rosetta Stone

An open-source database and API that translates non-standard LIS (Laboratory Information System) shorthands into standardized LOINC codes, enriched with reference ranges, critical values, and plain-English descriptions.

## Features

- **REST API** - Flask-based API for translations and lookups
- **CLI** - Command-line interface for quick lookups
- **Auto-translation** of lab test shorthands to LOINC codes
- **Fuzzy matching** with abbreviation expansion (80+ medical abbreviations)
- **Reference ranges** from NHANES population data
- **Consumer descriptions** from MedlinePlus (11,000+)
- **Drug-lab interactions** from DailyMed FDA labels
- **SNOMED mappings** via UMLS
- **161,000+ synonyms** from official LOINC

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
├── raw_data/              # Downloaded datasets (gitignored)
├── downloads/             # Manual downloads like LOINC (gitignored)
└── clinical_rosetta.db    # SQLite database (gitignored)
```

## Quick Start (Pre-built Database)

If you have access to a pre-built `clinical_rosetta.db`:

```bash
# Install the package
pip install -e .

# CLI usage
rosetta translate "ALT"
rosetta lookup 718-7
rosetta search glucose
rosetta stats

# Start API server
make api
# or: python -m rosetta.api
```

## Building from Scratch

### Prerequisites

```bash
# Install the package with dependencies
pip install -e .

# Additional dependencies for data generation
pip install pandas lxml python-dotenv
```

### Step 1: Create Schema & Download Public Data

```bash
python scripts/setup/schema.py
python scripts/setup/downloader.py
python scripts/setup/ingest.py
```

### Step 2: Download LOINC (Manual - Required)

1. Go to https://loinc.org/downloads/
2. Register (free) and download "LOINC Table" (full package)
3. Extract to `downloads/Loinc_2.81/`
4. Run ingestion:

```bash
python scripts/setup/ingest_loinc.py
```

### Step 3: Add Enrichments

```bash
python scripts/setup/enrichment.py
```

### Step 4: Fetch Additional Data (Optional, Long-running)

```bash
# Set UMLS API key in .env (get free key at https://uts.nlm.nih.gov/)
echo "UMLS_API_KEY=your-key-here" > .env

# Fetch MedlinePlus descriptions (~12 hours for all 43k codes)
python scripts/fetch/fetch_descriptions.py --workers 5

# Fetch UMLS/SNOMED mappings (~6 hours)
python scripts/fetch/fetch_umls.py --snomed --workers 4

# Fetch drug-lab interactions (~30 min)
python scripts/fetch/fetch_dailymed.py --fetch --workers 4

# Check status anytime
make status
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API info |
| `/health` | GET | Health check |
| `/stats` | GET | Database statistics |
| `/translate?q=ALT` | GET | Translate test name → LOINC |
| `/translate/batch` | POST | Batch translate (JSON array) |
| `/loinc/<code>` | GET | LOINC code details |
| `/search?q=glucose` | GET | Search for tests |
| `/reference-range/<code>` | GET | Reference ranges |
| `/critical-values` | GET | Critical values |
| `/drugs` | GET | Drug-lab interactions |

## Make Targets

```bash
make help          # Show all targets

# API & Package
make install       # Install package
make install-dev   # Install with dev dependencies
make api           # Start Flask API server
make test          # Run tests

# Data Generation
make setup-db      # Run full database setup
make fetch-all     # Run all fetchers (parallel)
make status        # Show fetch status

# Development
make format        # Format code (black, isort)
make lint          # Check code style
make clean         # Remove build artifacts
```

## Data Sources

| Source | License | What it provides |
|--------|---------|------------------|
| [LOINC](https://loinc.org) | Free (registration) | Standard codes, names, synonyms |
| [NHANES](https://www.cdc.gov/nchs/nhanes/) | Public Domain | Population reference ranges |
| [NCI Thesaurus](https://ncithesaurus.nci.nih.gov/) | Public Domain | Medical definitions |
| [MedlinePlus](https://medlineplus.gov/) | Public Domain | Consumer descriptions |
| [DailyMed](https://dailymed.nlm.nih.gov/) | Public Domain | Drug-lab interactions |
| [UMLS](https://uts.nlm.nih.gov/) | Free (registration) | SNOMED mappings |

## Adding Custom Mappings

Edit `scripts/setup/enrichment.py` and add to `CURATED_LIS_MAPPINGS`:

```python
"My_Hospital": [
    ("LOCAL_CODE", "LOINC-CODE", "Description"),
    ("Glu Fst", "1558-6", "Fasting Glucose"),
]
```

Then run: `python scripts/setup/enrichment.py`

## License

Database schema and scripts: MIT License

Data sources retain their original licenses (see table above).

## See Also

- [WORKFLOW.md](WORKFLOW.md) - Detailed usage guide
- [ROADMAP.md](ROADMAP.md) - Future development plans
- [RESEARCH_FINDINGS.md](RESEARCH_FINDINGS.md) - Data source research
- [scripts/README.md](scripts/README.md) - Data generation scripts
