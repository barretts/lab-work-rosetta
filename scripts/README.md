# Data Generation Scripts

Standalone scripts for building and maintaining the Clinical Rosetta Stone database.
These are **separate from the API** and should be run independently.

## Directory Structure

```
scripts/
├── setup/          # Database setup and initial data loading
│   ├── schema.py         # Create database schema
│   ├── downloader.py     # Download raw data (NHANES, NCI, etc.)
│   ├── ingest.py         # Ingest downloaded data into DB
│   ├── ingest_loinc.py   # Ingest LOINC table (requires manual download)
│   └── enrichment.py     # Add curated mappings and enrichments
│
├── fetch/          # API-based data fetching (long-running)
│   ├── fetch_descriptions.py  # MedlinePlus consumer descriptions
│   ├── fetch_umls.py          # UMLS/SNOMED mappings
│   └── fetch_dailymed.py      # Drug-lab interactions from FDA labels
│
└── tools/          # Utility scripts
    ├── auto_resolver.py  # Smart test name resolver
    ├── translate.py      # Batch translation tool
    └── rosetta_cli.py    # Legacy CLI (use `rosetta` command instead)
```

## Initial Setup

Run these in order to build the database from scratch:

```bash
# 1. Create database schema
python scripts/setup/schema.py

# 2. Download public datasets
python scripts/setup/downloader.py

# 3. Ingest downloaded data
python scripts/setup/ingest.py

# 4. (Manual) Download LOINC from https://loinc.org/downloads/
#    Extract to downloads/Loinc_2.81/
python scripts/setup/ingest_loinc.py

# 5. Add curated mappings and enrichments
python scripts/setup/enrichment.py
```

## Data Fetching

Long-running scripts to fetch additional data from APIs:

```bash
# Fetch MedlinePlus descriptions (supports parallel)
python scripts/fetch/fetch_descriptions.py --workers 5

# Fetch UMLS/SNOMED mappings (requires API key)
python scripts/fetch/fetch_umls.py --snomed --workers 4

# Fetch drug-lab interactions from DailyMed
python scripts/fetch/fetch_dailymed.py --fetch --workers 4

# Check status of all fetchers
python scripts/fetch/fetch_descriptions.py --status
python scripts/fetch/fetch_umls.py --status
python scripts/fetch/fetch_dailymed.py --status
```

## Utility Tools

```bash
# Translate a file of test names
python scripts/tools/translate.py test-list.txt -o results.csv

# Single test lookup
python scripts/tools/translate.py --single "Hemoglobin A1c"
```

## Configuration

- **Database path**: Scripts default to `./clinical_rosetta.db` in project root
- **UMLS API key**: Set in `.env` file as `UMLS_API_KEY=your-key`
- **Progress files**: `.fetch_progress.json`, `.umls_progress.json`, `.dailymed_progress.json`
