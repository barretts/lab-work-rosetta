# Clinical Rosetta Stone

An open-source database that translates non-standard LIS (Laboratory Information System) shorthands into standardized LOINC codes, enriched with reference ranges, critical values, and plain-English descriptions.

## Features

- **Auto-translation** of lab test shorthands to LOINC codes
- **Fuzzy matching** with abbreviation expansion (80+ medical abbreviations)
- **Self-learning** - improves from usage
- **Reference ranges** from NHANES population data (age/sex stratified)
- **Critical values** from CAP/CLSI/SAMHSA guidelines
- **Plain-English descriptions** from MedlinePlus
- **161,000+ synonyms** from official LOINC

## Database Contents

| Data | Count |
|------|-------|
| LOINC codes | 43,651 |
| LIS mappings | 43,924 |
| Synonyms | 161,691 |
| Reference distributions | 1,860 |
| Critical value rules | 46 |
| NCI definitions | 72,705 |

## Quick Start

```bash
# Translate a file of test names
python translate.py test-list.txt -o results.csv

# Translate a single test
python translate.py --single "Hemoglobin A1c"

# CLI lookup
python rosetta_cli.py translate "ALT"
python rosetta_cli.py lookup 718-7
python rosetta_cli.py range 2345-7 --age 45 --sex M
```

## Installation

```bash
# Clone the repository
git clone <repo-url>
cd blood-test-database

# Install dependencies
pip install pandas requests openpyxl

# The database (clinical_rosetta.db) is pre-built
# Or rebuild from scratch - see WORKFLOW.md
```

## Scripts

| Script | Purpose |
|--------|---------|
| `translate.py` | Main translation tool with enrichment |
| `rosetta_cli.py` | Command-line interface for lookups |
| `auto_resolver.py` | Smart resolver with fuzzy matching and learning |
| `schema.py` | Database schema definition |
| `downloader.py` | Downloads NHANES, NCI Thesaurus, etc. |
| `ingest.py` | Ingests NHANES and NCI data |
| `ingest_loinc.py` | Ingests LOINC 2.81 (requires manual download) |
| `enrichment.py` | Adds curated mappings, critical values, MedlinePlus |

## Data Sources

| Source | License | What it provides |
|--------|---------|------------------|
| [LOINC](https://loinc.org) | Free (registration required) | Standard lab test codes, names, synonyms |
| [NHANES](https://www.cdc.gov/nchs/nhanes/) | Public Domain | Population reference ranges |
| [NCI Thesaurus](https://ncithesaurus.nci.nih.gov/) | Public Domain | Medical definitions |
| [MedlinePlus](https://medlineplus.gov/) | Public Domain | Consumer-friendly descriptions |
| CAP/CLSI Guidelines | Published literature | Critical values |
| SAMHSA | Federal guidelines | Drug screen cutoffs |

## Output Format

The translator produces CSV with these columns:

| Column | Description |
|--------|-------------|
| `test_name` | Original LIS shorthand |
| `loinc_code` | Resolved LOINC code |
| `standard_name` | Official LOINC name |
| `confidence` | Match confidence (0-1) |
| `reference_low` | Lower reference limit |
| `reference_high` | Upper reference limit |
| `unit` | Unit of measure |
| `critical_low` | Critical low threshold |
| `critical_high` | Critical high threshold |
| `description` | Plain-English description |

## Adding Custom Mappings

Edit `enrichment.py` and add to `CURATED_LIS_MAPPINGS`:

```python
"My_Hospital": [
    ("LOCAL_CODE", "LOINC-CODE", "Description"),
    ("Glu Fst", "1558-6", "Fasting Glucose"),
]
```

Then run: `python enrichment.py`

## License

Database schema and scripts: MIT License

Data sources retain their original licenses (see table above).

## Contributing

1. Fork the repository
2. Add mappings with source documentation
3. Submit a pull request

## See Also

- [WORKFLOW.md](WORKFLOW.md) - Step-by-step usage guide
- [ROADMAP.md](ROADMAP.md) - Future development plans
- [RESEARCH_FINDINGS.md](RESEARCH_FINDINGS.md) - Data source research
