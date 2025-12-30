# Clinical Rosetta Stone - Workflow Guide

## Quick Reference

```bash
# Most common task - translate a file
python translate.py your-file.txt -o output.csv

# Single lookup with details
python translate.py --single "Hemoglobin A1c"

# CLI commands
python rosetta_cli.py translate "ALT"
python rosetta_cli.py lookup 718-7
python rosetta_cli.py stats
```

---

## Workflow 1: Translate Lab Test Names

### From a file:
```bash
# Input: text file with one test name per line
python translate.py test-list.txt

# With custom output name
python translate.py test-list.txt -o my_results.csv

# With higher confidence threshold (fewer false matches)
python translate.py test-list.txt -c 0.7
```

### Single test:
```bash
python translate.py --single "Sodium Level"
python translate.py --single "HbA1c"
python translate.py --single "UA Protein"
```

### Output columns:
- `test_name` - Your input
- `loinc_code` - Standardized LOINC code
- `standard_name` - Official name
- `confidence` - Match quality (1.0 = exact, 0.5+ = fuzzy)
- `reference_low/high` - Normal range (adults)
- `critical_low/high` - Panic values
- `description` - Plain-English explanation

---

## Workflow 2: CLI Lookups

### Translate shorthand to LOINC:
```bash
python rosetta_cli.py translate "WBC"
python rosetta_cli.py t "Alk Phos"  # shorthand
```

### Lookup LOINC code details:
```bash
python rosetta_cli.py lookup 718-7
python rosetta_cli.py l 2345-7  # shorthand
```

### Get reference ranges:
```bash
python rosetta_cli.py range 718-7
python rosetta_cli.py range 2345-7 --age 45 --sex M
```

### Get critical values:
```bash
python rosetta_cli.py critical
python rosetta_cli.py critical --loinc 2823-3
```

### Search by name:
```bash
python rosetta_cli.py search glucose
python rosetta_cli.py search hemoglobin
```

### Batch translate:
```bash
python rosetta_cli.py batch "WBC,RBC,HGB,PLT"
```

### Database stats:
```bash
python rosetta_cli.py stats
```

---

## Workflow 3: Rebuild Database from Scratch

If you need to rebuild the database:

### Step 1: Create schema
```bash
python schema.py
# Creates: clinical_rosetta.db with empty tables
```

### Step 2: Download public data
```bash
python downloader.py
# Downloads: NHANES, NCI Thesaurus to raw_data/
```

### Step 3: Ingest public data
```bash
python ingest.py
# Ingests: NHANES reference ranges, NCI definitions
```

### Step 4: Add LOINC (requires manual download)
```bash
# 1. Go to https://loinc.org/downloads/
# 2. Register (free) and download LOINC Table
# 3. Extract to downloads/Loinc_2.81/

python ingest_loinc.py
# Ingests: 43,000+ LOINC codes, 161,000+ synonyms
```

### Step 5: Add enrichments
```bash
python enrichment.py
# Adds: Curated LIS mappings, critical values, MedlinePlus descriptions
```

---

## Workflow 4: Add Custom Mappings

### Option A: Edit enrichment.py

Add to the `CURATED_LIS_MAPPINGS` dictionary:

```python
"My_Hospital_LIS": [
    ("LOCAL_CODE", "LOINC-CODE", "Description"),
    ("Glu Fst", "1558-6", "Fasting Glucose"),
    ("HgbA1C", "4548-4", "Hemoglobin A1c"),
],
```

Then run:
```bash
python enrichment.py
```

### Option B: Use the auto-resolver's learning

The resolver learns from usage. To manually confirm a mapping:

```python
from auto_resolver import AutoResolver
resolver = AutoResolver()
resolver.confirm("My Local Code", "1234-5")  # Boosts confidence
resolver.close()
```

---

## Workflow 5: Fetch More Descriptions

To fetch MedlinePlus descriptions for more tests:

```python
from enrichment import RosettaEnrichment
enricher = RosettaEnrichment()
enricher.fetch_medlineplus_descriptions(limit=100)  # Fetches 100 more
enricher.close()
```

---

## Script Reference

| Script | What it does | When to use |
|--------|--------------|-------------|
| `translate.py` | Translates files/single tests with enrichment | **Daily use** |
| `rosetta_cli.py` | Command-line lookups | **Daily use** |
| `auto_resolver.py` | Smart resolver (library) | Import in code |
| `schema.py` | Creates database schema | Initial setup only |
| `downloader.py` | Downloads NHANES, NCI | Initial setup only |
| `ingest.py` | Ingests NHANES, NCI data | Initial setup only |
| `ingest_loinc.py` | Ingests LOINC 2.81 | After LOINC download |
| `enrichment.py` | Adds curated data, MedlinePlus | After adding mappings |

---

## Troubleshooting

### "No mapping found"
- Try the fuzzy search: `python rosetta_cli.py search "partial name"`
- Add a custom mapping to `enrichment.py`
- Check spelling/abbreviations

### Low confidence matches
- The resolver expands abbreviations (hgb → hemoglobin)
- Confidence < 0.7 may be incorrect - verify manually
- Use `--confidence 0.7` for stricter matching

### Missing reference ranges
- NHANES only covers common lab tests
- Ranges are for ages 18-79, stratified by sex
- Pediatric ranges require CALIPER data (not yet integrated)

### Missing descriptions
- Run `enrichment.py` to fetch more from MedlinePlus
- MedlinePlus has rate limits (100 req/min)

---

## File Outputs

| File | Contents |
|------|----------|
| `*_translated.csv` | Translation results with enrichment |
| `clinical_rosetta.db` | SQLite database |
| `raw_data/` | Downloaded source files |
| `downloads/` | LOINC and other manual downloads |
