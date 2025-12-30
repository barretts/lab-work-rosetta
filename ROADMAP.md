# Clinical Rosetta Stone - Project Roadmap

**Last Updated:** December 30, 2025  
**Current Status:** MVP Complete - 80,626 records across 4 data layers

---

## Phase 1: Foundation ✅ COMPLETE

### 1.1 Research & Data Discovery ✅
- [x] Identified 23+ freely available data sources
- [x] Documented URLs, formats, licenses in RESEARCH_FINDINGS.md
- [x] Designed 4-layer schema (Identity, Statistical, Knowledge, Severity)

### 1.2 Data Download Infrastructure ✅
- [x] Created `downloader.py` with automatic fetching
- [x] Downloaded NHANES 2017-2020 lab data (11MB)
- [x] Downloaded NCI Thesaurus (16MB)
- [x] Downloaded loinc2hpo annotations (414KB)

### 1.3 Database Schema ✅
- [x] Created `schema.py` with 12 tables
- [x] Built 5 convenience views for common queries
- [x] Implemented SQLite database with foreign keys

### 1.4 Initial Data Ingestion ✅
- [x] NHANES reference ranges: 1,860 distributions
- [x] NCI Thesaurus: 72,705 lab-related concepts
- [x] Curated LIS mappings: 183 from 5 institution styles
- [x] Critical values: 35 rules from CAP/CLSI guidelines
- [x] MedlinePlus descriptions: 30 consumer-friendly texts
- [x] LOINC↔HPO mappings: 7,415 phenotype links

---

## Phase 2: Data Expansion (Priority: HIGH)

### 2.1 LOINC Core Table Integration
**Status:** Requires manual download (free registration)

- [ ] Register at https://loinc.org/downloads/
- [ ] Download "LOINC Table Core" (LoincTableCore.zip)
- [ ] Download "LOINC Top 2000+ Lab Observations"
- [ ] Create `ingest_loinc_core.py` to parse and load
- [ ] Add all 100,000+ LOINC codes with full metadata
- [ ] Import LOINC "Related Names" for synonym expansion

**Files to import:**
```
LoincTableCore/LoincTableCore.csv
LoincTop2000CommonLabObservations.csv
AccessoryFiles/PartRelatedCodeMapping.csv
```

### 2.2 Additional NHANES Panels
**Status:** URLs identified, ready to download

- [ ] Add Hepatitis panel (P_HEPA.xpt, P_HEPB.xpt)
- [ ] Add HIV antibody (P_HIQ.xpt)
- [ ] Add PSA (P_PSA.xpt)
- [ ] Add Cotinine/smoking biomarkers (P_COT.xpt)
- [ ] Add Heavy metals (P_PBCD.xpt)
- [ ] Add Urine albumin/creatinine (already partial)

### 2.3 Expand LIS Mapping Sources
**Status:** Need real institutional data

- [ ] Contact ARUP Labs for test catalog export
- [ ] Request LIVD files directly from IVD manufacturers:
  - bioMérieux: https://www.biomerieux.com
  - Abbott: https://www.corelaboratory.abbott
  - Roche: https://diagnostics.roche.com
  - Beckman Coulter: https://www.beckmancoulter.com
- [ ] Parse Epic/Cerner open data if available
- [ ] Add academic medical center mappings (if published)

### 2.4 Pediatric Reference Ranges
**Status:** Research identified source

- [ ] Contact CALIPER project for data access
  - URL: https://caliper.research.sickkids.ca/
  - Published data: PMID 22374939
- [ ] Extract pediatric ranges from published literature
- [ ] Add age-stratified ranges for 0-18 years

---

## Phase 3: Knowledge Enrichment (Priority: MEDIUM)

### 3.1 Complete MedlinePlus Integration
**Status:** API working, need to expand

- [ ] Fetch descriptions for all 100+ current LOINC codes
- [ ] Add Spanish translations (API supports `languageCode.c=es`)
- [ ] Cache results to avoid rate limiting
- [ ] Add "Also Called" synonyms from responses

### 3.2 SNOMED CT Integration
**Status:** Requires free UMLS license

- [ ] Register for UMLS license at https://uts.nlm.nih.gov/
- [ ] Download US Edition of SNOMED CT
- [ ] Download SNOMED CT to ICD-10-CM maps
- [ ] Build vocabulary_mapping entries for LOINC↔SNOMED

### 3.3 ICD-10 Linkage
**Status:** CCS files downloaded

- [ ] Parse CCS_SingleLevel_2015 for ICD-9/10 groupings
- [ ] Link lab tests to relevant diagnoses
- [ ] Add "commonly ordered for" associations

### 3.4 Consumer Health Vocabulary
**Status:** Requires UMLS access

- [ ] Download CHV from UMLS
- [ ] Map medical jargon → plain English
- [ ] Add to concept_synonym table

---

## Phase 4: Clinical Decision Support (Priority: MEDIUM)

### 4.1 CTCAE Severity Grading
**Status:** Download failing, need alternative approach

- [ ] Manually extract CTCAE v5.0 from PDF or find working URL
- [ ] Parse grade definitions with numeric thresholds
- [ ] Map CTCAE terms to LOINC codes
- [ ] Add ULN/LLN relative thresholds

### 4.2 Expand Critical Values
**Status:** Basic set complete

- [ ] Add institution-specific critical value sets
- [ ] Add age-adjusted critical values (pediatric/geriatric)
- [ ] Add pregnancy-specific ranges
- [ ] Add condition-specific thresholds (diabetes, renal)

### 4.3 Drug-Lab Interactions
**Status:** Not started

- [ ] Research DrugBank open data
- [ ] Identify drugs that affect lab values
- [ ] Add interference warnings
- [ ] Link to RxNorm codes

---

## Phase 5: API & Application Layer (Priority: HIGH)

### 5.1 Query API
**Status:** Not started

- [ ] Create `api.py` with FastAPI/Flask
- [ ] Endpoints:
  - `GET /translate?shorthand=ALT` → LOINC code
  - `GET /loinc/{code}` → full profile
  - `GET /reference-range?loinc=718-7&age=45&sex=M`
  - `GET /critical-values?loinc=2823-3`
  - `GET /search?q=hemoglobin`
- [ ] Add fuzzy matching for shorthand lookups
- [ ] Implement rate limiting
- [ ] Add OpenAPI documentation

### 5.2 CLI Tool
**Status:** Not started

- [ ] Create `rosetta-cli` command
- [ ] Commands:
  - `rosetta translate "Alk Phos"`
  - `rosetta lookup 718-7`
  - `rosetta range --loinc 2345-7 --age 30 --sex F`
  - `rosetta search hemoglobin`
- [ ] Add JSON/table output formats

### 5.3 Web Interface (Optional)
**Status:** Not started

- [ ] Simple search interface
- [ ] Lab test profile pages
- [ ] Reference range calculator
- [ ] Batch translation tool

---

## Phase 6: Quality & Maintenance (Priority: ONGOING)

### 6.1 Data Validation
- [ ] Add unit tests for ingestion pipelines
- [ ] Validate LOINC code formats
- [ ] Check reference range plausibility
- [ ] Detect duplicate mappings

### 6.2 Update Automation
- [ ] Script to check for new LOINC versions
- [ ] Auto-download new NHANES cycles
- [ ] Monitor NCI Thesaurus releases
- [ ] Track CTCAE version updates

### 6.3 Documentation
- [ ] API documentation
- [ ] Data dictionary for all tables
- [ ] Contributing guidelines
- [ ] License clarification for each data source

### 6.4 Community & Contributions
- [ ] GitHub repository setup
- [ ] Issue templates for new mappings
- [ ] Contribution workflow for LIS shorthands
- [ ] Validation process for submissions

---

## Data Source Checklist

| Source | Status | Priority | Blocker |
|--------|--------|----------|---------|
| LOINC Core Table | ⏳ Pending | HIGH | Manual download |
| NHANES (Core) | ✅ Done | - | - |
| NHANES (Extended) | ⏳ Pending | LOW | None |
| NCI Thesaurus | ✅ Done | - | - |
| loinc2hpo | ✅ Done | - | - |
| MedlinePlus API | ✅ Partial | MEDIUM | Rate limit |
| CTCAE | ❌ Failed | MEDIUM | Site errors |
| SNOMED CT | ⏳ Pending | MEDIUM | UMLS license |
| ARUP Catalog | ⏳ Pending | HIGH | Need to request |
| LIVD Files | ❌ Failed | MEDIUM | URLs broken |
| CALIPER | ⏳ Pending | LOW | Collaboration req |
| CHV | ⏳ Pending | LOW | UMLS license |

---

## Quick Start Commands

```bash
# Download all available data
python downloader.py

# Run initial ingestion
python ingest.py

# Add curated mappings and enrichments
python enrichment.py

# Check database status
sqlite3 clinical_rosetta.db "SELECT name, (SELECT COUNT(*) FROM pragma_table_info(name)) as cols FROM sqlite_master WHERE type='table';"
```

---

## Technical Debt

1. **NHANES URL brittleness** - CDC restructures URLs periodically
2. **No automated tests** - Need pytest suite
3. **MedlinePlus rate limiting** - Need proper caching layer
4. **No incremental updates** - Full re-ingestion required
5. **SQLite scalability** - May need PostgreSQL for production

---

## Contributing

To add new LIS mappings:
1. Edit `CURATED_LIS_MAPPINGS` in `enrichment.py`
2. Add tuple: `("Shorthand", "LOINC-CODE", "Description")`
3. Run `python enrichment.py`
4. Submit PR with source documentation

---

## References

- LOINC: https://loinc.org/
- NHANES: https://www.cdc.gov/nchs/nhanes/
- NCI Thesaurus: https://ncithesaurus.nci.nih.gov/
- MedlinePlus Connect: https://medlineplus.gov/connect/
- CTCAE: https://ctep.cancer.gov/
- SNOMED CT: https://www.nlm.nih.gov/healthit/snomedct/
