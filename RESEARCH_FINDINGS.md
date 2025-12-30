# Clinical Rosetta Stone - Data Sources Research Report

**Generated:** December 2025  
**Purpose:** Identify freely available datasets for translating LIS shorthands to standardized codes

---

## Executive Summary

This report identifies **23+ freely available data sources** across four data layers for building a Clinical Rosetta Stone database. All sources are publicly accessible (US Government, Open Source, or Creative Commons licensed).

---

## 1. THE IDENTITY LAYER (Linking Shorthand to Standard)

### 1.1 LOINC Core Table (Primary Standard)

| Attribute | Value |
|-----------|-------|
| **Source** | Regenstrief Institute |
| **URL** | https://loinc.org/downloads/ |
| **Format** | CSV (in ZIP archive) |
| **Filename** | `LoincTableCore.zip` → `LoincTableCore/LoincTableCore.csv` |
| **License** | Free with registration (LOINC License) |
| **Key Fields** | `LOINC_NUM`, `COMPONENT`, `PROPERTY`, `TIME_ASPCT`, `SYSTEM`, `SCALE_TYP`, `METHOD_TYP`, `LONG_COMMON_NAME`, `SHORTNAME` |
| **Notes** | Requires free account creation. Current version: 2.77 |

**Additional LOINC Files:**
- `LoincTop2000CommonLabObservations.csv` - Most frequently used codes
- `DocumentOntology.csv` - Document type classifications
- `LoincPartLink.csv` - Hierarchical relationships

### 1.2 LIS-to-LOINC Mapping Dictionaries (Open Source)

| Source | URL | Format | Description |
|--------|-----|--------|-------------|
| **GPC Doc Ontology** | https://github.com/gpcnetwork/gpc_doc_ontology | Python/CSV | Epic/Cerner document-to-LOINC mapper with bag-of-words algorithm |
| **loinc2hpoAnnotation** | https://github.com/TheJacksonLaboratory/loinc2hpoAnnotation | TSV | 2,000+ LOINC codes mapped to HPO phenotype terms |
| **LIVD Catalog (CDC)** | https://www.cdc.gov/laboratory-systems/php/livd-test-codemapping/ | XLSX | IVD test codes mapped to LOINC (SARS-CoV-2, Monkeypox, HIV, Lyme) |
| **Epic BDC_LOINC_CODES** | https://open.epic.com/EHITables/GetTable/BDC_LOINC_CODES.htm | HTML/Schema | Epic's LOINC code mapping schema documentation |
| **Snow Owl Server** | https://github.com/b2ihealthcare/snow-owl | Java/Docker | Full terminology server supporting LOINC, SNOMED, ICD-10/11, RxNorm |

### 1.3 Cross-Terminology Mapping Resources

| Source | URL | Format | Key Mappings |
|--------|-----|--------|--------------|
| **OHDSI Athena** | https://athena.ohdsi.org/ | CSV (bulk download) | 9M+ concepts: SNOMED↔ICD-10↔LOINC↔RxNorm |
| **OMOPHub Python SDK** | https://github.com/OMOPHub/omophub-python | API/Python | Query 90+ vocabularies programmatically |
| **NCI Thesaurus FLAT** | https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Thesaurus.FLAT.zip | TSV (16MB) | Medical term definitions, hierarchies, synonyms |
| **SNOMED CT (via NLM)** | https://www.nlm.nih.gov/healthit/snomedct/index.html | RF2 | US Edition + ICD-10-CM maps (requires UMLS license - FREE) |

### 1.4 Government Data Dictionaries

| Source | URL | Format | Description |
|--------|-----|--------|-------------|
| **CMS CCS (ICD Groupings)** | https://hcup-us.ahrq.gov/toolssoftware/ccs/ccs.jsp | CSV/SAS | Clinical Classifications Software - ICD-9/10 to categories |
| **VA VINCI (pending)** | https://www.hsrd.research.va.gov/for_researchers/vinci/ | Various | VHA research data (requires VA researcher access) |

---

## 2. THE STATISTICAL LAYER (Reference Ranges)

### 2.1 CDC NHANES Laboratory Data

| Cycle | Biochemistry File | CBC File | Demographics File |
|-------|------------------|----------|-------------------|
| **2017-2020** | `P_BIOPRO.XPT` | `P_CBC.XPT` | `P_DEMO.XPT` |
| **2017-2018** | `BIOPRO_J.XPT` | `CBC_J.XPT` | `DEMO_J.XPT` |

**Download Base URL:** `https://wwwn.cdc.gov/Nchs/Nhanes/[CYCLE]/[FILENAME]`

**Key SAS Variables (Biochemistry):**
| SAS Variable | LOINC | Test Name |
|--------------|-------|-----------|
| `LBXSATSI` | 1742-6 | ALT (SGPT) |
| `LBXSASSI` | 1920-8 | AST (SGOT) |
| `LBXSGL` | 2345-7 | Glucose, Serum |
| `LBXSAL` | 1751-7 | Albumin |
| `LBXSCR` | 2160-0 | Creatinine |
| `LBXSBU` | 3094-0 | BUN |
| `LBXSTP` | 2885-2 | Total Protein |
| `LBXSTB` | 1975-2 | Total Bilirubin |
| `LBXSAPSI` | 6768-6 | Alkaline Phosphatase |
| `LBXSLDSI` | 2532-0 | LDH |

**Key SAS Variables (CBC):**
| SAS Variable | LOINC | Test Name |
|--------------|-------|-----------|
| `LBXWBCSI` | 6690-2 | WBC Count |
| `LBXRBCSI` | 789-8 | RBC Count |
| `LBXHGB` | 718-7 | Hemoglobin |
| `LBXHCT` | 4544-3 | Hematocrit |
| `LBXPLTSI` | 777-3 | Platelet Count |
| `LBXMCVSI` | 787-2 | MCV |
| `LBXMPSI` | 32623-1 | MPV |
| `LBXNEPCT` | 770-8 | Neutrophils % |
| `LBXLYPCT` | 736-9 | Lymphocytes % |
| `LBXMOPCT` | 5905-5 | Monocytes % |

### 2.2 CALIPER Project (Pediatric Ranges)

| Attribute | Value |
|-----------|-------|
| **Source** | Hospital for Sick Children, Toronto |
| **URL** | https://caliper.research.sickkids.ca/ |
| **Format** | PDF/Web lookup (no bulk download) |
| **Coverage** | 100+ analytes, age 0-18, by sex |
| **Access** | Free web lookup; bulk data requires collaboration request |
| **Alternative** | Published papers with reference tables (PMID: 22374939) |

### 2.3 Critical/Panic Value Lists

| Source | URL | Format | Notes |
|--------|-----|--------|-------|
| **Mayo Clinic Labs** | https://www.mayocliniclabs.com/test-info/critical-values | HTML (scrape) | Blocked by robots.txt - manual extraction needed |
| **CLSI C56-A** | Publication | PDF | Consensus guideline (library access) |
| **CAP Surveys** | https://www.cap.org | PDF | College of American Pathologists proficiency data |

**Common Critical Values (from literature):**

| Analyte | LOINC | Critical Low | Critical High | Unit |
|---------|-------|--------------|---------------|------|
| Glucose | 2345-7 | <40 | >500 | mg/dL |
| Potassium | 2823-3 | <2.5 | >6.5 | mEq/L |
| Sodium | 2951-2 | <120 | >160 | mEq/L |
| Calcium | 17861-6 | <6.0 | >13.0 | mg/dL |
| Hemoglobin | 718-7 | <7.0 | >20.0 | g/dL |
| Platelets | 777-3 | <20,000 | >1,000,000 | /µL |
| WBC | 6690-2 | <2,000 | >30,000 | /µL |
| INR | 6301-6 | - | >5.0 | ratio |
| Troponin I | 10839-9 | - | >0.5 | ng/mL |

---

## 3. THE KNOWLEDGE LAYER (Descriptions & Context)

### 3.1 MedlinePlus Connect API

| Attribute | Value |
|-----------|-------|
| **Base URL** | `https://connect.medlineplus.gov/service` |
| **Format** | XML, JSON, JSONP |
| **License** | Free, no registration |
| **Rate Limit** | 100 requests/min per IP |

**LOINC Lab Test Query Example:**
```
https://connect.medlineplus.gov/service?mainSearchCriteria.v.cs=2.16.840.1.113883.6.1&mainSearchCriteria.v.c=2345-7&knowledgeResponseType=application/json
```

**Code Systems:**
| Type | OID |
|------|-----|
| LOINC | `2.16.840.1.113883.6.1` |
| ICD-10-CM | `2.16.840.1.113883.6.90` |
| SNOMED CT | `2.16.840.1.113883.6.96` |
| RxNorm | `2.16.840.1.113883.6.88` |
| NDC | `2.16.840.1.113883.6.69` |

### 3.2 MedlinePlus XML Bulk Downloads

| File | URL | Size | Update Frequency |
|------|-----|------|------------------|
| **Health Topics XML** | https://medlineplus.gov/xml.html | ~29MB | Daily (Tue-Sat) |
| **Compressed XML** | Same page | ~4.6MB | Daily |
| **Health Terms (Fitness)** | Same page | 7KB | Infrequent |
| **Health Terms (Nutrition)** | Same page | 14KB | Infrequent |

### 3.3 NCI Thesaurus (Plain English Definitions)

| File | URL | Format | Size |
|------|-----|--------|------|
| **Thesaurus.FLAT.zip** | https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Thesaurus.FLAT.zip | TSV | 16MB |
| **Thesaurus.OWL.zip** | https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Thesaurus.OWL.zip | OWL2/XML | 43MB |

**FLAT File Structure:**
```
code <tab> concept_IRI <tab> parents <tab> synonyms <tab> definition <tab> display_name <tab> concept_status <tab> semantic_type <tab> concept_in_subset
```

### 3.4 Consumer Health Vocabulary (CHV)

| Attribute | Value |
|-----------|-------|
| **Source** | NLM UMLS Metathesaurus |
| **Access** | Requires free UMLS license |
| **URL** | https://www.nlm.nih.gov/research/umls/sourcereleasedocs/current/CHV/ |
| **Purpose** | Maps medical jargon → patient-friendly terms |

---

## 4. THE CLINICAL SEVERITY LAYER (Toxicity & Panic)

### 4.1 CTCAE (Common Terminology Criteria for Adverse Events)

| Version | URL | Format |
|---------|-----|--------|
| **CTCAE v6.0 (2025)** | https://ctep.cancer.gov/protocoldevelopment/electronic_applications/ctc.htm | Excel |
| **CTCAE v5.0 (2017)** | Same page | Excel |
| **CTCAE v4.03 (2010)** | Same page | Excel |

**Direct Download:** `https://ctep.cancer.gov/protocoldevelopment/electronic_applications/docs/CTCAE_v5.0.xlsx`

**Grade Definitions:**
| Grade | Severity | Description |
|-------|----------|-------------|
| 1 | Mild | Asymptomatic; clinical/diagnostic observations only |
| 2 | Moderate | Minimal, local, noninvasive intervention indicated |
| 3 | Severe | Medically significant but not life-threatening |
| 4 | Life-threatening | Urgent intervention indicated |
| 5 | Death | Death related to adverse event |

**Laboratory-Relevant CTCAE Categories:**
- Investigations → Blood chemistry abnormalities
- Investigations → Hematologic abnormalities
- Metabolism and nutrition disorders

### 4.2 Legacy Version Mappings

| File | Purpose |
|------|---------|
| `CTCAE v3.0 to v4.0 Mapping.xlsx` | Forward migration |
| `CTCAE v4.0 to v3.0 Reverse Mapping.xlsx` | Backward compatibility |

---

## 5. COMPLETE SOURCE URL TABLE

| Layer | Source Name | URL | Format | License |
|-------|-------------|-----|--------|---------|
| Identity | LOINC Core Table | https://loinc.org/downloads/ | CSV | Free (registration) |
| Identity | GPC Doc Ontology | https://github.com/gpcnetwork/gpc_doc_ontology | Python/CSV | Open Source |
| Identity | loinc2hpo Annotations | https://github.com/TheJacksonLaboratory/loinc2hpoAnnotation | TSV | MIT |
| Identity | CDC LIVD Files | https://www.cdc.gov/laboratory-systems/php/livd-test-codemapping/ | XLSX | Public Domain |
| Identity | NCI Thesaurus | https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/ | FLAT/OWL | CC BY 4.0 |
| Identity | OHDSI Athena | https://athena.ohdsi.org/ | CSV | Apache 2.0 |
| Identity | CMS CCS | https://hcup-us.ahrq.gov/toolssoftware/ccs/ccs.jsp | CSV/SAS | Public Domain |
| Statistical | NHANES Lab Data | https://wwwn.cdc.gov/Nchs/Nhanes/ | XPT (SAS) | Public Domain |
| Statistical | CALIPER | https://caliper.research.sickkids.ca/ | Web/PDF | Academic |
| Knowledge | MedlinePlus Connect | https://connect.medlineplus.gov/service | JSON/XML | Public Domain |
| Knowledge | MedlinePlus XML | https://medlineplus.gov/xml.html | XML | Public Domain |
| Severity | CTCAE v5.0 | https://ctep.cancer.gov/.../CTCAE_v5.0.xlsx | Excel | Public Domain |
| Severity | CTCAE v6.0 | Same site | Excel | Public Domain |

---

## 6. PROPOSED SQL SCHEMA

```sql
-- ============================================
-- CLINICAL ROSETTA STONE DATABASE SCHEMA
-- ============================================

-- ---------------------------------------------
-- LAYER 1: CORE IDENTITY (The Standard)
-- ---------------------------------------------
CREATE TABLE loinc_concept (
    loinc_code VARCHAR(10) PRIMARY KEY,
    component VARCHAR(255),
    property VARCHAR(50),
    time_aspect VARCHAR(50),
    system VARCHAR(100),
    scale_type VARCHAR(20),
    method_type VARCHAR(100),
    long_common_name VARCHAR(255),
    short_name VARCHAR(50),
    class VARCHAR(100),
    status VARCHAR(20),
    rank_frequency INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_loinc_component ON loinc_concept(component);
CREATE INDEX idx_loinc_class ON loinc_concept(class);

-- ---------------------------------------------
-- LAYER 2: MAPPING (LIS Shorthand → Standard)
-- ---------------------------------------------
CREATE TABLE source_system (
    system_id INTEGER PRIMARY KEY AUTOINCREMENT,
    system_name VARCHAR(100) UNIQUE,  -- 'Epic', 'Cerner', 'Meditech', 'LabCorp'
    system_type VARCHAR(50),          -- 'EHR', 'LIS', 'Reference Lab'
    version VARCHAR(50)
);

CREATE TABLE lis_mapping (
    mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_system_id INTEGER REFERENCES source_system(system_id),
    source_code VARCHAR(50),
    source_description VARCHAR(255),   -- "Mono Auto", "Alk Phos"
    target_loinc VARCHAR(10) REFERENCES loinc_concept(loinc_code),
    mapping_confidence REAL,           -- 0.0 to 1.0
    mapping_source VARCHAR(100),       -- 'LIVD', 'Manual', 'ML_Inferred'
    verified_date DATE,
    UNIQUE(source_system_id, source_code)
);

CREATE INDEX idx_mapping_source_desc ON lis_mapping(source_description);
CREATE INDEX idx_mapping_loinc ON lis_mapping(target_loinc);

-- Cross-vocabulary mappings (SNOMED, ICD-10, etc.)
CREATE TABLE vocabulary_mapping (
    mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_vocabulary VARCHAR(50),     -- 'LOINC', 'SNOMED', 'ICD10CM'
    source_code VARCHAR(50),
    target_vocabulary VARCHAR(50),
    target_code VARCHAR(50),
    relationship_type VARCHAR(50),     -- 'MAPS_TO', 'EQUIVALENT', 'BROADER'
    mapping_source VARCHAR(100)
);

CREATE INDEX idx_vocab_map_source ON vocabulary_mapping(source_vocabulary, source_code);

-- ---------------------------------------------
-- LAYER 3: STATISTICAL (Reference Ranges)
-- ---------------------------------------------
CREATE TABLE reference_population (
    population_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_name VARCHAR(100),         -- 'NHANES_2017_2020', 'CALIPER'
    description TEXT,
    sample_size INTEGER,
    collection_start DATE,
    collection_end DATE,
    geographic_region VARCHAR(100)
);

CREATE TABLE reference_distribution (
    dist_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loinc_code VARCHAR(10) REFERENCES loinc_concept(loinc_code),
    population_id INTEGER REFERENCES reference_population(population_id),
    
    -- Demographics
    sex CHAR(1),                       -- 'M', 'F', 'A' (All)
    age_min_years REAL,
    age_max_years REAL,
    race_ethnicity VARCHAR(50),        -- NULL for combined
    
    -- Statistics
    n_samples INTEGER,
    mean REAL,
    std_dev REAL,
    p01 REAL,                          -- 1st percentile
    p025 REAL,                         -- 2.5th percentile (lower ref limit)
    p05 REAL,
    p25 REAL,
    p50 REAL,                          -- Median
    p75 REAL,
    p95 REAL,
    p975 REAL,                         -- 97.5th percentile (upper ref limit)
    p99 REAL,
    
    -- Reference Range (clinical)
    ref_low REAL,
    ref_high REAL,
    unit_of_measure VARCHAR(50),
    
    UNIQUE(loinc_code, population_id, sex, age_min_years, age_max_years)
);

CREATE INDEX idx_ref_dist_loinc ON reference_distribution(loinc_code);
CREATE INDEX idx_ref_dist_demo ON reference_distribution(sex, age_min_years, age_max_years);

-- ---------------------------------------------
-- LAYER 4: KNOWLEDGE (Human-Readable Context)
-- ---------------------------------------------
CREATE TABLE concept_description (
    desc_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loinc_code VARCHAR(10) REFERENCES loinc_concept(loinc_code),
    source VARCHAR(100),               -- 'MedlinePlus', 'NCI_Thesaurus', 'Wikipedia'
    language VARCHAR(10) DEFAULT 'en',
    description_type VARCHAR(50),      -- 'consumer', 'professional', 'brief'
    description_text TEXT,
    source_url VARCHAR(500),
    retrieved_date DATE,
    
    UNIQUE(loinc_code, source, language, description_type)
);

CREATE TABLE concept_synonym (
    synonym_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loinc_code VARCHAR(10) REFERENCES loinc_concept(loinc_code),
    synonym_text VARCHAR(255),
    synonym_type VARCHAR(50),          -- 'abbreviation', 'lay_term', 'clinical'
    source VARCHAR(100)
);

CREATE INDEX idx_synonym_text ON concept_synonym(synonym_text);

-- ---------------------------------------------
-- LAYER 5: SEVERITY (Clinical Alerts)
-- ---------------------------------------------
CREATE TABLE severity_standard (
    standard_id INTEGER PRIMARY KEY AUTOINCREMENT,
    standard_name VARCHAR(100),        -- 'CTCAE_v5', 'CAP_Critical', 'Institution_XYZ'
    version VARCHAR(20),
    effective_date DATE,
    description TEXT
);

CREATE TABLE severity_rule (
    rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loinc_code VARCHAR(10) REFERENCES loinc_concept(loinc_code),
    standard_id INTEGER REFERENCES severity_standard(standard_id),
    
    -- Thresholds
    direction VARCHAR(10),             -- 'HIGH', 'LOW', 'ABNORMAL'
    threshold_value REAL,
    threshold_operator VARCHAR(5),     -- '>', '>=', '<', '<=', '='
    unit_of_measure VARCHAR(50),
    
    -- Severity
    severity_level VARCHAR(20),        -- 'Critical', 'Grade_1', 'Grade_4'
    severity_code INTEGER,             -- 1-5 for CTCAE
    severity_label VARCHAR(100),       -- 'Life-threatening'
    clinical_description TEXT,
    
    -- Context
    applies_to_age_min REAL,
    applies_to_age_max REAL,
    applies_to_sex CHAR(1)
);

CREATE INDEX idx_severity_loinc ON severity_rule(loinc_code);
CREATE INDEX idx_severity_level ON severity_rule(severity_level);

-- ---------------------------------------------
-- VIEWS FOR COMMON QUERIES
-- ---------------------------------------------

-- Complete test profile with all metadata
CREATE VIEW v_test_profile AS
SELECT 
    l.loinc_code,
    l.long_common_name,
    l.short_name,
    l.component,
    l.class,
    d.description_text AS consumer_description,
    rd.ref_low,
    rd.ref_high,
    rd.unit_of_measure,
    rd.sex,
    rd.age_min_years,
    rd.age_max_years
FROM loinc_concept l
LEFT JOIN concept_description d ON l.loinc_code = d.loinc_code AND d.description_type = 'consumer'
LEFT JOIN reference_distribution rd ON l.loinc_code = rd.loinc_code;

-- Quick lookup for LIS code translation
CREATE VIEW v_lis_translation AS
SELECT 
    m.source_code AS lis_code,
    m.source_description AS lis_name,
    s.system_name,
    l.loinc_code,
    l.long_common_name AS standard_name
FROM lis_mapping m
JOIN source_system s ON m.source_system_id = s.system_id
JOIN loinc_concept l ON m.target_loinc = l.loinc_code;
```

---

## 7. PYTHON PARSING STRATEGIES

### 7.1 NHANES XPT File Parser

```python
"""
NHANES XPT (SAS Transport) File Parser
Uses the 'xport' or 'pandas' library to read SAS transport files.
"""
import pandas as pd
import numpy as np
from pathlib import Path
import requests

# Option 1: Using pandas (recommended - built-in support)
def load_nhanes_xpt(url_or_path: str) -> pd.DataFrame:
    """
    Load NHANES XPT file from URL or local path.
    
    Args:
        url_or_path: Either a URL (https://...) or local file path
        
    Returns:
        pandas DataFrame with decoded data
    """
    if url_or_path.startswith('http'):
        # Download to temp file first (pandas needs seekable file)
        import tempfile
        response = requests.get(url_or_path)
        response.raise_for_status()
        
        with tempfile.NamedTemporaryFile(suffix='.xpt', delete=False) as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name
        
        df = pd.read_sas(tmp_path, format='xport')
        Path(tmp_path).unlink()  # Clean up
    else:
        df = pd.read_sas(url_or_path, format='xport')
    
    return df


# Option 2: Using xport library (more control)
def load_nhanes_xpt_detailed(filepath: str) -> dict:
    """
    Load XPT with full metadata using xport library.
    
    pip install xport
    """
    import xport.v56
    
    with open(filepath, 'rb') as f:
        library = xport.v56.load(f)
    
    # Library is dict-like, contains datasets
    result = {}
    for name, dataset in library.items():
        result[name] = {
            'data': pd.DataFrame(dataset),
            'label': dataset.label if hasattr(dataset, 'label') else None,
            'variables': {
                col: {
                    'label': dataset[col].label if hasattr(dataset[col], 'label') else None,
                    'format': dataset[col].format if hasattr(dataset[col], 'format') else None
                }
                for col in dataset.columns
            }
        }
    return result


# NHANES-specific processing
NHANES_VARIABLE_MAP = {
    # Biochemistry Panel
    'LBXSATSI': {'loinc': '1742-6', 'name': 'Alanine aminotransferase (ALT)', 'unit': 'U/L'},
    'LBXSASSI': {'loinc': '1920-8', 'name': 'Aspartate aminotransferase (AST)', 'unit': 'U/L'},
    'LBXSGL': {'loinc': '2345-7', 'name': 'Glucose, serum', 'unit': 'mg/dL'},
    'LBXSAL': {'loinc': '1751-7', 'name': 'Albumin', 'unit': 'g/dL'},
    'LBXSCR': {'loinc': '2160-0', 'name': 'Creatinine', 'unit': 'mg/dL'},
    'LBXSBU': {'loinc': '3094-0', 'name': 'Blood urea nitrogen', 'unit': 'mg/dL'},
    'LBXSTP': {'loinc': '2885-2', 'name': 'Total protein', 'unit': 'g/dL'},
    'LBXSTB': {'loinc': '1975-2', 'name': 'Total bilirubin', 'unit': 'mg/dL'},
    'LBXSAPSI': {'loinc': '6768-6', 'name': 'Alkaline phosphatase', 'unit': 'U/L'},
    'LBXSLDSI': {'loinc': '2532-0', 'name': 'Lactate dehydrogenase', 'unit': 'U/L'},
    'LBXSUA': {'loinc': '3084-1', 'name': 'Uric acid', 'unit': 'mg/dL'},
    'LBXSNASI': {'loinc': '2951-2', 'name': 'Sodium', 'unit': 'mmol/L'},
    'LBXSKSI': {'loinc': '2823-3', 'name': 'Potassium', 'unit': 'mmol/L'},
    'LBXSCLSI': {'loinc': '2075-0', 'name': 'Chloride', 'unit': 'mmol/L'},
    'LBXSC3SI': {'loinc': '2028-9', 'name': 'Bicarbonate', 'unit': 'mmol/L'},
    'LBXSPH': {'loinc': '2777-1', 'name': 'Phosphorus', 'unit': 'mg/dL'},
    'LBXSCA': {'loinc': '17861-6', 'name': 'Calcium', 'unit': 'mg/dL'},
    'LBXSCH': {'loinc': '2093-3', 'name': 'Total cholesterol', 'unit': 'mg/dL'},
    'LBXSTR': {'loinc': '2571-8', 'name': 'Triglycerides', 'unit': 'mg/dL'},
    'LBXSGB': {'loinc': '2276-4', 'name': 'Gamma glutamyl transferase', 'unit': 'U/L'},
    'LBXSIR': {'loinc': '2498-4', 'name': 'Iron', 'unit': 'ug/dL'},
    'LBXSGTSI': {'loinc': '14927-8', 'name': 'GGT', 'unit': 'U/L'},
    
    # Complete Blood Count
    'LBXWBCSI': {'loinc': '6690-2', 'name': 'White blood cell count', 'unit': '1000 cells/uL'},
    'LBXRBCSI': {'loinc': '789-8', 'name': 'Red blood cell count', 'unit': 'million cells/uL'},
    'LBXHGB': {'loinc': '718-7', 'name': 'Hemoglobin', 'unit': 'g/dL'},
    'LBXHCT': {'loinc': '4544-3', 'name': 'Hematocrit', 'unit': '%'},
    'LBXMCVSI': {'loinc': '787-2', 'name': 'Mean cell volume', 'unit': 'fL'},
    'LBXMCHSI': {'loinc': '785-6', 'name': 'Mean cell hemoglobin', 'unit': 'pg'},
    'LBXMC': {'loinc': '786-4', 'name': 'MCHC', 'unit': 'g/dL'},
    'LBXRDW': {'loinc': '788-0', 'name': 'Red cell distribution width', 'unit': '%'},
    'LBXPLTSI': {'loinc': '777-3', 'name': 'Platelet count', 'unit': '1000 cells/uL'},
    'LBXMPSI': {'loinc': '32623-1', 'name': 'Mean platelet volume', 'unit': 'fL'},
    'LBXNEPCT': {'loinc': '770-8', 'name': 'Segmented neutrophils percent', 'unit': '%'},
    'LBXLYPCT': {'loinc': '736-9', 'name': 'Lymphocyte percent', 'unit': '%'},
    'LBXMOPCT': {'loinc': '5905-5', 'name': 'Monocyte percent', 'unit': '%'},
    'LBXEOPCT': {'loinc': '713-8', 'name': 'Eosinophils percent', 'unit': '%'},
    'LBXBAPCT': {'loinc': '706-2', 'name': 'Basophils percent', 'unit': '%'},
}

def calculate_reference_ranges(df: pd.DataFrame, demo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate reference ranges from NHANES data by demographic groups.
    
    Args:
        df: Lab data DataFrame with SEQN as key
        demo_df: Demographics DataFrame with SEQN, RIAGENDR, RIDAGEYR
        
    Returns:
        DataFrame with percentile statistics per group
    """
    # Merge demographics
    merged = df.merge(
        demo_df[['SEQN', 'RIAGENDR', 'RIDAGEYR']], 
        on='SEQN', 
        how='inner'
    )
    
    # Map sex codes
    merged['sex'] = merged['RIAGENDR'].map({1.0: 'M', 2.0: 'F'})
    
    # Create age groups
    age_bins = [0, 1, 5, 12, 18, 30, 45, 60, 75, 120]
    age_labels = ['0-1', '1-5', '5-12', '12-18', '18-30', '30-45', '45-60', '60-75', '75+']
    merged['age_group'] = pd.cut(merged['RIDAGEYR'], bins=age_bins, labels=age_labels)
    
    results = []
    
    for sas_var, meta in NHANES_VARIABLE_MAP.items():
        if sas_var not in merged.columns:
            continue
            
        # Filter valid values
        data = merged[['sex', 'age_group', 'RIDAGEYR', sas_var]].dropna()
        
        if len(data) < 30:  # Minimum sample size
            continue
        
        # Calculate stats by group
        for (sex, age_grp), group in data.groupby(['sex', 'age_group']):
            values = group[sas_var]
            n = len(values)
            
            if n < 20:
                continue
                
            results.append({
                'loinc_code': meta['loinc'],
                'test_name': meta['name'],
                'unit': meta['unit'],
                'sex': sex,
                'age_group': str(age_grp),
                'n_samples': n,
                'mean': values.mean(),
                'std': values.std(),
                'p025': values.quantile(0.025),
                'p05': values.quantile(0.05),
                'p25': values.quantile(0.25),
                'p50': values.quantile(0.50),
                'p75': values.quantile(0.75),
                'p95': values.quantile(0.95),
                'p975': values.quantile(0.975),
            })
    
    return pd.DataFrame(results)
```

### 7.2 CTCAE Excel Parser

```python
"""
CTCAE v5.0 Excel Parser
Extracts adverse event grades with laboratory-specific thresholds.
"""
import pandas as pd
import re
from typing import Dict, List, Optional

def parse_ctcae_excel(filepath: str) -> pd.DataFrame:
    """
    Parse CTCAE Excel file into structured format.
    
    The CTCAE file has a complex structure:
    - Multiple sheets (by MedDRA SOC)
    - Headers with merged cells
    - Grade definitions in columns
    """
    # Read all sheets
    xlsx = pd.ExcelFile(filepath)
    
    all_terms = []
    
    for sheet_name in xlsx.sheet_names:
        if sheet_name.lower() in ['title page', 'instructions', 'toc']:
            continue
            
        df = pd.read_excel(xlsx, sheet_name=sheet_name, header=None)
        
        # Find header row (contains "Grade 1", "Grade 2", etc.)
        header_row = None
        for idx, row in df.iterrows():
            row_str = ' '.join(str(x) for x in row.values)
            if 'Grade 1' in row_str and 'Grade 2' in row_str:
                header_row = idx
                break
        
        if header_row is None:
            continue
            
        # Parse data rows
        headers = df.iloc[header_row].tolist()
        
        # Find column indices
        term_col = next((i for i, h in enumerate(headers) if 'Term' in str(h)), 0)
        grade_cols = {
            g: next((i for i, h in enumerate(headers) if f'Grade {g}' in str(h)), None)
            for g in [1, 2, 3, 4, 5]
        }
        
        for idx in range(header_row + 1, len(df)):
            row = df.iloc[idx]
            term = str(row.iloc[term_col]) if pd.notna(row.iloc[term_col]) else ''
            
            if not term or term == 'nan':
                continue
                
            for grade, col_idx in grade_cols.items():
                if col_idx is None:
                    continue
                    
                definition = str(row.iloc[col_idx]) if pd.notna(row.iloc[col_idx]) else ''
                
                if definition and definition != 'nan' and definition != '-':
                    all_terms.append({
                        'soc': sheet_name,
                        'term': term,
                        'grade': grade,
                        'definition': definition,
                        'has_numeric_threshold': bool(re.search(r'[\d.]+\s*x?\s*ULN|[\d.]+\s*-\s*[\d.]+', definition))
                    })
    
    return pd.DataFrame(all_terms)


def extract_lab_thresholds(ctcae_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract numeric thresholds from CTCAE lab-related terms.
    
    Common patterns:
    - ">ULN - 1.5 x ULN"
    - "<LLN - 75 mg/dL"
    - "1.5 - 3.0 x ULN"
    """
    # Lab-related SOCs
    lab_socs = ['Investigations', 'Metabolism and nutrition disorders']
    
    lab_terms = ctcae_df[ctcae_df['soc'].isin(lab_socs)].copy()
    
    # Pattern matching for thresholds
    patterns = {
        'uln_range': r'([\d.]+)\s*-?\s*([\d.]+)?\s*x?\s*ULN',
        'lln_range': r'([\d.]+)\s*-?\s*([\d.]+)?\s*x?\s*LLN',
        'absolute_range': r'([<>]?)\s*([\d.]+)\s*-?\s*([\d.]+)?\s*(mg/dL|g/dL|mmol/L|%)',
    }
    
    results = []
    
    for _, row in lab_terms.iterrows():
        defn = row['definition']
        
        # Try to extract ULN-based threshold
        uln_match = re.search(patterns['uln_range'], defn, re.IGNORECASE)
        if uln_match:
            results.append({
                'term': row['term'],
                'grade': row['grade'],
                'threshold_type': 'ULN_multiple',
                'threshold_low': float(uln_match.group(1)) if uln_match.group(1) else None,
                'threshold_high': float(uln_match.group(2)) if uln_match.group(2) else None,
                'raw_definition': defn
            })
            continue
            
        # Try absolute values
        abs_match = re.search(patterns['absolute_range'], defn, re.IGNORECASE)
        if abs_match:
            results.append({
                'term': row['term'],
                'grade': row['grade'],
                'threshold_type': 'absolute',
                'operator': abs_match.group(1) or '=',
                'threshold_low': float(abs_match.group(2)) if abs_match.group(2) else None,
                'threshold_high': float(abs_match.group(3)) if abs_match.group(3) else None,
                'unit': abs_match.group(4),
                'raw_definition': defn
            })
    
    return pd.DataFrame(results)


# CTCAE Term to LOINC mapping (manual curation needed)
CTCAE_LOINC_MAP = {
    'Alanine aminotransferase increased': '1742-6',
    'Aspartate aminotransferase increased': '1920-8',
    'Blood bilirubin increased': '1975-2',
    'Alkaline phosphatase increased': '6768-6',
    'Creatinine increased': '2160-0',
    'GGT increased': '2324-2',
    'Lipase increased': '3040-3',
    'Amylase increased': '1798-8',
    'Anemia': '718-7',
    'Lymphocyte count decreased': '731-0',
    'Neutrophil count decreased': '751-8',
    'Platelet count decreased': '777-3',
    'White blood cell decreased': '6690-2',
    'Hyperglycemia': '2345-7',
    'Hypoglycemia': '2345-7',
    'Hyperkalemia': '2823-3',
    'Hypokalemia': '2823-3',
    'Hypernatremia': '2951-2',
    'Hyponatremia': '2951-2',
    'Hypercalcemia': '17861-6',
    'Hypocalcemia': '17861-6',
}
```

### 7.3 MedlinePlus Connect API Client

```python
"""
MedlinePlus Connect API Client
Fetches consumer-friendly health information by LOINC code.
"""
import requests
import json
import time
from typing import Optional, Dict, List
from dataclasses import dataclass
from functools import lru_cache

@dataclass
class MedlinePlusResult:
    title: str
    url: str
    summary: str
    also_called: List[str]
    source: str

class MedlinePlusConnectClient:
    """
    Client for MedlinePlus Connect API.
    
    Rate limit: 100 requests/minute
    Supports caching to minimize API calls.
    """
    
    BASE_URL = "https://connect.medlineplus.gov/service"
    
    # Code system OIDs
    CODE_SYSTEMS = {
        'LOINC': '2.16.840.1.113883.6.1',
        'ICD10CM': '2.16.840.1.113883.6.90',
        'ICD9CM': '2.16.840.1.113883.6.103',
        'SNOMEDCT': '2.16.840.1.113883.6.96',
        'RXNORM': '2.16.840.1.113883.6.88',
        'NDC': '2.16.840.1.113883.6.69',
        'CPT': '2.16.840.1.113883.6.12',
    }
    
    def __init__(self, cache_ttl: int = 86400):
        self.session = requests.Session()
        self.cache_ttl = cache_ttl
        self._request_times = []
        
    def _rate_limit(self):
        """Enforce 100 requests/minute rate limit."""
        now = time.time()
        # Remove requests older than 60 seconds
        self._request_times = [t for t in self._request_times if now - t < 60]
        
        if len(self._request_times) >= 100:
            sleep_time = 60 - (now - self._request_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        self._request_times.append(time.time())
    
    @lru_cache(maxsize=1000)
    def get_by_loinc(self, loinc_code: str, language: str = 'en') -> Optional[MedlinePlusResult]:
        """
        Get health information by LOINC code.
        
        Args:
            loinc_code: LOINC code (e.g., '2345-7')
            language: 'en' for English, 'es' for Spanish
            
        Returns:
            MedlinePlusResult or None if not found
        """
        return self._query(
            code_system='LOINC',
            code=loinc_code,
            language=language
        )
    
    def get_by_icd10(self, icd_code: str, language: str = 'en') -> Optional[MedlinePlusResult]:
        """Get health information by ICD-10-CM code."""
        return self._query(
            code_system='ICD10CM',
            code=icd_code,
            language=language
        )
    
    def _query(
        self, 
        code_system: str, 
        code: str, 
        language: str = 'en'
    ) -> Optional[MedlinePlusResult]:
        """
        Execute query against MedlinePlus Connect.
        """
        self._rate_limit()
        
        params = {
            'mainSearchCriteria.v.cs': self.CODE_SYSTEMS[code_system],
            'mainSearchCriteria.v.c': code,
            'knowledgeResponseType': 'application/json',
        }
        
        if language == 'es':
            params['informationRecipient.languageCode.c'] = 'es'
        
        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Parse response
            feed = data.get('feed', {})
            entries = feed.get('entry', [])
            
            if not entries:
                return None
            
            entry = entries[0]  # Take first result
            
            # Extract summary components
            summaries = entry.get('summary', {})
            summary_text = ''
            also_called = []
            
            if isinstance(summaries, dict):
                summary_text = summaries.get('_value', '')
            elif isinstance(summaries, list):
                for s in summaries:
                    if s.get('_class') == 'NLMalsoCalled':
                        also_called.append(s.get('_value', ''))
                    else:
                        summary_text = s.get('_value', '')
            
            return MedlinePlusResult(
                title=entry.get('title', {}).get('_value', ''),
                url=entry.get('link', [{}])[0].get('href', '') if entry.get('link') else '',
                summary=summary_text,
                also_called=also_called,
                source='MedlinePlus'
            )
            
        except requests.exceptions.RequestException as e:
            print(f"Error querying MedlinePlus: {e}")
            return None


def batch_fetch_descriptions(loinc_codes: List[str]) -> Dict[str, MedlinePlusResult]:
    """
    Fetch descriptions for multiple LOINC codes.
    Respects rate limiting.
    """
    client = MedlinePlusConnectClient()
    results = {}
    
    for code in loinc_codes:
        result = client.get_by_loinc(code)
        if result:
            results[code] = result
        time.sleep(0.6)  # ~100 requests/minute
    
    return results
```

### 7.4 NCI Thesaurus FLAT File Parser

```python
"""
NCI Thesaurus FLAT file parser.
Downloads and parses the tab-delimited terminology file.
"""
import pandas as pd
import zipfile
import requests
from io import BytesIO
from typing import Dict, List, Optional

NCI_THESAURUS_URL = "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Thesaurus.FLAT.zip"

def download_nci_thesaurus(output_dir: str = './raw_data') -> str:
    """Download and extract NCI Thesaurus FLAT file."""
    import os
    
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'Thesaurus.txt')
    
    if os.path.exists(output_path):
        return output_path
    
    print("Downloading NCI Thesaurus (~16MB compressed)...")
    response = requests.get(NCI_THESAURUS_URL, stream=True)
    response.raise_for_status()
    
    with zipfile.ZipFile(BytesIO(response.content)) as zf:
        zf.extract('Thesaurus.txt', output_dir)
    
    return output_path


def parse_nci_thesaurus(filepath: str) -> pd.DataFrame:
    """
    Parse NCI Thesaurus FLAT file.
    
    Format: code <tab> IRI <tab> parents <tab> synonyms <tab> definition <tab> 
            display_name <tab> status <tab> semantic_type <tab> subsets
    """
    columns = [
        'code',
        'concept_iri', 
        'parents',
        'synonyms',
        'definition',
        'display_name',
        'concept_status',
        'semantic_type',
        'concept_in_subset'
    ]
    
    df = pd.read_csv(
        filepath,
        sep='\t',
        names=columns,
        dtype=str,
        na_values=[''],
        keep_default_na=False
    )
    
    # Parse pipe-delimited fields
    df['parent_codes'] = df['parents'].apply(lambda x: x.split('|') if x else [])
    df['synonym_list'] = df['synonyms'].apply(lambda x: x.split('|') if x else [])
    df['preferred_name'] = df['synonym_list'].apply(lambda x: x[0] if x else '')
    
    return df


def extract_lab_concepts(nci_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract laboratory-related concepts from NCI Thesaurus.
    """
    # Semantic types related to labs
    lab_semantic_types = [
        'Laboratory Procedure',
        'Laboratory or Test Result', 
        'Diagnostic Procedure',
        'Clinical Attribute',
        'Finding',
    ]
    
    # Filter by semantic type
    lab_df = nci_df[
        nci_df['semantic_type'].str.contains('|'.join(lab_semantic_types), case=False, na=False)
    ].copy()
    
    return lab_df[['code', 'preferred_name', 'definition', 'semantic_type', 'synonym_list']]


def build_synonym_lookup(nci_df: pd.DataFrame) -> Dict[str, str]:
    """
    Build a lookup table from synonyms to NCI codes.
    Useful for mapping non-standard terms to concepts.
    """
    lookup = {}
    
    for _, row in nci_df.iterrows():
        code = row['code']
        for synonym in row['synonym_list']:
            normalized = synonym.lower().strip()
            if normalized and normalized not in lookup:
                lookup[normalized] = code
    
    return lookup
```

---

## 8. IMPLEMENTATION ROADMAP

### Phase 1: Foundation (Week 1-2)
1. ✅ Download CTCAE v5.0 (already in `downloads/`)
2. Register for free LOINC account, download core table
3. Download NCI Thesaurus FLAT file
4. Set up SQLite database with schema

### Phase 2: Identity Layer (Week 3-4)
1. Ingest LOINC core table
2. Parse and ingest NCI Thesaurus for definitions
3. Clone gpc_doc_ontology repo for Epic/Cerner mappings
4. Download LIVD files from CDC

### Phase 3: Statistical Layer (Week 5-6)
1. Download NHANES 2017-2020 XPT files
2. Run percentile calculations by demographic group
3. Populate reference_distribution table
4. Document data quality notes

### Phase 4: Knowledge Layer (Week 7-8)
1. Implement MedlinePlus Connect client
2. Batch-fetch descriptions for top 2000 LOINC codes
3. Cache results in concept_description table
4. Add synonym mappings from NCI Thesaurus

### Phase 5: Severity Layer (Week 9-10)
1. Parse CTCAE Excel with grade extraction
2. Map CTCAE terms to LOINC codes
3. Implement threshold evaluation logic
4. Add critical value rules from literature

### Phase 6: API & Documentation (Week 11-12)
1. Build REST API for lookups
2. Create search functionality (fuzzy matching)
3. Write user documentation
4. Open-source release preparation

---

## 9. NOTES & CAVEATS

### Data Licensing
- **LOINC**: Free but requires accepting license agreement
- **SNOMED CT**: Free via UMLS license (US users)
- **NCI Thesaurus**: CC BY 4.0
- **NHANES**: Public domain (US Government)
- **CTCAE**: Public domain (NCI/NIH)
- **MedlinePlus**: Public domain, but drug info from ASHP cannot be redistributed

### Data Quality Considerations
1. NHANES reference ranges are population-based, not clinical decision thresholds
2. CTCAE thresholds are relative to patient's baseline, not absolute values
3. LIS codes vary significantly between institutions
4. LOINC mappings require clinical validation

### Maintenance Requirements
- LOINC: Updates twice yearly
- NHANES: New cycles every 2 years
- CTCAE: Major versions every 5-10 years
- MedlinePlus: Content updated continuously
- NCI Thesaurus: Monthly releases

---

*Report compiled from publicly available sources. All URLs verified as of December 2025.*
