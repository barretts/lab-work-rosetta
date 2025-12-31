#!/usr/bin/env python3
"""
Clinical Rosetta Stone - Database Schema
Enhanced schema for linking LIS shorthands to standardized codes.
"""

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
-- ============================================
-- CLINICAL ROSETTA STONE DATABASE SCHEMA v2
-- ============================================

-- ---------------------------------------------
-- LAYER 1: CORE IDENTITY (The Standard)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS loinc_concept (
    loinc_code TEXT PRIMARY KEY,
    component TEXT,
    property TEXT,
    time_aspect TEXT,
    system TEXT,
    scale_type TEXT,
    method_type TEXT,
    long_common_name TEXT,
    short_name TEXT,
    class TEXT,
    class_type TEXT,
    status TEXT DEFAULT 'ACTIVE',
    rank_frequency INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_loinc_component ON loinc_concept(component);
CREATE INDEX IF NOT EXISTS idx_loinc_class ON loinc_concept(class);
CREATE INDEX IF NOT EXISTS idx_loinc_short_name ON loinc_concept(short_name);

-- ---------------------------------------------
-- LAYER 2: MAPPING (LIS Shorthand → Standard)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS source_system (
    system_id INTEGER PRIMARY KEY AUTOINCREMENT,
    system_name TEXT UNIQUE NOT NULL,
    system_type TEXT,  -- 'EHR', 'LIS', 'Reference Lab', 'IVD'
    vendor TEXT,
    version TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lis_mapping (
    mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_system_id INTEGER REFERENCES source_system(system_id),
    source_code TEXT,
    source_description TEXT,
    target_loinc TEXT REFERENCES loinc_concept(loinc_code),
    mapping_confidence REAL DEFAULT 1.0,
    mapping_source TEXT,  -- 'LIVD', 'Manual', 'NCI', 'Inferred'
    verified_date TEXT,
    notes TEXT,
    UNIQUE(source_system_id, source_code, target_loinc)
);

CREATE INDEX IF NOT EXISTS idx_mapping_source_desc ON lis_mapping(source_description);
CREATE INDEX IF NOT EXISTS idx_mapping_loinc ON lis_mapping(target_loinc);
CREATE INDEX IF NOT EXISTS idx_mapping_source_code ON lis_mapping(source_code);

-- Cross-vocabulary mappings (SNOMED, ICD-10, etc.)
CREATE TABLE IF NOT EXISTS vocabulary_mapping (
    mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_vocabulary TEXT NOT NULL,
    source_code TEXT NOT NULL,
    source_name TEXT,
    target_vocabulary TEXT NOT NULL,
    target_code TEXT NOT NULL,
    target_name TEXT,
    relationship_type TEXT DEFAULT 'MAPS_TO',
    mapping_source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_vocabulary, source_code, target_vocabulary, target_code)
);

CREATE INDEX IF NOT EXISTS idx_vocab_map_source ON vocabulary_mapping(source_vocabulary, source_code);
CREATE INDEX IF NOT EXISTS idx_vocab_map_target ON vocabulary_mapping(target_vocabulary, target_code);

-- ---------------------------------------------
-- LAYER 3: STATISTICAL (Reference Ranges)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS reference_population (
    population_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_name TEXT UNIQUE NOT NULL,
    description TEXT,
    sample_size INTEGER,
    collection_start TEXT,
    collection_end TEXT,
    geographic_region TEXT,
    source_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS reference_distribution (
    dist_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loinc_code TEXT REFERENCES loinc_concept(loinc_code),
    population_id INTEGER REFERENCES reference_population(population_id),
    
    -- Demographics
    sex TEXT,  -- 'M', 'F', 'A' (All)
    age_min_years REAL,
    age_max_years REAL,
    race_ethnicity TEXT,
    
    -- Statistics
    n_samples INTEGER,
    mean REAL,
    std_dev REAL,
    p01 REAL,
    p025 REAL,
    p05 REAL,
    p25 REAL,
    p50 REAL,
    p75 REAL,
    p95 REAL,
    p975 REAL,
    p99 REAL,
    
    -- Clinical Reference Range
    ref_low REAL,
    ref_high REAL,
    unit_of_measure TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(loinc_code, population_id, sex, age_min_years, age_max_years, race_ethnicity)
);

CREATE INDEX IF NOT EXISTS idx_ref_dist_loinc ON reference_distribution(loinc_code);
CREATE INDEX IF NOT EXISTS idx_ref_dist_demo ON reference_distribution(sex, age_min_years, age_max_years);

-- ---------------------------------------------
-- LAYER 4: KNOWLEDGE (Human-Readable Context)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS concept_description (
    desc_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loinc_code TEXT REFERENCES loinc_concept(loinc_code),
    source TEXT NOT NULL,
    language TEXT DEFAULT 'en',
    description_type TEXT,  -- 'consumer', 'professional', 'brief'
    description_text TEXT,
    source_url TEXT,
    retrieved_date TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(loinc_code, source, language, description_type)
);

CREATE INDEX IF NOT EXISTS idx_desc_loinc ON concept_description(loinc_code);

CREATE TABLE IF NOT EXISTS concept_synonym (
    synonym_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loinc_code TEXT REFERENCES loinc_concept(loinc_code),
    synonym_text TEXT NOT NULL,
    synonym_type TEXT,  -- 'abbreviation', 'lay_term', 'clinical', 'lis_shorthand'
    source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(loinc_code, synonym_text, source)
);

CREATE INDEX IF NOT EXISTS idx_synonym_text ON concept_synonym(synonym_text);
CREATE INDEX IF NOT EXISTS idx_synonym_loinc ON concept_synonym(loinc_code);

-- NCI Thesaurus concepts for definitions
CREATE TABLE IF NOT EXISTS nci_concept (
    nci_code TEXT PRIMARY KEY,
    preferred_name TEXT,
    definition TEXT,
    semantic_type TEXT,
    parents TEXT,  -- pipe-delimited parent codes
    synonyms TEXT,  -- pipe-delimited synonyms
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_nci_name ON nci_concept(preferred_name);

-- ---------------------------------------------
-- LAYER 5: SEVERITY (Clinical Alerts)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS severity_standard (
    standard_id INTEGER PRIMARY KEY AUTOINCREMENT,
    standard_name TEXT UNIQUE NOT NULL,
    version TEXT,
    effective_date TEXT,
    description TEXT,
    source_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS severity_rule (
    rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loinc_code TEXT REFERENCES loinc_concept(loinc_code),
    standard_id INTEGER REFERENCES severity_standard(standard_id),
    
    -- Thresholds
    direction TEXT,  -- 'HIGH', 'LOW', 'ABNORMAL'
    threshold_value REAL,
    threshold_operator TEXT,  -- '>', '>=', '<', '<=', '='
    threshold_uln_multiple REAL,  -- For relative thresholds like "3x ULN"
    unit_of_measure TEXT,
    
    -- Severity
    severity_level TEXT,  -- 'Critical', 'Grade_1', etc.
    severity_code INTEGER,
    severity_label TEXT,
    clinical_description TEXT,
    
    -- Context
    applies_to_age_min REAL,
    applies_to_age_max REAL,
    applies_to_sex TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_severity_loinc ON severity_rule(loinc_code);
CREATE INDEX IF NOT EXISTS idx_severity_level ON severity_rule(severity_level);

-- CTCAE terms table
CREATE TABLE IF NOT EXISTS ctcae_term (
    term_id INTEGER PRIMARY KEY AUTOINCREMENT,
    soc TEXT,  -- System Organ Class
    term_name TEXT NOT NULL,
    grade INTEGER NOT NULL,
    definition TEXT,
    loinc_code TEXT REFERENCES loinc_concept(loinc_code),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(term_name, grade)
);

CREATE INDEX IF NOT EXISTS idx_ctcae_term ON ctcae_term(term_name);
CREATE INDEX IF NOT EXISTS idx_ctcae_loinc ON ctcae_term(loinc_code);

-- ---------------------------------------------
-- VIEWS FOR COMMON QUERIES
-- ---------------------------------------------

-- Complete test profile lookup
CREATE VIEW IF NOT EXISTS v_test_profile AS
SELECT 
    l.loinc_code,
    l.long_common_name,
    l.short_name,
    l.component,
    l.class,
    l.scale_type,
    l.rank_frequency,
    d.description_text AS consumer_description,
    d.source AS description_source
FROM loinc_concept l
LEFT JOIN concept_description d 
    ON l.loinc_code = d.loinc_code 
    AND d.description_type = 'consumer'
    AND d.language = 'en';

-- LIS code translation view
CREATE VIEW IF NOT EXISTS v_lis_translation AS
SELECT 
    m.source_code AS lis_code,
    m.source_description AS lis_name,
    s.system_name,
    s.vendor,
    l.loinc_code,
    l.long_common_name AS standard_name,
    l.component,
    m.mapping_confidence
FROM lis_mapping m
JOIN source_system s ON m.source_system_id = s.system_id
LEFT JOIN loinc_concept l ON m.target_loinc = l.loinc_code;

-- Reference ranges with demographics
CREATE VIEW IF NOT EXISTS v_reference_ranges AS
SELECT
    l.loinc_code,
    l.long_common_name,
    l.component,
    p.dataset_name,
    r.sex,
    r.age_min_years,
    r.age_max_years,
    r.n_samples,
    r.p025 AS ref_low_2_5,
    r.p975 AS ref_high_97_5,
    r.p50 AS median,
    r.mean,
    r.std_dev,
    r.unit_of_measure
FROM reference_distribution r
JOIN loinc_concept l ON r.loinc_code = l.loinc_code
JOIN reference_population p ON r.population_id = p.population_id;

-- Critical values view
CREATE VIEW IF NOT EXISTS v_critical_values AS
SELECT
    l.loinc_code,
    l.long_common_name,
    sr.direction,
    sr.threshold_value,
    sr.threshold_operator,
    sr.unit_of_measure,
    sr.severity_level,
    sr.clinical_description,
    ss.standard_name
FROM severity_rule sr
JOIN loinc_concept l ON sr.loinc_code = l.loinc_code
JOIN severity_standard ss ON sr.standard_id = ss.standard_id
WHERE sr.severity_level IN ('Critical', 'Grade_4', 'Grade_5');

-- Synonym search view  
CREATE VIEW IF NOT EXISTS v_synonym_search AS
SELECT 
    cs.synonym_text,
    cs.synonym_type,
    l.loinc_code,
    l.long_common_name,
    l.short_name,
    l.component
FROM concept_synonym cs
JOIN loinc_concept l ON cs.loinc_code = l.loinc_code;
"""


def init_database(db_path: str = "clinical_rosetta.db") -> sqlite3.Connection:
    """Initialize the database with the schema."""
    logger.info(f"Initializing database: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")

    # Execute schema
    conn.executescript(SCHEMA_SQL)
    conn.commit()

    logger.info("Database schema initialized successfully")
    return conn


def get_table_counts(conn: sqlite3.Connection) -> dict:
    """Get row counts for all tables."""
    tables = [
        "loinc_concept",
        "source_system",
        "lis_mapping",
        "vocabulary_mapping",
        "reference_population",
        "reference_distribution",
        "concept_description",
        "concept_synonym",
        "nci_concept",
        "severity_standard",
        "severity_rule",
        "ctcae_term",
    ]

    counts = {}
    for table in tables:
        try:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
        except sqlite3.OperationalError:
            counts[table] = 0

    return counts


def print_schema_summary(conn: sqlite3.Connection):
    """Print a summary of the database schema."""
    counts = get_table_counts(conn)

    print("\n" + "=" * 50)
    print("CLINICAL ROSETTA STONE DATABASE")
    print("=" * 50)

    layers = {
        "IDENTITY": ["loinc_concept", "source_system", "lis_mapping", "vocabulary_mapping"],
        "STATISTICAL": ["reference_population", "reference_distribution"],
        "KNOWLEDGE": ["concept_description", "concept_synonym", "nci_concept"],
        "SEVERITY": ["severity_standard", "severity_rule", "ctcae_term"],
    }

    for layer, tables in layers.items():
        print(f"\n{layer} LAYER:")
        for table in tables:
            print(f"  {table}: {counts.get(table, 0):,} rows")

    total = sum(counts.values())
    print(f"\nTOTAL RECORDS: {total:,}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    conn = init_database()
    print_schema_summary(conn)
    conn.close()
