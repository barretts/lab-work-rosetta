#!/usr/bin/env python3
"""
Clinical Rosetta Stone - Data Enrichment Module
Adds curated mappings, critical values, and API integrations.
"""

import sqlite3
import pandas as pd
import requests
import time
import logging
from pathlib import Path
from typing import Optional, Dict, List
from functools import lru_cache

logger = logging.getLogger(__name__)

# Project root is two levels up from scripts/setup/
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "clinical_rosetta.db"
DATA_DIR = PROJECT_ROOT / "raw_data"


# ============================================================================
# CURATED INSTITUTION MAPPINGS
# Based on published standards and common LIS conventions
# ============================================================================

# Common LIS shorthands from multiple institutions
# Sources: ARUP catalog patterns, Quest naming conventions, hospital lab manuals
CURATED_LIS_MAPPINGS = {
    # ----- COMPLETE BLOOD COUNT -----
    "Generic_LIS": [
        # WBC and differentials
        ("WBC", "6690-2", "White Blood Cell Count"),
        ("White Count", "6690-2", "White Blood Cell Count"),
        ("Leukocytes", "6690-2", "White Blood Cell Count"),
        ("RBC", "789-8", "Red Blood Cell Count"),
        ("Red Count", "789-8", "Red Blood Cell Count"),
        ("Erythrocytes", "789-8", "Red Blood Cell Count"),
        ("HGB", "718-7", "Hemoglobin"),
        ("Hgb", "718-7", "Hemoglobin"),
        ("HCT", "4544-3", "Hematocrit"),
        ("Hct", "4544-3", "Hematocrit"),
        ("PLT", "777-3", "Platelet Count"),
        ("Platelets", "777-3", "Platelet Count"),
        ("Plt Count", "777-3", "Platelet Count"),
        ("MCV", "787-2", "Mean Corpuscular Volume"),
        ("MCH", "785-6", "Mean Corpuscular Hemoglobin"),
        ("MCHC", "786-4", "Mean Corpuscular Hemoglobin Concentration"),
        ("RDW", "788-0", "Red Cell Distribution Width"),
        ("RDW-CV", "788-0", "Red Cell Distribution Width"),
        ("MPV", "32623-1", "Mean Platelet Volume"),
        
        # Differential
        ("Neut %", "770-8", "Neutrophils %"),
        ("Neutrophils", "770-8", "Neutrophils %"),
        ("Segs", "770-8", "Neutrophils %"),
        ("Polys", "770-8", "Neutrophils %"),
        ("Lymph %", "736-9", "Lymphocytes %"),
        ("Lymphocytes", "736-9", "Lymphocytes %"),
        ("Lymphs", "736-9", "Lymphocytes %"),
        ("Mono %", "5905-5", "Monocytes %"),
        ("Monocytes", "5905-5", "Monocytes %"),
        ("Monos", "5905-5", "Monocytes %"),
        ("Eos %", "713-8", "Eosinophils %"),
        ("Eosinophils", "713-8", "Eosinophils %"),
        ("Eos", "713-8", "Eosinophils %"),
        ("Baso %", "706-2", "Basophils %"),
        ("Basophils", "706-2", "Basophils %"),
        ("Basos", "706-2", "Basophils %"),
        
        # Absolute counts
        ("ANC", "751-8", "Absolute Neutrophil Count"),
        ("Neut Abs", "751-8", "Absolute Neutrophil Count"),
        ("Neutro Absolute", "751-8", "Absolute Neutrophil Count"),
        ("ALC", "731-0", "Absolute Lymphocyte Count"),
        ("Lymph Abs", "731-0", "Absolute Lymphocyte Count"),
        ("AMC", "742-7", "Absolute Monocyte Count"),
        ("Mono Abs", "742-7", "Absolute Monocyte Count"),
        ("AEC", "711-2", "Absolute Eosinophil Count"),
        ("Eos Abs", "711-2", "Absolute Eosinophil Count"),
    ],
    
    "LabCorp_Style": [
        # Basic Metabolic Panel
        ("Glucose", "2345-7", "Glucose, Serum"),
        ("Gluc", "2345-7", "Glucose, Serum"),
        ("BUN", "3094-0", "Blood Urea Nitrogen"),
        ("Urea Nitrogen", "3094-0", "Blood Urea Nitrogen"),
        ("Creatinine", "2160-0", "Creatinine, Serum"),
        ("Creat", "2160-0", "Creatinine, Serum"),
        ("Sodium", "2951-2", "Sodium, Serum"),
        ("Na", "2951-2", "Sodium, Serum"),
        ("Potassium", "2823-3", "Potassium, Serum"),
        ("K", "2823-3", "Potassium, Serum"),
        ("Chloride", "2075-0", "Chloride, Serum"),
        ("Cl", "2075-0", "Chloride, Serum"),
        ("CO2", "2028-9", "Carbon Dioxide, Total"),
        ("Bicarb", "2028-9", "Carbon Dioxide, Total"),
        ("HCO3", "2028-9", "Carbon Dioxide, Total"),
        ("Calcium", "17861-6", "Calcium, Serum"),
        ("Ca", "17861-6", "Calcium, Serum"),
        
        # Comprehensive Metabolic Panel additions
        ("Albumin", "1751-7", "Albumin, Serum"),
        ("Alb", "1751-7", "Albumin, Serum"),
        ("Total Protein", "2885-2", "Total Protein, Serum"),
        ("TP", "2885-2", "Total Protein, Serum"),
        ("Bilirubin Total", "1975-2", "Bilirubin, Total"),
        ("T Bili", "1975-2", "Bilirubin, Total"),
        ("TBili", "1975-2", "Bilirubin, Total"),
        ("Bilirubin Direct", "1968-7", "Bilirubin, Direct"),
        ("D Bili", "1968-7", "Bilirubin, Direct"),
        ("DBili", "1968-7", "Bilirubin, Direct"),
        ("Alk Phos", "6768-6", "Alkaline Phosphatase"),
        ("ALP", "6768-6", "Alkaline Phosphatase"),
        ("Alkaline Phosphatase", "6768-6", "Alkaline Phosphatase"),
        ("AST", "1920-8", "Aspartate Aminotransferase"),
        ("SGOT", "1920-8", "Aspartate Aminotransferase"),
        ("ALT", "1742-6", "Alanine Aminotransferase"),
        ("SGPT", "1742-6", "Alanine Aminotransferase"),
        ("GGT", "2324-2", "Gamma Glutamyl Transferase"),
        ("GGTP", "2324-2", "Gamma Glutamyl Transferase"),
    ],
    
    "Quest_Style": [
        # Lipid Panel
        ("Cholesterol", "2093-3", "Cholesterol, Total"),
        ("Total Chol", "2093-3", "Cholesterol, Total"),
        ("TC", "2093-3", "Cholesterol, Total"),
        ("Triglycerides", "2571-8", "Triglycerides"),
        ("Trig", "2571-8", "Triglycerides"),
        ("TG", "2571-8", "Triglycerides"),
        ("HDL", "2085-9", "HDL Cholesterol"),
        ("HDL-C", "2085-9", "HDL Cholesterol"),
        ("LDL", "13457-7", "LDL Cholesterol, Calculated"),
        ("LDL-C", "13457-7", "LDL Cholesterol, Calculated"),
        ("LDL Calc", "13457-7", "LDL Cholesterol, Calculated"),
        ("VLDL", "13458-5", "VLDL Cholesterol, Calculated"),
        
        # Thyroid
        ("TSH", "3016-3", "Thyroid Stimulating Hormone"),
        ("T4 Free", "3024-7", "Free T4"),
        ("FT4", "3024-7", "Free T4"),
        ("T3 Free", "3051-0", "Free T3"),
        ("FT3", "3051-0", "Free T3"),
        ("T4 Total", "3026-2", "Total T4"),
        ("T3 Total", "3053-6", "Total T3"),
        
        # Diabetes
        ("HbA1c", "4548-4", "Hemoglobin A1c"),
        ("A1C", "4548-4", "Hemoglobin A1c"),
        ("Glycohemoglobin", "4548-4", "Hemoglobin A1c"),
        ("Fasting Glucose", "1558-6", "Fasting Glucose"),
        ("FBG", "1558-6", "Fasting Glucose"),
    ],
    
    "Hospital_Common": [
        # Cardiac markers
        ("Troponin I", "10839-9", "Troponin I"),
        ("TnI", "10839-9", "Troponin I"),
        ("Troponin T", "6598-7", "Troponin T"),
        ("TnT", "6598-7", "Troponin T"),
        ("BNP", "30934-4", "B-type Natriuretic Peptide"),
        ("NT-proBNP", "33762-6", "NT-proBNP"),
        ("CK", "2157-6", "Creatine Kinase"),
        ("CPK", "2157-6", "Creatine Kinase"),
        ("CK-MB", "13969-1", "Creatine Kinase MB"),
        ("LDH", "2532-0", "Lactate Dehydrogenase"),
        
        # Coagulation
        ("PT", "5902-2", "Prothrombin Time"),
        ("Pro Time", "5902-2", "Prothrombin Time"),
        ("INR", "6301-6", "INR"),
        ("PTT", "3173-2", "Partial Thromboplastin Time"),
        ("aPTT", "3173-2", "Partial Thromboplastin Time"),
        ("Fibrinogen", "3255-7", "Fibrinogen"),
        ("D-Dimer", "48065-7", "D-Dimer"),
        
        # Iron studies
        ("Iron", "2498-4", "Iron, Serum"),
        ("Fe", "2498-4", "Iron, Serum"),
        ("TIBC", "2500-7", "Total Iron Binding Capacity"),
        ("Ferritin", "2276-4", "Ferritin"),
        ("Transferrin", "3034-6", "Transferrin"),
        ("Iron Sat", "2502-3", "Iron Saturation"),
        ("TSAT", "2502-3", "Iron Saturation"),
        
        # Inflammatory markers
        ("CRP", "1988-5", "C-Reactive Protein"),
        ("hs-CRP", "30522-7", "High Sensitivity CRP"),
        ("ESR", "30341-2", "Erythrocyte Sedimentation Rate"),
        ("Sed Rate", "30341-2", "Erythrocyte Sedimentation Rate"),
        ("Procalcitonin", "75241-0", "Procalcitonin"),
        ("PCT", "75241-0", "Procalcitonin"),
        
        # Renal
        ("eGFR", "33914-3", "Estimated GFR"),
        ("GFR", "33914-3", "Estimated GFR"),
        ("Uric Acid", "3084-1", "Uric Acid"),
        ("UA", "3084-1", "Uric Acid"),
        ("Phosphorus", "2777-1", "Phosphorus, Serum"),
        ("Phos", "2777-1", "Phosphorus, Serum"),
        ("Magnesium", "19123-9", "Magnesium, Serum"),
        ("Mag", "19123-9", "Magnesium, Serum"),
        ("Mg", "19123-9", "Magnesium, Serum"),
        
        # Liver
        ("Ammonia", "1841-6", "Ammonia"),
        ("NH3", "1841-6", "Ammonia"),
        ("Amylase", "1798-8", "Amylase"),
        ("Lipase", "3040-3", "Lipase"),
        
        # Electrolytes/ABG
        ("Lactate", "2524-7", "Lactate"),
        ("Lactic Acid", "2524-7", "Lactate"),
        ("pH", "2744-1", "pH, Blood"),
        ("pCO2", "2019-8", "pCO2"),
        ("pO2", "2703-7", "pO2"),
        ("Base Excess", "1925-7", "Base Excess"),
        ("BE", "1925-7", "Base Excess"),
        
        # Urinalysis
        ("UA Glucose", "25428-4", "Urine Glucose"),
        ("UA Protein", "20454-5", "Urine Protein"),
        ("UA Blood", "5794-3", "Urine Blood"),
        ("UA WBC", "5821-4", "Urine WBC"),
        ("UA Bacteria", "25145-4", "Urine Bacteria"),
        ("Specific Gravity", "5811-5", "Urine Specific Gravity"),
        ("SG", "5811-5", "Urine Specific Gravity"),
    ],
    
    "ARUP_Style": [
        # Vitamins and minerals
        ("Vitamin D", "1989-3", "Vitamin D, 25-Hydroxy"),
        ("25-OH Vit D", "1989-3", "Vitamin D, 25-Hydroxy"),
        ("Vit D 25", "1989-3", "Vitamin D, 25-Hydroxy"),
        ("Vitamin B12", "2132-9", "Vitamin B12"),
        ("B12", "2132-9", "Vitamin B12"),
        ("Folate", "2284-8", "Folate, Serum"),
        ("Folic Acid", "2284-8", "Folate, Serum"),
        
        # Hormones
        ("Cortisol AM", "2143-6", "Cortisol, AM"),
        ("Testosterone", "2986-8", "Testosterone, Total"),
        ("Test Total", "2986-8", "Testosterone, Total"),
        ("Free Test", "2991-8", "Testosterone, Free"),
        ("Estradiol", "2243-4", "Estradiol"),
        ("E2", "2243-4", "Estradiol"),
        ("FSH", "15067-2", "FSH"),
        ("LH", "10501-5", "LH"),
        ("Prolactin", "2842-3", "Prolactin"),
        ("PRL", "2842-3", "Prolactin"),
        
        # Tumor markers
        ("PSA", "2857-1", "PSA, Total"),
        ("CEA", "2039-6", "CEA"),
        ("AFP", "1834-1", "Alpha-fetoprotein"),
        ("CA-125", "10334-1", "CA-125"),
        ("CA 19-9", "24108-3", "CA 19-9"),
    ],
}


# ============================================================================
# CRITICAL VALUES FROM PUBLISHED LITERATURE
# Sources: CAP, CLSI, Major hospital consensus guidelines
# ============================================================================

CRITICAL_VALUES = [
    # Hematology
    {"loinc": "718-7", "name": "Hemoglobin", "low": 7.0, "high": 20.0, "unit": "g/dL"},
    {"loinc": "4544-3", "name": "Hematocrit", "low": 20.0, "high": 60.0, "unit": "%"},
    {"loinc": "777-3", "name": "Platelet Count", "low": 20.0, "high": 1000.0, "unit": "10^3/uL"},
    {"loinc": "6690-2", "name": "WBC", "low": 2.0, "high": 30.0, "unit": "10^3/uL"},
    {"loinc": "751-8", "name": "ANC", "low": 0.5, "high": None, "unit": "10^3/uL"},
    
    # Chemistry
    {"loinc": "2345-7", "name": "Glucose", "low": 40.0, "high": 500.0, "unit": "mg/dL"},
    {"loinc": "2823-3", "name": "Potassium", "low": 2.5, "high": 6.5, "unit": "mEq/L"},
    {"loinc": "2951-2", "name": "Sodium", "low": 120.0, "high": 160.0, "unit": "mEq/L"},
    {"loinc": "17861-6", "name": "Calcium", "low": 6.0, "high": 13.0, "unit": "mg/dL"},
    {"loinc": "19123-9", "name": "Magnesium", "low": 1.0, "high": 4.0, "unit": "mg/dL"},
    {"loinc": "2777-1", "name": "Phosphorus", "low": 1.0, "high": 8.5, "unit": "mg/dL"},
    {"loinc": "2160-0", "name": "Creatinine", "low": None, "high": 10.0, "unit": "mg/dL"},
    {"loinc": "1975-2", "name": "Bilirubin, Total", "low": None, "high": 15.0, "unit": "mg/dL"},
    {"loinc": "1841-6", "name": "Ammonia", "low": None, "high": 100.0, "unit": "umol/L"},
    
    # Cardiac
    {"loinc": "10839-9", "name": "Troponin I", "low": None, "high": 0.5, "unit": "ng/mL"},
    {"loinc": "6598-7", "name": "Troponin T", "low": None, "high": 0.1, "unit": "ng/mL"},
    {"loinc": "2524-7", "name": "Lactate", "low": None, "high": 4.0, "unit": "mmol/L"},
    
    # Coagulation
    {"loinc": "6301-6", "name": "INR", "low": None, "high": 5.0, "unit": "ratio"},
    {"loinc": "3173-2", "name": "PTT", "low": None, "high": 100.0, "unit": "sec"},
    {"loinc": "3255-7", "name": "Fibrinogen", "low": 100.0, "high": None, "unit": "mg/dL"},
    
    # Blood Gas
    {"loinc": "2744-1", "name": "pH, Blood", "low": 7.2, "high": 7.6, "unit": "pH"},
    {"loinc": "2019-8", "name": "pCO2", "low": 20.0, "high": 70.0, "unit": "mmHg"},
    {"loinc": "2703-7", "name": "pO2", "low": 40.0, "high": None, "unit": "mmHg"},
]


# ============================================================================
# ENRICHMENT CLASS
# ============================================================================

class RosettaEnrichment:
    """Enriches the Clinical Rosetta database with curated data."""
    
    def __init__(self, db_path: str = DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        
    def close(self):
        if self.conn:
            self.conn.close()
    
    def ingest_curated_mappings(self):
        """Ingest all curated LIS mappings."""
        logger.info("Ingesting curated LIS mappings...")
        
        total = 0
        for system_name, mappings in CURATED_LIS_MAPPINGS.items():
            # Create source system
            self.conn.execute("""
                INSERT OR IGNORE INTO source_system (system_name, system_type, vendor)
                VALUES (?, ?, ?)
            """, (system_name, 'LIS', 'Curated'))
            
            cursor = self.conn.execute(
                "SELECT system_id FROM source_system WHERE system_name = ?",
                (system_name,)
            )
            system_id = cursor.fetchone()[0]
            
            for shorthand, loinc, name in mappings:
                # Ensure LOINC exists
                self.conn.execute("""
                    INSERT OR IGNORE INTO loinc_concept (loinc_code, long_common_name)
                    VALUES (?, ?)
                """, (loinc, name))
                
                # Add mapping
                try:
                    self.conn.execute("""
                        INSERT OR IGNORE INTO lis_mapping 
                        (source_system_id, source_code, source_description, target_loinc, mapping_source)
                        VALUES (?, ?, ?, ?, ?)
                    """, (system_id, shorthand, name, loinc, 'curated_standards'))
                    total += 1
                except Exception as e:
                    logger.error(f"Error inserting {shorthand}: {e}")
        
        self.conn.commit()
        logger.info(f"Ingested {total} curated LIS mappings")
        return total
    
    def ingest_critical_values(self):
        """Ingest critical/panic values."""
        logger.info("Ingesting critical values...")
        
        # Create severity standard
        self.conn.execute("""
            INSERT OR IGNORE INTO severity_standard 
            (standard_name, version, description)
            VALUES (?, ?, ?)
        """, ('Critical_Values', '1.0', 'Consensus critical values from CAP/CLSI guidelines'))
        
        cursor = self.conn.execute(
            "SELECT standard_id FROM severity_standard WHERE standard_name = 'Critical_Values'"
        )
        standard_id = cursor.fetchone()[0]
        
        count = 0
        for cv in CRITICAL_VALUES:
            # Ensure LOINC exists
            self.conn.execute("""
                INSERT OR IGNORE INTO loinc_concept (loinc_code, long_common_name)
                VALUES (?, ?)
            """, (cv['loinc'], cv['name']))
            
            # Add low critical value
            if cv.get('low') is not None:
                self.conn.execute("""
                    INSERT OR IGNORE INTO severity_rule 
                    (loinc_code, standard_id, direction, threshold_value, threshold_operator,
                     unit_of_measure, severity_level, severity_label, clinical_description)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    cv['loinc'], standard_id, 'LOW', cv['low'], '<',
                    cv['unit'], 'Critical', 'Critical Low',
                    f"Critical low {cv['name']}: < {cv['low']} {cv['unit']}"
                ))
                count += 1
            
            # Add high critical value
            if cv.get('high') is not None:
                self.conn.execute("""
                    INSERT OR IGNORE INTO severity_rule 
                    (loinc_code, standard_id, direction, threshold_value, threshold_operator,
                     unit_of_measure, severity_level, severity_label, clinical_description)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    cv['loinc'], standard_id, 'HIGH', cv['high'], '>',
                    cv['unit'], 'Critical', 'Critical High',
                    f"Critical high {cv['name']}: > {cv['high']} {cv['unit']}"
                ))
                count += 1
        
        self.conn.commit()
        logger.info(f"Ingested {count} critical value rules")
        return count
    
    def ingest_loinc2hpo(self):
        """Ingest loinc2hpo annotations."""
        logger.info("Ingesting loinc2hpo annotations...")
        
        filepath = DATA_DIR / "loinc2hpo_annotations.tsv"
        if not filepath.exists():
            logger.warning(f"loinc2hpo file not found: {filepath}")
            return 0
        
        df = pd.read_csv(filepath, sep='\t')
        
        # Create source system for HPO mappings
        self.conn.execute("""
            INSERT OR IGNORE INTO source_system 
            (system_name, system_type, vendor)
            VALUES (?, ?, ?)
        """, ('HPO_Phenotype', 'Ontology', 'Monarch Initiative'))
        
        count = 0
        unique_loincs = df['loincId'].unique()
        
        for loinc in unique_loincs:
            # Add as vocabulary mapping (LOINC -> HPO)
            subset = df[df['loincId'] == loinc]
            
            for _, row in subset.iterrows():
                hpo_id = row['hpoTermId']
                outcome = row['outcome']  # H, L, N, POS, NEG
                
                try:
                    self.conn.execute("""
                        INSERT OR IGNORE INTO vocabulary_mapping 
                        (source_vocabulary, source_code, target_vocabulary, target_code, 
                         relationship_type, mapping_source)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        'LOINC', str(loinc), 'HPO', hpo_id,
                        f'OUTCOME_{outcome}', 'loinc2hpo'
                    ))
                    count += 1
                except Exception as e:
                    pass
        
        self.conn.commit()
        logger.info(f"Ingested {count} LOINC-to-HPO mappings ({len(unique_loincs)} unique LOINC codes)")
        return count
    
    def fetch_medlineplus_descriptions(self, limit: int = 50):
        """Fetch consumer descriptions from MedlinePlus Connect API."""
        logger.info("Fetching MedlinePlus descriptions...")
        
        # Get LOINC codes that don't have descriptions yet
        cursor = self.conn.execute("""
            SELECT loinc_code, long_common_name FROM loinc_concept
            WHERE loinc_code NOT IN (
                SELECT loinc_code FROM concept_description WHERE source = 'MedlinePlus'
            )
            ORDER BY rank_frequency DESC NULLS LAST
            LIMIT ?
        """, (limit,))
        
        codes = cursor.fetchall()
        logger.info(f"Fetching descriptions for {len(codes)} LOINC codes...")
        
        base_url = "https://connect.medlineplus.gov/service"
        loinc_oid = "2.16.840.1.113883.6.1"
        
        fetched = 0
        for loinc_code, name in codes:
            try:
                params = {
                    'mainSearchCriteria.v.cs': loinc_oid,
                    'mainSearchCriteria.v.c': loinc_code,
                    'knowledgeResponseType': 'application/json',
                }
                
                response = requests.get(base_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    feed = data.get('feed', {})
                    entries = feed.get('entry', [])
                    
                    if entries:
                        entry = entries[0]
                        title = entry.get('title', {}).get('_value', '')
                        
                        # Extract summary
                        summary_data = entry.get('summary', {})
                        summary = ''
                        if isinstance(summary_data, dict):
                            summary = summary_data.get('_value', '')
                        elif isinstance(summary_data, list):
                            for s in summary_data:
                                if isinstance(s, dict) and '_value' in s:
                                    summary = s.get('_value', '')
                                    break
                        
                        if summary:
                            self.conn.execute("""
                                INSERT OR REPLACE INTO concept_description 
                                (loinc_code, source, language, description_type, description_text, 
                                 source_url, retrieved_date)
                                VALUES (?, ?, ?, ?, ?, ?, date('now'))
                            """, (
                                loinc_code, 'MedlinePlus', 'en', 'consumer',
                                summary[:2000],  # Truncate if too long
                                f"https://medlineplus.gov/lab-tests/{title.lower().replace(' ', '-')}/"
                            ))
                            fetched += 1
                            logger.info(f"  ✓ {loinc_code}: {title[:50]}...")
                
                time.sleep(0.7)  # Rate limit: 100 req/min
                
            except Exception as e:
                logger.warning(f"  ✗ {loinc_code}: {e}")
        
        self.conn.commit()
        logger.info(f"Fetched {fetched} MedlinePlus descriptions")
        return fetched
    
    def run_all(self):
        """Run all enrichment steps."""
        logger.info("=" * 60)
        logger.info("CLINICAL ROSETTA STONE - DATA ENRICHMENT")
        logger.info("=" * 60)
        
        results = {
            'curated_mappings': self.ingest_curated_mappings(),
            'critical_values': self.ingest_critical_values(),
            'loinc2hpo': self.ingest_loinc2hpo(),
            'medlineplus': self.fetch_medlineplus_descriptions(limit=30),
        }
        
        # Print summary
        print("\n" + "=" * 50)
        print("ENRICHMENT SUMMARY")
        print("=" * 50)
        for name, count in results.items():
            print(f"  {name}: {count}")
        
        return results


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    enricher = RosettaEnrichment()
    try:
        enricher.run_all()
    finally:
        enricher.close()


if __name__ == "__main__":
    main()
