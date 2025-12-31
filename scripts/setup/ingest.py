#!/usr/bin/env python3
"""
Clinical Rosetta Stone - Data Ingestion Pipeline
Processes downloaded data files into the unified database.
"""

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Project root is two levels up from scripts/setup/
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "raw_data"
DB_PATH = PROJECT_ROOT / "clinical_rosetta.db"

# Import schema from same directory
import sys

sys.path.insert(0, str(Path(__file__).parent))
from schema import init_database, print_schema_summary

# ============================================================================
# NHANES VARIABLE MAPPINGS
# ============================================================================

NHANES_BIOCHEM_MAP = {
    "LBXSATSI": {"loinc": "1742-6", "name": "Alanine aminotransferase (ALT)", "unit": "U/L"},
    "LBXSASSI": {"loinc": "1920-8", "name": "Aspartate aminotransferase (AST)", "unit": "U/L"},
    "LBXSGL": {"loinc": "2345-7", "name": "Glucose, serum", "unit": "mg/dL"},
    "LBXSGLU": {"loinc": "2345-7", "name": "Glucose, serum", "unit": "mg/dL"},
    "LBXSAL": {"loinc": "1751-7", "name": "Albumin, serum", "unit": "g/dL"},
    "LBXSCR": {"loinc": "2160-0", "name": "Creatinine, serum", "unit": "mg/dL"},
    "LBXSBU": {"loinc": "3094-0", "name": "Blood urea nitrogen", "unit": "mg/dL"},
    "LBXSTP": {"loinc": "2885-2", "name": "Total protein, serum", "unit": "g/dL"},
    "LBXSTB": {"loinc": "1975-2", "name": "Total bilirubin", "unit": "mg/dL"},
    "LBXSAPSI": {"loinc": "6768-6", "name": "Alkaline phosphatase", "unit": "U/L"},
    "LBXSLDSI": {"loinc": "2532-0", "name": "Lactate dehydrogenase (LDH)", "unit": "U/L"},
    "LBXSUA": {"loinc": "3084-1", "name": "Uric acid", "unit": "mg/dL"},
    "LBXSNASI": {"loinc": "2951-2", "name": "Sodium, serum", "unit": "mmol/L"},
    "LBXSKSI": {"loinc": "2823-3", "name": "Potassium, serum", "unit": "mmol/L"},
    "LBXSCLSI": {"loinc": "2075-0", "name": "Chloride, serum", "unit": "mmol/L"},
    "LBXSC3SI": {"loinc": "2028-9", "name": "Bicarbonate (CO2)", "unit": "mmol/L"},
    "LBXSPH": {"loinc": "2777-1", "name": "Phosphorus, serum", "unit": "mg/dL"},
    "LBXSCA": {"loinc": "17861-6", "name": "Calcium, serum", "unit": "mg/dL"},
    "LBXSCH": {"loinc": "2093-3", "name": "Total cholesterol", "unit": "mg/dL"},
    "LBXSGTSI": {"loinc": "2324-2", "name": "Gamma glutamyl transferase (GGT)", "unit": "U/L"},
    "LBXSGB": {"loinc": "14629-0", "name": "Globulin", "unit": "g/dL"},
    "LBXSIR": {"loinc": "2498-4", "name": "Iron, serum", "unit": "ug/dL"},
    "LBXSOSSI": {"loinc": "2951-2", "name": "Osmolality", "unit": "mmol/kg"},
}

NHANES_CBC_MAP = {
    "LBXWBCSI": {"loinc": "6690-2", "name": "White blood cell count", "unit": "10^3/uL"},
    "LBXRBCSI": {"loinc": "789-8", "name": "Red blood cell count", "unit": "10^6/uL"},
    "LBXHGB": {"loinc": "718-7", "name": "Hemoglobin", "unit": "g/dL"},
    "LBXHCT": {"loinc": "4544-3", "name": "Hematocrit", "unit": "%"},
    "LBXMCVSI": {"loinc": "787-2", "name": "Mean cell volume (MCV)", "unit": "fL"},
    "LBXMCHSI": {"loinc": "785-6", "name": "Mean cell hemoglobin (MCH)", "unit": "pg"},
    "LBXMC": {
        "loinc": "786-4",
        "name": "Mean cell hemoglobin concentration (MCHC)",
        "unit": "g/dL",
    },
    "LBXRDW": {"loinc": "788-0", "name": "Red cell distribution width (RDW)", "unit": "%"},
    "LBXPLTSI": {"loinc": "777-3", "name": "Platelet count", "unit": "10^3/uL"},
    "LBXMPSI": {"loinc": "32623-1", "name": "Mean platelet volume (MPV)", "unit": "fL"},
    "LBXNEPCT": {"loinc": "770-8", "name": "Neutrophils %", "unit": "%"},
    "LBXLYPCT": {"loinc": "736-9", "name": "Lymphocytes %", "unit": "%"},
    "LBXMOPCT": {"loinc": "5905-5", "name": "Monocytes %", "unit": "%"},
    "LBXEOPCT": {"loinc": "713-8", "name": "Eosinophils %", "unit": "%"},
    "LBXBAPCT": {"loinc": "706-2", "name": "Basophils %", "unit": "%"},
    "LBDNENO": {"loinc": "751-8", "name": "Neutrophils count", "unit": "10^3/uL"},
    "LBDLYMNO": {"loinc": "731-0", "name": "Lymphocytes count", "unit": "10^3/uL"},
    "LBDMONO": {"loinc": "742-7", "name": "Monocytes count", "unit": "10^3/uL"},
    "LBDEONO": {"loinc": "711-2", "name": "Eosinophils count", "unit": "10^3/uL"},
    "LBDBANO": {"loinc": "704-7", "name": "Basophils count", "unit": "10^3/uL"},
}

NHANES_OTHER_MAP = {
    # Lipids
    "LBXTR": {"loinc": "2571-8", "name": "Triglycerides", "unit": "mg/dL"},
    "LBDTRSI": {"loinc": "2571-8", "name": "Triglycerides (SI)", "unit": "mmol/L"},
    "LBDLDL": {"loinc": "13457-7", "name": "LDL Cholesterol (calc)", "unit": "mg/dL"},
    # Glycohemoglobin
    "LBXGH": {"loinc": "4548-4", "name": "Hemoglobin A1c", "unit": "%"},
    # Iron
    "LBXIRN": {"loinc": "2498-4", "name": "Iron, serum", "unit": "ug/dL"},
    "LBXTIB": {"loinc": "2500-7", "name": "TIBC", "unit": "ug/dL"},
    "LBXFER": {"loinc": "2276-4", "name": "Ferritin", "unit": "ng/mL"},
    # Kidney
    "LBXUMA": {"loinc": "14957-5", "name": "Albumin, urine", "unit": "mg/L"},
    "LBXUCR": {"loinc": "2161-8", "name": "Creatinine, urine", "unit": "mg/dL"},
    "LBXCOT": {"loinc": "14685-2", "name": "Cotinine", "unit": "ng/mL"},
}

# Combine all maps
ALL_NHANES_MAPS = {**NHANES_BIOCHEM_MAP, **NHANES_CBC_MAP, **NHANES_OTHER_MAP}


# ============================================================================
# INGESTOR CLASS
# ============================================================================


class RosettaIngestor:
    """Ingests downloaded data files into the Clinical Rosetta database."""

    def __init__(self, db_path: str = DB_PATH, data_dir: Path = DATA_DIR):
        self.db_path = db_path
        self.data_dir = data_dir
        self.conn = init_database(db_path)

    def close(self):
        if self.conn:
            self.conn.close()

    # -------------------------------------------------------------------------
    # NHANES Ingestion
    # -------------------------------------------------------------------------

    def ingest_nhanes(self):
        """Process all NHANES XPT files and calculate reference distributions."""
        logger.info("Starting NHANES ingestion...")

        # Load demographics
        demo_path = self.data_dir / "NHANES_P_DEMO.XPT"
        if not demo_path.exists():
            logger.error(f"Demographics file not found: {demo_path}")
            return

        demo_df = pd.read_sas(demo_path)
        logger.info(f"Loaded demographics: {len(demo_df)} participants")

        # Create population record
        pop_id = self._create_population(
            "NHANES_2017_2020_PrePandemic",
            "NHANES 2017-March 2020 Pre-pandemic data",
            len(demo_df),
            "2017-01-01",
            "2020-03-01",
            "United States",
            "https://wwwn.cdc.gov/nchs/nhanes/",
        )

        # Process each lab file
        lab_files = [
            ("NHANES_P_BIOPRO.XPT", {**NHANES_BIOCHEM_MAP}),
            ("NHANES_P_CBC.XPT", NHANES_CBC_MAP),
            ("NHANES_P_TRIGLY.XPT", {"LBXTR": NHANES_OTHER_MAP["LBXTR"]}),
            ("NHANES_P_GHB.XPT", {"LBXGH": NHANES_OTHER_MAP["LBXGH"]}),
            (
                "NHANES_P_FETIB.XPT",
                {
                    "LBXIRN": NHANES_OTHER_MAP.get("LBXIRN", NHANES_OTHER_MAP.get("LBXFER")),
                    "LBXTIB": NHANES_OTHER_MAP.get("LBXTIB"),
                    "LBXFER": NHANES_OTHER_MAP.get("LBXFER"),
                },
            ),
            (
                "NHANES_P_ALB_CR.XPT",
                {
                    "LBXUMA": NHANES_OTHER_MAP.get("LBXUMA"),
                    "LBXUCR": NHANES_OTHER_MAP.get("LBXUCR"),
                },
            ),
        ]

        for filename, var_map in lab_files:
            filepath = self.data_dir / filename
            if filepath.exists():
                self._process_nhanes_file(filepath, demo_df, var_map, pop_id)
            else:
                logger.warning(f"Lab file not found: {filename}")

        self.conn.commit()
        logger.info("NHANES ingestion complete")

    def _create_population(self, name, desc, size, start, end, region, url) -> int:
        """Create or get population record."""
        cursor = self.conn.execute(
            "SELECT population_id FROM reference_population WHERE dataset_name = ?", (name,)
        )
        row = cursor.fetchone()
        if row:
            return row[0]

        self.conn.execute(
            """
            INSERT INTO reference_population 
            (dataset_name, description, sample_size, collection_start, collection_end, geographic_region, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (name, desc, size, start, end, region, url),
        )
        self.conn.commit()

        cursor = self.conn.execute("SELECT last_insert_rowid()")
        return cursor.fetchone()[0]

    def _process_nhanes_file(
        self, filepath: Path, demo_df: pd.DataFrame, var_map: dict, pop_id: int
    ):
        """Process a single NHANES XPT file."""
        logger.info(f"Processing {filepath.name}...")

        try:
            lab_df = pd.read_sas(filepath)
        except Exception as e:
            logger.error(f"Error reading {filepath}: {e}")
            return

        # Merge with demographics
        if "SEQN" not in lab_df.columns:
            logger.warning(f"No SEQN column in {filepath.name}")
            return

        merged = lab_df.merge(demo_df[["SEQN", "RIAGENDR", "RIDAGEYR"]], on="SEQN", how="inner")

        merged["sex"] = merged["RIAGENDR"].map({1.0: "M", 2.0: "F"})

        # Define age groups
        age_bins = [(0, 6), (6, 12), (12, 18), (18, 30), (30, 45), (45, 60), (60, 75), (75, 120)]

        # Process each variable
        for sas_var, meta in var_map.items():
            if meta is None or sas_var not in merged.columns:
                continue

            # Ensure LOINC concept exists
            self._upsert_loinc(meta["loinc"], meta["name"], meta.get("unit"))

            # Calculate stats by sex and age group
            for sex in ["M", "F", "A"]:  # A = All
                for age_min, age_max in age_bins:
                    if sex == "A":
                        subset = merged[
                            (merged["RIDAGEYR"] >= age_min) & (merged["RIDAGEYR"] < age_max)
                        ][sas_var].dropna()
                    else:
                        subset = merged[
                            (merged["sex"] == sex)
                            & (merged["RIDAGEYR"] >= age_min)
                            & (merged["RIDAGEYR"] < age_max)
                        ][sas_var].dropna()

                    if len(subset) < 30:  # Minimum sample size
                        continue

                    stats = {
                        "n": len(subset),
                        "mean": subset.mean(),
                        "std": subset.std(),
                        "p01": subset.quantile(0.01),
                        "p025": subset.quantile(0.025),
                        "p05": subset.quantile(0.05),
                        "p25": subset.quantile(0.25),
                        "p50": subset.quantile(0.50),
                        "p75": subset.quantile(0.75),
                        "p95": subset.quantile(0.95),
                        "p975": subset.quantile(0.975),
                        "p99": subset.quantile(0.99),
                    }

                    self._insert_distribution(
                        meta["loinc"],
                        pop_id,
                        sex if sex != "A" else None,
                        age_min,
                        age_max,
                        stats,
                        meta.get("unit"),
                    )

        logger.info(f"  Processed {filepath.name}")

    def _upsert_loinc(self, loinc_code: str, name: str, unit: str = None):
        """Insert or update LOINC concept."""
        try:
            self.conn.execute(
                """
                INSERT INTO loinc_concept (loinc_code, long_common_name, component)
                VALUES (?, ?, ?)
                ON CONFLICT(loinc_code) DO UPDATE SET
                    long_common_name = COALESCE(loinc_concept.long_common_name, excluded.long_common_name)
            """,
                (loinc_code, name, name.split(",")[0] if name else None),
            )
        except Exception as e:
            logger.error(f"Error upserting LOINC {loinc_code}: {e}")

    def _insert_distribution(
        self,
        loinc: str,
        pop_id: int,
        sex: str,
        age_min: float,
        age_max: float,
        stats: dict,
        unit: str,
    ):
        """Insert reference distribution."""
        try:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO reference_distribution 
                (loinc_code, population_id, sex, age_min_years, age_max_years,
                 n_samples, mean, std_dev, p01, p025, p05, p25, p50, p75, p95, p975, p99,
                 ref_low, ref_high, unit_of_measure)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    loinc,
                    pop_id,
                    sex,
                    age_min,
                    age_max,
                    stats["n"],
                    stats["mean"],
                    stats["std"],
                    stats["p01"],
                    stats["p025"],
                    stats["p05"],
                    stats["p25"],
                    stats["p50"],
                    stats["p75"],
                    stats["p95"],
                    stats["p975"],
                    stats["p99"],
                    stats["p025"],
                    stats["p975"],  # Use 2.5-97.5 percentiles as ref range
                    unit,
                ),
            )
        except Exception as e:
            logger.error(f"Error inserting distribution for {loinc}: {e}")

    # -------------------------------------------------------------------------
    # NCI Thesaurus Ingestion
    # -------------------------------------------------------------------------

    def ingest_nci_thesaurus(self):
        """Ingest NCI Thesaurus for definitions and synonyms."""
        logger.info("Starting NCI Thesaurus ingestion...")

        thesaurus_path = self.data_dir / "NCI_Thesaurus.FLAT" / "Thesaurus.txt"
        if not thesaurus_path.exists():
            logger.error(f"NCI Thesaurus not found: {thesaurus_path}")
            return

        # Read in chunks due to size (~200k concepts)
        chunk_size = 10000
        total = 0
        lab_related = 0

        # Semantic types related to lab tests
        lab_types = [
            "Laboratory Procedure",
            "Laboratory or Test Result",
            "Diagnostic Procedure",
            "Clinical Attribute",
            "Finding",
            "Sign or Symptom",
            "Pharmacologic Substance",
            "Enzyme",
            "Amino Acid, Peptide, or Protein",
            "Biologically Active Substance",
        ]

        columns = [
            "code",
            "concept_iri",
            "parents",
            "synonyms",
            "definition",
            "display_name",
            "concept_status",
            "semantic_type",
            "concept_in_subset",
        ]

        logger.info("Reading NCI Thesaurus (this may take a minute)...")

        for chunk in pd.read_csv(
            thesaurus_path,
            sep="\t",
            names=columns,
            dtype=str,
            chunksize=chunk_size,
            na_values=[""],
            keep_default_na=False,
            on_bad_lines="skip",
        ):
            for _, row in chunk.iterrows():
                total += 1

                # Filter to lab-related semantic types
                sem_type = str(row.get("semantic_type", ""))
                if not any(lt.lower() in sem_type.lower() for lt in lab_types):
                    continue

                lab_related += 1

                code = str(row["code"])
                preferred = (
                    str(row.get("synonyms", "")).split("|")[0] if row.get("synonyms") else ""
                )
                definition = str(row.get("definition", "")) if row.get("definition") else None

                try:
                    self.conn.execute(
                        """
                        INSERT OR REPLACE INTO nci_concept 
                        (nci_code, preferred_name, definition, semantic_type, parents, synonyms)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """,
                        (
                            code,
                            preferred,
                            definition,
                            sem_type,
                            str(row.get("parents", "")),
                            str(row.get("synonyms", "")),
                        ),
                    )
                except Exception as e:
                    logger.error(f"Error inserting NCI concept {code}: {e}")

            self.conn.commit()
            logger.info(f"  Processed {total:,} concepts ({lab_related:,} lab-related)")

        logger.info(f"NCI Thesaurus ingestion complete: {lab_related:,} lab-related concepts")

    # -------------------------------------------------------------------------
    # CTCAE Ingestion
    # -------------------------------------------------------------------------

    def ingest_ctcae(self):
        """Ingest CTCAE v5.0 Excel file."""
        logger.info("Starting CTCAE ingestion...")

        # Check both locations
        ctcae_paths = [
            self.data_dir / "CTCAE_v5.0.xlsx",
            Path("./downloads") / "CTCAE_v5.0.xlsx",
        ]

        ctcae_path = None
        for p in ctcae_paths:
            if p.exists():
                ctcae_path = p
                break

        if not ctcae_path:
            logger.error("CTCAE file not found")
            return

        # Create severity standard
        self.conn.execute(
            """
            INSERT OR IGNORE INTO severity_standard 
            (standard_name, version, effective_date, description, source_url)
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                "CTCAE",
                "5.0",
                "2017-11-27",
                "Common Terminology Criteria for Adverse Events v5.0",
                "https://ctep.cancer.gov/protocoldevelopment/electronic_applications/ctc.htm",
            ),
        )
        self.conn.commit()

        # Get standard ID
        cursor = self.conn.execute(
            "SELECT standard_id FROM severity_standard WHERE standard_name = 'CTCAE'"
        )
        standard_id = cursor.fetchone()[0]

        # Read Excel (requires openpyxl for .xlsx files)
        try:
            xlsx = pd.ExcelFile(ctcae_path, engine="openpyxl")
        except ImportError:
            logger.error("openpyxl not installed. Run: pip install openpyxl")
            return
        except Exception as e:
            logger.error(f"Error opening CTCAE Excel: {e}")
            return
        total_terms = 0

        for sheet in xlsx.sheet_names:
            if sheet.lower() in [
                "title page",
                "instructions",
                "appendix",
                "toc",
                "table of contents",
            ]:
                continue

            try:
                df = pd.read_excel(xlsx, sheet_name=sheet, header=None)

                # Find header row
                header_row = None
                for idx, row in df.iterrows():
                    row_str = " ".join(str(x) for x in row.values if pd.notna(x))
                    if "Grade 1" in row_str and "Grade 2" in row_str:
                        header_row = idx
                        break

                if header_row is None:
                    continue

                # Parse terms
                headers = df.iloc[header_row].tolist()
                term_col = 0
                grade_cols = {}

                for i, h in enumerate(headers):
                    h_str = str(h)
                    if "term" in h_str.lower() or "adverse event" in h_str.lower():
                        term_col = i
                    for g in [1, 2, 3, 4, 5]:
                        if f"Grade {g}" in h_str or f"grade {g}" in h_str.lower():
                            grade_cols[g] = i

                # Extract terms
                for idx in range(header_row + 1, len(df)):
                    row = df.iloc[idx]
                    term = str(row.iloc[term_col]) if pd.notna(row.iloc[term_col]) else ""

                    if not term or term == "nan" or len(term) < 2:
                        continue

                    for grade, col_idx in grade_cols.items():
                        if col_idx is None or col_idx >= len(row):
                            continue

                        definition = str(row.iloc[col_idx]) if pd.notna(row.iloc[col_idx]) else ""

                        if (
                            definition
                            and definition != "nan"
                            and definition != "-"
                            and len(definition) > 1
                        ):
                            try:
                                self.conn.execute(
                                    """
                                    INSERT OR REPLACE INTO ctcae_term 
                                    (soc, term_name, grade, definition)
                                    VALUES (?, ?, ?, ?)
                                """,
                                    (sheet, term.strip(), grade, definition.strip()),
                                )
                                total_terms += 1
                            except Exception as e:
                                logger.error(f"Error inserting CTCAE term: {e}")

            except Exception as e:
                logger.warning(f"Error processing sheet {sheet}: {e}")

        self.conn.commit()
        logger.info(f"CTCAE ingestion complete: {total_terms} term definitions")

        # Map lab-related CTCAE terms to LOINC
        self._map_ctcae_to_loinc(standard_id)

    def _map_ctcae_to_loinc(self, standard_id: int):
        """Map CTCAE lab terms to LOINC codes."""
        ctcae_loinc_map = {
            "Alanine aminotransferase increased": ("1742-6", "HIGH"),
            "Aspartate aminotransferase increased": ("1920-8", "HIGH"),
            "Alkaline phosphatase increased": ("6768-6", "HIGH"),
            "Blood bilirubin increased": ("1975-2", "HIGH"),
            "Creatinine increased": ("2160-0", "HIGH"),
            "GGT increased": ("2324-2", "HIGH"),
            "Lipase increased": ("3040-3", "HIGH"),
            "Amylase increased": ("1798-8", "HIGH"),
            "Glucose increased": ("2345-7", "HIGH"),
            "Anemia": ("718-7", "LOW"),
            "Hemoglobin decreased": ("718-7", "LOW"),
            "Lymphocyte count decreased": ("731-0", "LOW"),
            "Neutrophil count decreased": ("751-8", "LOW"),
            "Platelet count decreased": ("777-3", "LOW"),
            "White blood cell decreased": ("6690-2", "LOW"),
            "Hyperglycemia": ("2345-7", "HIGH"),
            "Hypoglycemia": ("2345-7", "LOW"),
            "Hyperkalemia": ("2823-3", "HIGH"),
            "Hypokalemia": ("2823-3", "LOW"),
            "Hypernatremia": ("2951-2", "HIGH"),
            "Hyponatremia": ("2951-2", "LOW"),
            "Hypercalcemia": ("17861-6", "HIGH"),
            "Hypocalcemia": ("17861-6", "LOW"),
            "Hyperuricemia": ("3084-1", "HIGH"),
        }

        mapped = 0
        cursor = self.conn.execute("SELECT term_name, grade, definition FROM ctcae_term")

        for row in cursor.fetchall():
            term_name, grade, definition = row

            # Check if this term maps to a LOINC
            for pattern, (loinc, direction) in ctcae_loinc_map.items():
                if pattern.lower() in term_name.lower():
                    # Update CTCAE term with LOINC
                    self.conn.execute(
                        "UPDATE ctcae_term SET loinc_code = ? WHERE term_name = ? AND grade = ?",
                        (loinc, term_name, grade),
                    )

                    # Create severity rule
                    self.conn.execute(
                        """
                        INSERT OR IGNORE INTO severity_rule 
                        (loinc_code, standard_id, direction, severity_level, severity_code, clinical_description)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """,
                        (loinc, standard_id, direction, f"Grade_{grade}", grade, definition),
                    )

                    mapped += 1
                    break

        self.conn.commit()
        logger.info(f"Mapped {mapped} CTCAE terms to LOINC codes")

    # -------------------------------------------------------------------------
    # LIS Seed Data
    # -------------------------------------------------------------------------

    def ingest_lis_seed_data(self):
        """Ingest the LIS seed data with common shorthands."""
        logger.info("Starting LIS seed data ingestion...")

        seed_path = Path("./downloads/lis_seed_data.csv")
        if not seed_path.exists():
            logger.warning(f"LIS seed data not found: {seed_path}")
            return

        # Create source system
        self.conn.execute(
            """
            INSERT OR IGNORE INTO source_system (system_name, system_type, vendor)
            VALUES (?, ?, ?)
        """,
            ("Generic_LIS", "LIS", "Various"),
        )
        self.conn.commit()

        cursor = self.conn.execute(
            "SELECT system_id FROM source_system WHERE system_name = 'Generic_LIS'"
        )
        system_id = cursor.fetchone()[0]

        df = pd.read_csv(seed_path)
        count = 0

        for _, row in df.iterrows():
            # Handle different possible column names
            shorthand = row.get("source_code") or row.get("shorthand")
            loinc = row.get("target_loinc") or row.get("loinc") or row.get("loinc_code")
            name = row.get("description_hint") or row.get("description") or row.get("name")

            if not shorthand or not loinc:
                continue

            # Ensure LOINC exists
            self._upsert_loinc(str(loinc), str(name) if name else None)

            # Add mapping
            try:
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO lis_mapping 
                    (source_system_id, source_code, source_description, target_loinc, mapping_source)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        system_id,
                        str(shorthand),
                        str(name) if name else None,
                        str(loinc),
                        "seed_data",
                    ),
                )
                count += 1
            except Exception as e:
                logger.error(f"Error inserting LIS mapping: {e}")

        self.conn.commit()
        logger.info(f"LIS seed data ingestion complete: {count} mappings")

    # -------------------------------------------------------------------------
    # Run All
    # -------------------------------------------------------------------------

    def run_all(self):
        """Run all ingestion steps."""
        logger.info("=" * 60)
        logger.info("CLINICAL ROSETTA STONE - DATA INGESTION")
        logger.info("=" * 60)

        steps = [
            ("LIS Seed Data", self.ingest_lis_seed_data),
            ("NHANES Reference Ranges", self.ingest_nhanes),
            ("NCI Thesaurus", self.ingest_nci_thesaurus),
            ("CTCAE Severity", self.ingest_ctcae),
        ]

        for name, func in steps:
            logger.info(f"\n--- {name} ---")
            try:
                func()
            except Exception as e:
                logger.error(f"Error in {name}: {e}")

        # Print summary
        print_schema_summary(self.conn)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    ingestor = RosettaIngestor()
    try:
        ingestor.run_all()
    finally:
        ingestor.close()


if __name__ == "__main__":
    main()
