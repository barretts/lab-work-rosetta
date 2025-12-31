#!/usr/bin/env python3
"""
Clinical Rosetta Stone - LOINC Ingestion
Ingests LOINC Table Core and accessory files from official LOINC download.
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Project root is two levels up from scripts/setup/
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "clinical_rosetta.db"
LOINC_PATH = PROJECT_ROOT / "downloads" / "Loinc_2.81"

# Lab-related LOINC classes to import
LAB_CLASSES = ["CHEM", "HEM/BC", "SERO", "UA", "COAG", "DRUG/TOX", "MICRO", "BLDBK", "CELLMARK"]


def ingest_loinc_core(conn: sqlite3.Connection, loinc_path: Path = LOINC_PATH):
    """Ingest LoincTableCore.csv - main LOINC table."""
    core_file = loinc_path / "LoincTableCore" / "LoincTableCore.csv"

    if not core_file.exists():
        logger.error(f"LOINC core file not found: {core_file}")
        return 0

    logger.info(f"Loading {core_file}...")
    df = pd.read_csv(core_file, low_memory=False)
    logger.info(f"  Loaded {len(df):,} LOINC codes")

    # Filter to active lab-related codes
    lab_df = df[
        (df["STATUS"] == "ACTIVE")
        & (
            df["CLASS"].isin(LAB_CLASSES)
            | df["CLASS"].str.contains("|".join(LAB_CLASSES), na=False)
        )
    ]
    logger.info(f"  Filtered to {len(lab_df):,} active lab-related codes")

    inserted = 0
    for _, row in lab_df.iterrows():
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO loinc_concept 
                (loinc_code, long_common_name, short_name, component, property, 
                 time_aspect, system, scale_type, method_type, class, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    row["LOINC_NUM"],
                    row["LONG_COMMON_NAME"],
                    row.get("SHORTNAME"),
                    row.get("COMPONENT"),
                    row.get("PROPERTY"),
                    row.get("TIME_ASPCT"),
                    row.get("SYSTEM"),
                    row.get("SCALE_TYP"),
                    row.get("METHOD_TYP"),
                    row.get("CLASS"),
                    row.get("STATUS"),
                ),
            )
            inserted += 1
        except Exception as e:
            logger.debug(f"Error inserting {row['LOINC_NUM']}: {e}")

    conn.commit()
    logger.info(f"  Inserted {inserted:,} LOINC codes")
    return inserted


def ingest_consumer_names(conn: sqlite3.Connection, loinc_path: Path = LOINC_PATH):
    """Ingest ConsumerName.csv as synonyms."""
    consumer_file = loinc_path / "AccessoryFiles" / "ConsumerName" / "ConsumerName.csv"

    if not consumer_file.exists():
        logger.warning(f"Consumer names file not found: {consumer_file}")
        return 0

    logger.info(f"Loading {consumer_file}...")
    df = pd.read_csv(consumer_file)
    logger.info(f"  Loaded {len(df):,} consumer names")

    count = 0
    for _, row in df.iterrows():
        loinc = row["LoincNumber"]
        name = row["ConsumerName"]
        if pd.notna(name) and name:
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO concept_synonym 
                    (loinc_code, source, synonym_type, synonym_text)
                    VALUES (?, ?, ?, ?)
                """,
                    (loinc, "LOINC_Consumer", "consumer", str(name)[:500]),
                )
                count += 1
            except Exception:
                pass

    conn.commit()
    logger.info(f"  Added {count:,} consumer name synonyms")
    return count


def ingest_shortnames(conn: sqlite3.Connection, loinc_path: Path = LOINC_PATH):
    """Ingest SHORTNAME as synonyms and LIS mappings."""
    core_file = loinc_path / "LoincTableCore" / "LoincTableCore.csv"

    if not core_file.exists():
        return 0

    logger.info("Adding shortnames as synonyms and mappings...")
    df = pd.read_csv(core_file, low_memory=False)

    # Create source system
    conn.execute(
        """
        INSERT OR IGNORE INTO source_system (system_name, system_type, vendor) 
        VALUES (?, ?, ?)
    """,
        ("LOINC_ShortName", "Standard", "Regenstrief"),
    )

    cursor = conn.execute(
        "SELECT system_id FROM source_system WHERE system_name = ?", ("LOINC_ShortName",)
    )
    sys_id = cursor.fetchone()[0]

    syn_count = 0
    map_count = 0

    for _, row in df.iterrows():
        shortname = row.get("SHORTNAME")
        loinc = row["LOINC_NUM"]
        longname = row.get("LONG_COMMON_NAME")

        if pd.notna(shortname) and shortname:
            # Add as synonym
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO concept_synonym 
                    (loinc_code, source, synonym_type, synonym_text)
                    VALUES (?, ?, ?, ?)
                """,
                    (loinc, "LOINC_ShortName", "short", str(shortname)[:500]),
                )
                syn_count += 1
            except Exception:
                pass

            # Add as LIS mapping
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO lis_mapping 
                    (source_system_id, source_code, source_description, target_loinc, mapping_source)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        sys_id,
                        str(shortname),
                        str(longname)[:200] if pd.notna(longname) else None,
                        loinc,
                        "loinc_shortname",
                    ),
                )
                map_count += 1
            except Exception:
                pass

    conn.commit()
    logger.info(f"  Added {syn_count:,} shortname synonyms")
    logger.info(f"  Added {map_count:,} shortname mappings")
    return syn_count + map_count


def run_all(db_path: str = None, loinc_path: str = None):
    """Run all LOINC ingestion steps."""
    db = Path(db_path) if db_path else DB_PATH
    loinc = Path(loinc_path) if loinc_path else LOINC_PATH

    if not loinc.exists():
        logger.error(f"LOINC directory not found: {loinc}")
        logger.info("Download LOINC from https://loinc.org/downloads/")
        return

    logger.info("=" * 60)
    logger.info("LOINC INGESTION")
    logger.info("=" * 60)

    conn = sqlite3.connect(str(db))

    try:
        results = {
            "loinc_codes": ingest_loinc_core(conn, loinc),
            "consumer_names": ingest_consumer_names(conn, loinc),
            "shortnames": ingest_shortnames(conn, loinc),
        }

        print("\n" + "=" * 50)
        print("LOINC INGESTION COMPLETE")
        print("=" * 50)
        for name, count in results.items():
            print(f"  {name}: {count:,}")

    finally:
        conn.close()


if __name__ == "__main__":
    run_all()
