"""Lab test name resolver - translates shorthand names to LOINC codes."""

import re
from difflib import SequenceMatcher

from rosetta.core.database import dict_from_row, get_db_connection


class LabTestResolver:
    """Resolves lab test shorthand names to standardized LOINC codes."""

    ABBREVIATION_MAP = {
        "hgb": "hemoglobin",
        "hb": "hemoglobin",
        "wbc": "white blood cell",
        "rbc": "red blood cell",
        "plt": "platelet",
        "plts": "platelet",
        "hct": "hematocrit",
        "mcv": "mean corpuscular volume",
        "mch": "mean corpuscular hemoglobin",
        "mchc": "mean corpuscular hemoglobin concentration",
        "rdw": "red cell distribution width",
        "mpv": "mean platelet volume",
        "ast": "aspartate aminotransferase",
        "alt": "alanine aminotransferase",
        "alp": "alkaline phosphatase",
        "alk phos": "alkaline phosphatase",
        "ggt": "gamma glutamyl transferase",
        "ldh": "lactate dehydrogenase",
        "bun": "blood urea nitrogen",
        "cr": "creatinine",
        "creat": "creatinine",
        "egfr": "glomerular filtration rate",
        "gfr": "glomerular filtration rate",
        "tsh": "thyroid stimulating hormone",
        "t3": "triiodothyronine",
        "t4": "thyroxine",
        "ft3": "free triiodothyronine",
        "ft4": "free thyroxine",
        "hba1c": "hemoglobin a1c",
        "a1c": "hemoglobin a1c",
        "psa": "prostate specific antigen",
        "bnp": "b-type natriuretic peptide",
        "crp": "c-reactive protein",
        "esr": "erythrocyte sedimentation rate",
        "pt": "prothrombin time",
        "inr": "international normalized ratio",
        "ptt": "partial thromboplastin time",
        "aptt": "activated partial thromboplastin time",
        "na": "sodium",
        "k": "potassium",
        "cl": "chloride",
        "co2": "bicarbonate",
        "hco3": "bicarbonate",
        "ca": "calcium",
        "mg": "magnesium",
        "phos": "phosphorus",
        "phosphate": "phosphorus",
        "ua": "urinalysis",
        "trig": "triglycerides",
        "chol": "cholesterol",
        "hdl": "high density lipoprotein cholesterol",
        "ldl": "low density lipoprotein cholesterol",
        "vldl": "very low density lipoprotein cholesterol",
        "bili": "bilirubin",
        "tbili": "total bilirubin",
        "dbili": "direct bilirubin",
        "alb": "albumin",
        "glob": "globulin",
        "tp": "total protein",
        "tibc": "total iron binding capacity",
        "tsat": "iron saturation",
        "ferr": "ferritin",
        "b12": "vitamin b12",
        "folate": "folic acid",
        "vitd": "vitamin d",
        "25ohd": "25-hydroxyvitamin d",
    }

    def __init__(self, db_path: str | None = None):
        self.conn = get_db_connection(db_path)

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def normalize(self, text: str) -> str:
        """Normalize text for matching."""
        text = text.lower().strip()
        # Expand abbreviations
        for abbrev, expansion in self.ABBREVIATION_MAP.items():
            pattern = r"\b" + re.escape(abbrev) + r"\b"
            text = re.sub(pattern, expansion, text)
        # Remove common suffixes
        text = re.sub(r"\s*(level|count|test|panel|auto|absolute)\s*$", "", text)
        return text

    def resolve(self, test_name: str, min_confidence: float = 0.5) -> dict | None:
        """
        Resolve a lab test name to a LOINC code.

        Returns dict with: loinc_code, loinc_name, confidence, method
        """
        if not test_name or len(test_name.strip()) < 2:
            return None

        normalized = self.normalize(test_name)

        # Try exact match on synonyms first
        result = self._exact_synonym_match(test_name)
        if result:
            return result

        # Try exact match on normalized name
        result = self._exact_name_match(normalized)
        if result:
            return result

        # Try fuzzy match
        result = self._fuzzy_match(normalized, min_confidence)
        if result:
            return result

        return None

    def _exact_synonym_match(self, test_name: str) -> dict | None:
        """Check for exact match in concept_synonym table."""
        cursor = self.conn.execute(
            """
            SELECT lc.loinc_code, lc.long_common_name
            FROM concept_synonym cs
            JOIN loinc_concept lc ON cs.loinc_code = lc.loinc_code
            WHERE LOWER(cs.synonym_text) = LOWER(?)
            LIMIT 1
            """,
            (test_name,),
        )
        row = cursor.fetchone()
        if row:
            return {
                "loinc_code": row["loinc_code"],
                "loinc_name": row["long_common_name"],
                "confidence": 1.0,
                "method": "exact_synonym",
            }
        return None

    def _exact_name_match(self, normalized: str) -> dict | None:
        """Check for exact match on LOINC long_common_name."""
        cursor = self.conn.execute(
            """
            SELECT loinc_code, long_common_name
            FROM loinc_concept
            WHERE LOWER(long_common_name) LIKE ?
            AND status = 'ACTIVE'
            ORDER BY LENGTH(long_common_name)
            LIMIT 1
            """,
            (f"%{normalized}%",),
        )
        row = cursor.fetchone()
        if row:
            return {
                "loinc_code": row["loinc_code"],
                "loinc_name": row["long_common_name"],
                "confidence": 0.9,
                "method": "exact_name",
            }
        return None

    def _fuzzy_match(self, normalized: str, min_confidence: float) -> dict | None:
        """Fuzzy match against LOINC names and synonyms."""
        # Get candidate names
        cursor = self.conn.execute(
            """
            SELECT loinc_code, long_common_name
            FROM loinc_concept
            WHERE status = 'ACTIVE'
            AND (
                long_common_name LIKE ?
                OR short_name LIKE ?
            )
            LIMIT 100
            """,
            (f"%{normalized[:4]}%", f"%{normalized[:4]}%"),
        )

        best_match = None
        best_score = 0

        for row in cursor.fetchall():
            name_lower = row["long_common_name"].lower()
            score = SequenceMatcher(None, normalized, name_lower).ratio()

            if score > best_score and score >= min_confidence:
                best_score = score
                best_match = {
                    "loinc_code": row["loinc_code"],
                    "loinc_name": row["long_common_name"],
                    "confidence": round(score, 2),
                    "method": "fuzzy",
                }

        return best_match

    def get_loinc_details(self, loinc_code: str) -> dict | None:
        """Get full details for a LOINC code."""
        cursor = self.conn.execute(
            """
            SELECT
                lc.loinc_code,
                lc.long_common_name,
                lc.short_name,
                lc.class as loinc_class,
                lc.component,
                lc.property,
                lc.time_aspect,
                lc.system,
                lc.scale_type,
                lc.method_type,
                lc.status
            FROM loinc_concept lc
            WHERE lc.loinc_code = ?
            """,
            (loinc_code,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        result = dict_from_row(row)

        # Add description
        desc_cursor = self.conn.execute(
            """
            SELECT description_text, source
            FROM concept_description
            WHERE loinc_code = ? AND description_type = 'consumer'
            LIMIT 1
            """,
            (loinc_code,),
        )
        desc_row = desc_cursor.fetchone()
        if desc_row:
            result["description"] = desc_row["description_text"]
            result["description_source"] = desc_row["source"]

        # Add reference ranges from reference_distribution
        try:
            range_cursor = self.conn.execute(
                """
                SELECT sex, age_min_years as age_low, age_max_years as age_high,
                       ref_low as reference_low, ref_high as reference_high, unit_of_measure as unit
                FROM reference_distribution
                WHERE loinc_code = ?
                """,
                (loinc_code,),
            )
            result["reference_ranges"] = [dict_from_row(r) for r in range_cursor.fetchall()]
        except Exception:
            result["reference_ranges"] = []

        # Add critical values from severity_rule
        try:
            crit_cursor = self.conn.execute(
                """
                SELECT threshold_value, direction, unit_of_measure as unit,
                       clinical_description as notes
                FROM severity_rule
                WHERE loinc_code = ? AND severity_level = 'Critical'
                """,
                (loinc_code,),
            )
            crit_rows = crit_cursor.fetchall()
            if crit_rows:
                result["critical_values"] = [dict_from_row(r) for r in crit_rows]
        except Exception:
            pass

        return result

    def search(self, query: str, limit: int = 20) -> list:
        """Search for lab tests by name."""
        cursor = self.conn.execute(
            """
            SELECT DISTINCT loinc_code, long_common_name, short_name
            FROM loinc_concept
            WHERE status = 'ACTIVE'
            AND (
                long_common_name LIKE ?
                OR short_name LIKE ?
                OR component LIKE ?
            )
            ORDER BY
                CASE WHEN long_common_name LIKE ? THEN 0 ELSE 1 END,
                LENGTH(long_common_name)
            LIMIT ?
            """,
            (f"%{query}%", f"%{query}%", f"%{query}%", f"{query}%", limit),
        )
        return [dict_from_row(row) for row in cursor.fetchall()]

    def get_reference_range(
        self, loinc_code: str, age: int | None = None, sex: str | None = None
    ) -> dict | None:
        """Get reference range for a LOINC code, optionally filtered by age/sex."""
        try:
            query = """
                SELECT sex, age_min_years as age_low, age_max_years as age_high,
                       ref_low as reference_low, ref_high as reference_high,
                       unit_of_measure as unit
                FROM reference_distribution
                WHERE loinc_code = ?
            """
            params = [loinc_code]

            if sex:
                query += " AND (sex = ? OR sex = 'A')"
                params.append(sex.upper()[0] if sex else "A")

            if age is not None:
                query += (
                    " AND (age_min_years IS NULL OR age_min_years <= ?)"
                    + " AND (age_max_years IS NULL OR age_max_years >= ?)"
                )
                params.extend([age, age])

            query += " LIMIT 1"

            cursor = self.conn.execute(query, params)
            row = cursor.fetchone()
            return dict_from_row(row) if row else None
        except Exception:
            return None

    def get_critical_values(self, loinc_code: str | None = None) -> list:
        """Get critical values, optionally for a specific LOINC code."""
        try:
            if loinc_code:
                cursor = self.conn.execute(
                    """
                    SELECT sr.loinc_code, sr.threshold_value, sr.direction,
                           sr.unit_of_measure as unit, sr.clinical_description as notes,
                           lc.long_common_name
                    FROM severity_rule sr
                    JOIN loinc_concept lc ON sr.loinc_code = lc.loinc_code
                    WHERE sr.loinc_code = ? AND sr.severity_level = 'Critical'
                    """,
                    (loinc_code,),
                )
            else:
                cursor = self.conn.execute(
                    """
                    SELECT sr.loinc_code, sr.threshold_value, sr.direction,
                           sr.unit_of_measure as unit, sr.clinical_description as notes,
                           lc.long_common_name
                    FROM severity_rule sr
                    JOIN loinc_concept lc ON sr.loinc_code = lc.loinc_code
                    WHERE sr.severity_level = 'Critical'
                    ORDER BY lc.long_common_name
                    """
                )
            return [dict_from_row(row) for row in cursor.fetchall()]
        except Exception:
            return []

    def get_drug_interactions(self, drug_name: str | None = None) -> list:
        """Get drug-lab interactions."""
        if drug_name:
            cursor = self.conn.execute(
                """
                SELECT drug_name, keyword, interaction_text
                FROM drug_lab_interaction
                WHERE LOWER(drug_name) LIKE LOWER(?)
                ORDER BY keyword
                """,
                (f"%{drug_name}%",),
            )
        else:
            cursor = self.conn.execute(
                """
                SELECT DISTINCT drug_name, COUNT(*) as interaction_count
                FROM drug_lab_interaction
                GROUP BY drug_name
                ORDER BY drug_name
                """
            )
        return [dict_from_row(row) for row in cursor.fetchall()]

    def get_stats(self) -> dict:
        """Get database statistics."""
        stats = {}

        tables = [
            ("loinc_concept", "LOINC codes"),
            ("concept_synonym", "Synonyms"),
            ("concept_description", "Descriptions"),
            ("reference_distribution", "Reference ranges"),
            ("severity_rule", "Critical values"),
            ("drug_lab_interaction", "Drug interactions"),
        ]

        for table, label in tables:
            try:
                cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table}")
                stats[label] = cursor.fetchone()[0]
            except Exception:
                stats[label] = 0

        return stats
