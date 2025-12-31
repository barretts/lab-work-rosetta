#!/usr/bin/env python3
"""
Clinical Rosetta Stone - Command Line Interface
Query LIS shorthands, LOINC codes, reference ranges, and critical values.
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

DB_PATH = Path(__file__).parent / "clinical_rosetta.db"


class RosettaCLI:
    """CLI interface for the Clinical Rosetta Stone database."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DB_PATH)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        if self.conn:
            self.conn.close()

    def translate(self, shorthand: str, fuzzy: bool = True) -> List[Dict]:
        """Translate LIS shorthand to LOINC code(s)."""
        results = []

        # Exact match first
        cursor = self.conn.execute(
            """
            SELECT m.source_code, m.target_loinc, l.long_common_name, 
                   l.short_name, s.system_name, m.mapping_confidence
            FROM lis_mapping m
            JOIN loinc_concept l ON m.target_loinc = l.loinc_code
            JOIN source_system s ON m.source_system_id = s.system_id
            WHERE LOWER(m.source_code) = LOWER(?)
        """,
            (shorthand,),
        )

        for row in cursor.fetchall():
            results.append(
                {
                    "match_type": "exact",
                    "shorthand": row["source_code"],
                    "loinc_code": row["target_loinc"],
                    "name": row["long_common_name"],
                    "short_name": row["short_name"],
                    "source": row["system_name"],
                    "confidence": row["mapping_confidence"] or 1.0,
                }
            )

        # Fuzzy match if no exact match and fuzzy enabled
        if not results and fuzzy:
            cursor = self.conn.execute(
                """
                SELECT m.source_code, m.target_loinc, l.long_common_name,
                       l.short_name, s.system_name
                FROM lis_mapping m
                JOIN loinc_concept l ON m.target_loinc = l.loinc_code
                JOIN source_system s ON m.source_system_id = s.system_id
                WHERE LOWER(m.source_code) LIKE LOWER(?)
                   OR LOWER(m.source_description) LIKE LOWER(?)
                LIMIT 10
            """,
                (f"%{shorthand}%", f"%{shorthand}%"),
            )

            for row in cursor.fetchall():
                results.append(
                    {
                        "match_type": "fuzzy",
                        "shorthand": row["source_code"],
                        "loinc_code": row["target_loinc"],
                        "name": row["long_common_name"],
                        "short_name": row["short_name"],
                        "source": row["system_name"],
                        "confidence": 0.5,
                    }
                )

        return results

    def lookup(self, loinc_code: str) -> Optional[Dict]:
        """Get full profile for a LOINC code."""
        cursor = self.conn.execute(
            """
            SELECT * FROM loinc_concept WHERE loinc_code = ?
        """,
            (loinc_code,),
        )

        row = cursor.fetchone()
        if not row:
            return None

        result = dict(row)

        # Get reference ranges
        cursor = self.conn.execute(
            """
            SELECT sex, age_min_years, age_max_years, 
                   p025 as ref_low, p975 as ref_high, p50 as median,
                   unit_of_measure, n_samples
            FROM reference_distribution
            WHERE loinc_code = ?
            ORDER BY sex, age_min_years
        """,
            (loinc_code,),
        )
        result["reference_ranges"] = [dict(r) for r in cursor.fetchall()]

        # Get critical values
        cursor = self.conn.execute(
            """
            SELECT direction, threshold_value, threshold_operator,
                   unit_of_measure, severity_label, clinical_description
            FROM severity_rule
            WHERE loinc_code = ?
        """,
            (loinc_code,),
        )
        result["critical_values"] = [dict(r) for r in cursor.fetchall()]

        # Get consumer description
        cursor = self.conn.execute(
            """
            SELECT description_text, source, source_url
            FROM concept_description
            WHERE loinc_code = ? AND description_type = 'consumer'
        """,
            (loinc_code,),
        )
        desc = cursor.fetchone()
        if desc:
            result["consumer_description"] = dict(desc)

        # Get synonyms/aliases
        cursor = self.conn.execute(
            """
            SELECT source_code FROM lis_mapping WHERE target_loinc = ?
        """,
            (loinc_code,),
        )
        result["aliases"] = [r["source_code"] for r in cursor.fetchall()]

        return result

    def reference_range(self, loinc_code: str, age: int = None, sex: str = None) -> List[Dict]:
        """Get reference range for a LOINC code, optionally filtered by demographics."""
        query = """
            SELECT l.long_common_name, r.sex, r.age_min_years, r.age_max_years,
                   r.p025 as ref_low, r.p975 as ref_high, r.p50 as median,
                   r.mean, r.std_dev, r.unit_of_measure, r.n_samples,
                   p.dataset_name
            FROM reference_distribution r
            JOIN loinc_concept l ON r.loinc_code = l.loinc_code
            JOIN reference_population p ON r.population_id = p.population_id
            WHERE r.loinc_code = ?
        """
        params = [loinc_code]

        if age is not None:
            query += " AND r.age_min_years <= ? AND r.age_max_years > ?"
            params.extend([age, age])

        if sex:
            query += " AND (r.sex = ? OR r.sex IS NULL)"
            params.append(sex.upper())

        query += " ORDER BY r.sex, r.age_min_years"

        cursor = self.conn.execute(query, params)
        return [dict(r) for r in cursor.fetchall()]

    def critical_values(self, loinc_code: str = None) -> List[Dict]:
        """Get critical values, optionally for a specific LOINC code."""
        if loinc_code:
            cursor = self.conn.execute(
                """
                SELECT l.loinc_code, l.long_common_name,
                       sr.direction, sr.threshold_value, sr.threshold_operator,
                       sr.unit_of_measure, sr.severity_label
                FROM severity_rule sr
                JOIN loinc_concept l ON sr.loinc_code = l.loinc_code
                WHERE sr.loinc_code = ?
                ORDER BY sr.direction
            """,
                (loinc_code,),
            )
        else:
            cursor = self.conn.execute(
                """
                SELECT l.loinc_code, l.long_common_name,
                       sr.direction, sr.threshold_value, sr.threshold_operator,
                       sr.unit_of_measure, sr.severity_label
                FROM severity_rule sr
                JOIN loinc_concept l ON sr.loinc_code = l.loinc_code
                ORDER BY l.long_common_name, sr.direction
            """
            )

        return [dict(r) for r in cursor.fetchall()]

    def search(self, query: str, limit: int = 20) -> List[Dict]:
        """Search for LOINC codes by name or component."""
        cursor = self.conn.execute(
            """
            SELECT loinc_code, long_common_name, short_name, component, class
            FROM loinc_concept
            WHERE LOWER(long_common_name) LIKE LOWER(?)
               OR LOWER(component) LIKE LOWER(?)
               OR LOWER(short_name) LIKE LOWER(?)
            LIMIT ?
        """,
            (f"%{query}%", f"%{query}%", f"%{query}%", limit),
        )

        return [dict(r) for r in cursor.fetchall()]

    def batch_translate(self, shorthands: List[str]) -> Dict[str, Any]:
        """Translate multiple shorthands at once."""
        results = {}
        for shorthand in shorthands:
            matches = self.translate(shorthand, fuzzy=False)
            if matches:
                results[shorthand] = {
                    "found": True,
                    "loinc": matches[0]["loinc_code"],
                    "name": matches[0]["name"],
                }
            else:
                results[shorthand] = {"found": False}
        return results

    def stats(self) -> Dict[str, int]:
        """Get database statistics."""
        tables = [
            "loinc_concept",
            "lis_mapping",
            "vocabulary_mapping",
            "reference_distribution",
            "nci_concept",
            "concept_description",
            "severity_rule",
            "source_system",
        ]

        stats = {}
        for table in tables:
            cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = cursor.fetchone()[0]

        return stats


def format_table(rows: List[Dict], columns: List[str] = None) -> str:
    """Format results as a simple table."""
    if not rows:
        return "No results found."

    if columns is None:
        columns = list(rows[0].keys())

    # Calculate column widths
    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            val = str(row.get(col, ""))[:50]
            widths[col] = max(widths[col], len(val))

    # Build table
    lines = []
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    lines.append(header)
    lines.append("-" * len(header))

    for row in rows:
        line = " | ".join(
            str(row.get(col, "")).ljust(widths[col])[: widths[col]] for col in columns
        )
        lines.append(line)

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Clinical Rosetta Stone - LIS Code Translator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  rosetta translate "Alk Phos"
  rosetta translate "U THC Scr"
  rosetta lookup 718-7
  rosetta range 2345-7 --age 45 --sex M
  rosetta critical
  rosetta search hemoglobin
  rosetta batch "WBC,RBC,HGB,PLT"
  rosetta stats
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # translate command
    p_trans = subparsers.add_parser(
        "translate", aliases=["t"], help="Translate LIS shorthand to LOINC"
    )
    p_trans.add_argument("shorthand", help="LIS shorthand code to translate")
    p_trans.add_argument("--no-fuzzy", action="store_true", help="Disable fuzzy matching")
    p_trans.add_argument("--json", action="store_true", help="Output as JSON")

    # lookup command
    p_lookup = subparsers.add_parser("lookup", aliases=["l"], help="Lookup full LOINC profile")
    p_lookup.add_argument("loinc", help="LOINC code")
    p_lookup.add_argument("--json", action="store_true", help="Output as JSON")

    # range command
    p_range = subparsers.add_parser("range", aliases=["r"], help="Get reference ranges")
    p_range.add_argument("loinc", help="LOINC code")
    p_range.add_argument("--age", type=int, help="Patient age")
    p_range.add_argument("--sex", choices=["M", "F"], help="Patient sex")
    p_range.add_argument("--json", action="store_true", help="Output as JSON")

    # critical command
    p_crit = subparsers.add_parser("critical", aliases=["c"], help="Get critical values")
    p_crit.add_argument("--loinc", help="Optional: specific LOINC code")
    p_crit.add_argument("--json", action="store_true", help="Output as JSON")

    # search command
    p_search = subparsers.add_parser("search", aliases=["s"], help="Search for LOINC codes")
    p_search.add_argument("query", help="Search query")
    p_search.add_argument("--limit", type=int, default=20, help="Max results")
    p_search.add_argument("--json", action="store_true", help="Output as JSON")

    # batch command
    p_batch = subparsers.add_parser("batch", aliases=["b"], help="Batch translate multiple codes")
    p_batch.add_argument("codes", help="Comma-separated list of shorthands")
    p_batch.add_argument("--json", action="store_true", help="Output as JSON")

    # stats command
    p_stats = subparsers.add_parser("stats", help="Database statistics")
    p_stats.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    cli = RosettaCLI()

    try:
        if args.command in ("translate", "t"):
            results = cli.translate(args.shorthand, fuzzy=not args.no_fuzzy)
            if args.json:
                print(json.dumps(results, indent=2))
            elif results:
                print(f'\nTranslation for: "{args.shorthand}"')
                print("=" * 60)
                for r in results:
                    print(f"  LOINC: {r['loinc_code']}")
                    print(f"  Name:  {r['name']}")
                    print(f"  Match: {r['match_type']} (confidence: {r['confidence']})")
                    print(f"  From:  {r['source']}")
                    print()
            else:
                print(f'No mapping found for: "{args.shorthand}"')

        elif args.command in ("lookup", "l"):
            result = cli.lookup(args.loinc)
            if args.json:
                print(json.dumps(result, indent=2, default=str))
            elif result:
                print(f"\nLOINC Profile: {args.loinc}")
                print("=" * 60)
                print(f"  Name: {result.get('long_common_name')}")
                print(f"  Short: {result.get('short_name')}")
                print(f"  Component: {result.get('component')}")
                print(f"  Class: {result.get('class')}")

                if result.get("aliases"):
                    print(f"\n  Aliases: {', '.join(result['aliases'][:10])}")

                if result.get("consumer_description"):
                    desc = result["consumer_description"]["description_text"][:200]
                    print(f"\n  Description: {desc}...")

                if result.get("reference_ranges"):
                    print(f"\n  Reference Ranges ({len(result['reference_ranges'])} groups):")
                    for rr in result["reference_ranges"][:5]:
                        sex = rr.get("sex") or "All"
                        print(
                            f"    {sex} {int(rr['age_min_years'])}-{int(rr['age_max_years'])}y: "
                            f"{rr['ref_low']:.1f}-{rr['ref_high']:.1f} {rr['unit_of_measure']}"
                        )

                if result.get("critical_values"):
                    print(f"\n  Critical Values:")
                    for cv in result["critical_values"]:
                        print(
                            f"    {cv['severity_label']}: {cv['threshold_operator']} "
                            f"{cv['threshold_value']} {cv['unit_of_measure']}"
                        )
            else:
                print(f"LOINC code not found: {args.loinc}")

        elif args.command in ("range", "r"):
            results = cli.reference_range(args.loinc, args.age, args.sex)
            if args.json:
                print(json.dumps(results, indent=2, default=str))
            elif results:
                print(f"\nReference Ranges for LOINC {args.loinc}")
                if args.age:
                    print(f"  Filtered: age={args.age}")
                if args.sex:
                    print(f"  Filtered: sex={args.sex}")
                print("=" * 60)
                cols = [
                    "sex",
                    "age_min_years",
                    "age_max_years",
                    "ref_low",
                    "ref_high",
                    "unit_of_measure",
                    "n_samples",
                ]
                print(format_table(results, cols))
            else:
                print(f"No reference ranges found for: {args.loinc}")

        elif args.command in ("critical", "c"):
            results = cli.critical_values(args.loinc)
            if args.json:
                print(json.dumps(results, indent=2))
            elif results:
                print("\nCritical Values")
                print("=" * 60)
                cols = [
                    "loinc_code",
                    "long_common_name",
                    "direction",
                    "threshold_operator",
                    "threshold_value",
                    "unit_of_measure",
                ]
                print(format_table(results, cols))
            else:
                print("No critical values found.")

        elif args.command in ("search", "s"):
            results = cli.search(args.query, args.limit)
            if args.json:
                print(json.dumps(results, indent=2))
            elif results:
                print(f'\nSearch results for: "{args.query}"')
                print("=" * 60)
                cols = ["loinc_code", "long_common_name", "class"]
                print(format_table(results, cols))
            else:
                print(f'No results for: "{args.query}"')

        elif args.command in ("batch", "b"):
            codes = [c.strip() for c in args.codes.split(",")]
            results = cli.batch_translate(codes)
            if args.json:
                print(json.dumps(results, indent=2))
            else:
                print("\nBatch Translation Results")
                print("=" * 60)
                for code, result in results.items():
                    if result["found"]:
                        print(f"  ✓ \"{code}\" → {result['loinc']}: {result['name'][:40]}")
                    else:
                        print(f'  ✗ "{code}" - not found')

        elif args.command == "stats":
            stats = cli.stats()
            if args.json:
                print(json.dumps(stats, indent=2))
            else:
                print("\nDatabase Statistics")
                print("=" * 40)
                total = 0
                for table, count in stats.items():
                    print(f"  {table:25} {count:>8,}")
                    total += count
                print("-" * 40)
                print(f"  {'TOTAL':25} {total:>8,}")

    finally:
        cli.close()


if __name__ == "__main__":
    main()
