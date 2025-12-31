"""Command-line interface for Clinical Rosetta Stone."""

import argparse
import json
import sys
from typing import Optional

from rosetta import __version__
from rosetta.core.resolver import LabTestResolver


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="rosetta",
        description="Clinical Rosetta Stone - Lab test translation CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  rosetta translate "ALT"              # Translate a test name
  rosetta lookup 718-7                 # Look up LOINC code details
  rosetta search hemoglobin            # Search for tests
  rosetta range 718-7 --age 45 --sex M # Get reference range
  rosetta stats                        # Show database statistics
        """,
    )

    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # translate command
    translate_parser = subparsers.add_parser("translate", aliases=["t"], help="Translate test name")
    translate_parser.add_argument("name", help="Test name to translate")
    translate_parser.add_argument(
        "-c", "--confidence", type=float, default=0.5, help="Minimum confidence (default: 0.5)"
    )
    translate_parser.add_argument("-j", "--json", action="store_true", help="Output as JSON")

    # lookup command
    lookup_parser = subparsers.add_parser("lookup", aliases=["l"], help="Look up LOINC code")
    lookup_parser.add_argument("code", help="LOINC code")
    lookup_parser.add_argument("-j", "--json", action="store_true", help="Output as JSON")

    # search command
    search_parser = subparsers.add_parser("search", aliases=["s"], help="Search for tests")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument("-n", "--limit", type=int, default=10, help="Max results")
    search_parser.add_argument("-j", "--json", action="store_true", help="Output as JSON")

    # range command
    range_parser = subparsers.add_parser("range", aliases=["r"], help="Get reference range")
    range_parser.add_argument("code", help="LOINC code")
    range_parser.add_argument("--age", type=int, help="Patient age")
    range_parser.add_argument("--sex", help="Patient sex (M/F)")
    range_parser.add_argument("-j", "--json", action="store_true", help="Output as JSON")

    # critical command
    critical_parser = subparsers.add_parser("critical", help="Get critical values")
    critical_parser.add_argument("--loinc", help="Filter by LOINC code")
    critical_parser.add_argument("-j", "--json", action="store_true", help="Output as JSON")

    # stats command
    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.add_argument("-j", "--json", action="store_true", help="Output as JSON")

    # batch command
    batch_parser = subparsers.add_parser("batch", help="Batch translate from comma-separated list")
    batch_parser.add_argument("names", help="Comma-separated test names")
    batch_parser.add_argument("-j", "--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    try:
        with LabTestResolver() as resolver:
            if args.command in ("translate", "t"):
                return cmd_translate(resolver, args)
            elif args.command in ("lookup", "l"):
                return cmd_lookup(resolver, args)
            elif args.command in ("search", "s"):
                return cmd_search(resolver, args)
            elif args.command in ("range", "r"):
                return cmd_range(resolver, args)
            elif args.command == "critical":
                return cmd_critical(resolver, args)
            elif args.command == "stats":
                return cmd_stats(resolver, args)
            elif args.command == "batch":
                return cmd_batch(resolver, args)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


def cmd_translate(resolver: LabTestResolver, args) -> int:
    """Handle translate command."""
    result = resolver.resolve(args.name, min_confidence=args.confidence)

    if args.json:
        print(json.dumps({"input": args.name, "result": result}, indent=2))
    elif result:
        print(f"Input:      {args.name}")
        print(f"LOINC:      {result['loinc_code']}")
        print(f"Name:       {result['loinc_name']}")
        print(f"Confidence: {result['confidence']:.0%}")
        print(f"Method:     {result['method']}")
    else:
        print(f"No match found for: {args.name}")
        return 1

    return 0


def cmd_lookup(resolver: LabTestResolver, args) -> int:
    """Handle lookup command."""
    details = resolver.get_loinc_details(args.code)

    if args.json:
        print(json.dumps(details, indent=2))
    elif details:
        print(f"LOINC Code:  {details['loinc_code']}")
        print(f"Name:        {details['long_common_name']}")
        print(f"Component:   {details.get('component', 'N/A')}")
        print(f"System:      {details.get('system', 'N/A')}")
        print(f"Units:       {details.get('example_units', 'N/A')}")
        print(f"Class:       {details.get('loinc_class', 'N/A')}")
        if details.get("description"):
            print(f"\nDescription: {details['description'][:200]}...")
        if details.get("reference_ranges"):
            print("\nReference Ranges:")
            for rr in details["reference_ranges"][:3]:
                sex = rr.get("sex", "Both")
                low = rr.get("reference_low", "?")
                high = rr.get("reference_high", "?")
                unit = rr.get("unit", "")
                print(f"  {sex}: {low} - {high} {unit}")
    else:
        print(f"LOINC code not found: {args.code}")
        return 1

    return 0


def cmd_search(resolver: LabTestResolver, args) -> int:
    """Handle search command."""
    results = resolver.search(args.query, limit=args.limit)

    if args.json:
        print(json.dumps(results, indent=2))
    elif results:
        print(f"Found {len(results)} results for '{args.query}':\n")
        for r in results:
            print(f"  {r['loinc_code']:12} {r['long_common_name'][:60]}")
    else:
        print(f"No results for: {args.query}")

    return 0


def cmd_range(resolver: LabTestResolver, args) -> int:
    """Handle range command."""
    result = resolver.get_reference_range(args.code, age=args.age, sex=args.sex)

    if args.json:
        print(json.dumps(result, indent=2))
    elif result:
        print(f"LOINC: {args.code}")
        print(f"Sex:   {result.get('sex', 'Both')}")
        print(f"Age:   {result.get('age_low', '?')} - {result.get('age_high', '?')}")
        print(f"Range: {result.get('reference_low', '?')} - {result.get('reference_high', '?')} {result.get('unit', '')}")
    else:
        print(f"No reference range found for: {args.code}")
        return 1

    return 0


def cmd_critical(resolver: LabTestResolver, args) -> int:
    """Handle critical command."""
    results = resolver.get_critical_values(args.loinc if hasattr(args, "loinc") else None)

    if args.json:
        print(json.dumps(results, indent=2))
    elif results:
        print(f"Critical Values ({len(results)} total):\n")
        for cv in results:
            name = cv.get("long_common_name", cv.get("loinc_code", "?"))[:40]
            low = cv.get("critical_low", "")
            high = cv.get("critical_high", "")
            unit = cv.get("unit", "")
            print(f"  {name:42} Low: {low or 'N/A':>8}  High: {high or 'N/A':>8} {unit}")
    else:
        print("No critical values found.")

    return 0


def cmd_stats(resolver: LabTestResolver, args) -> int:
    """Handle stats command."""
    stats = resolver.get_stats()

    if args.json:
        print(json.dumps(stats, indent=2))
    else:
        print("Clinical Rosetta Stone - Database Statistics\n")
        print("=" * 40)
        for label, count in stats.items():
            print(f"  {label:25} {count:>10,}")

    return 0


def cmd_batch(resolver: LabTestResolver, args) -> int:
    """Handle batch command."""
    names = [n.strip() for n in args.names.split(",")]
    results = []

    for name in names:
        result = resolver.resolve(name)
        results.append(
            {
                "input": name,
                "loinc_code": result["loinc_code"] if result else None,
                "loinc_name": result["loinc_name"] if result else None,
                "confidence": result["confidence"] if result else None,
            }
        )

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print(f"{'Input':30} {'LOINC':12} {'Name':40} {'Conf':>6}")
        print("-" * 90)
        for r in results:
            code = r["loinc_code"] or "NOT_FOUND"
            name = (r["loinc_name"] or "")[:40]
            conf = f"{r['confidence']:.0%}" if r["confidence"] else "N/A"
            print(f"{r['input']:30} {code:12} {name:40} {conf:>6}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
