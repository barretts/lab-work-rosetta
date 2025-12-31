#!/usr/bin/env python3
"""
Clinical Rosetta Stone - Lab Test Translator
Translates LIS shorthands to LOINC codes with enriched clinical context.
"""

import sqlite3
import csv
import re
import argparse
from pathlib import Path
from html import unescape
from auto_resolver import AutoResolver

DB_PATH = Path(__file__).parent / "clinical_rosetta.db"


def strip_html(text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not text:
        return ''
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def get_enrichment(conn: sqlite3.Connection, loinc_code: str) -> dict:
    """Get enrichment data for a LOINC code."""
    data = {
        'reference_low': '',
        'reference_high': '',
        'unit': '',
        'critical_low': '',
        'critical_high': '',
        'description': '',
        'clinical_note': ''
    }
    
    # Consumer description
    cursor = conn.execute('''
        SELECT description_text FROM concept_description 
        WHERE loinc_code = ? AND description_type = 'consumer'
        LIMIT 1
    ''', (loinc_code,))
    row = cursor.fetchone()
    if row and row[0]:
        data['description'] = strip_html(row[0])[:300]
    
    # Professional/clinical note
    cursor = conn.execute('''
        SELECT description_text FROM concept_description 
        WHERE loinc_code = ? AND description_type = 'professional'
        LIMIT 1
    ''', (loinc_code,))
    row = cursor.fetchone()
    if row and row[0]:
        data['clinical_note'] = strip_html(row[0])[:200]
    
    # Reference range (adult)
    cursor = conn.execute('''
        SELECT p025, p975, unit_of_measure
        FROM reference_distribution 
        WHERE loinc_code = ? AND age_min_years <= 45 AND age_max_years > 45
        ORDER BY sex NULLS FIRST
        LIMIT 1
    ''', (loinc_code,))
    row = cursor.fetchone()
    if row and row[0] and row[1]:
        data['reference_low'] = f'{row[0]:.1f}'
        data['reference_high'] = f'{row[1]:.1f}'
        data['unit'] = row[2] or ''
    
    # Critical values
    cursor = conn.execute('''
        SELECT direction, threshold_value, unit_of_measure
        FROM severity_rule 
        WHERE loinc_code = ? AND severity_level = 'Critical'
    ''', (loinc_code,))
    for row in cursor.fetchall():
        if row[0] == 'LOW' and row[1]:
            data['critical_low'] = f'<{row[1]}'
        elif row[0] == 'HIGH' and row[1]:
            data['critical_high'] = f'>{row[1]}'
    
    return data


def translate_file(input_file: str, output_file: str = None, min_confidence: float = 0.5):
    """Translate a file of test names to LOINC codes with enrichment."""
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"Error: Input file not found: {input_file}")
        return
    
    # Default output filename
    if not output_file:
        output_file = input_path.stem + "_translated.csv"
    
    # Read input
    with open(input_path, 'r') as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
    
    # Filter obvious non-test-names (result values, metadata, etc.)
    SKIP_PATTERN = r'^(\d|NEG|POS|Clear|Normal|NONE|Trace|Date|Time|Page|CST|Auth|[\•\*\@\-]|\d+/|Light-Yellow|mEq|mg/dL|IntlUnit|mL/min|mOsm|x10|ratio$|Test Name|Test Result|Final|Report|.{1,2}$)'
    test_names = [l for l in lines if not re.search(SKIP_PATTERN, l, re.IGNORECASE)]
    
    print(f"Found {len(test_names)} potential test names in {input_file}")
    
    # Initialize resolver and database
    resolver = AutoResolver()
    conn = sqlite3.connect(str(DB_PATH))
    
    results = []
    found = 0
    
    for name in test_names:
        r = resolver.resolve(name, min_confidence=min_confidence)
        
        if r:
            enrichment = get_enrichment(conn, r.loinc_code)
            results.append({
                'test_name': name,
                'loinc_code': r.loinc_code,
                'standard_name': r.loinc_name,
                'confidence': f'{r.confidence:.2f}',
                'method': r.method,
                **enrichment
            })
            found += 1
        else:
            results.append({
                'test_name': name,
                'loinc_code': '',
                'standard_name': '',
                'confidence': '',
                'method': 'NOT_FOUND',
                'reference_low': '',
                'reference_high': '',
                'unit': '',
                'critical_low': '',
                'critical_high': '',
                'description': '',
                'clinical_note': ''
            })
    
    # Write CSV
    fieldnames = ['test_name', 'loinc_code', 'standard_name', 'confidence', 'method',
                  'reference_low', 'reference_high', 'unit', 
                  'critical_low', 'critical_high', 'description', 'clinical_note']
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"Translated {found}/{len(test_names)} tests")
    print(f"Output written to: {output_file}")
    
    resolver.close()
    conn.close()
    
    return results


def translate_single(test_name: str):
    """Translate a single test name and print details."""
    resolver = AutoResolver()
    conn = sqlite3.connect(str(DB_PATH))
    
    r = resolver.resolve(test_name)
    
    if r:
        enrichment = get_enrichment(conn, r.loinc_code)
        
        print(f"\n{test_name}")
        print("=" * 50)
        print(f"  LOINC: {r.loinc_code}")
        print(f"  Name: {r.loinc_name}")
        print(f"  Confidence: {r.confidence:.2f} ({r.method})")
        
        if enrichment['reference_low']:
            print(f"  Reference: {enrichment['reference_low']}-{enrichment['reference_high']} {enrichment['unit']}")
        
        if enrichment['critical_low'] or enrichment['critical_high']:
            crit = []
            if enrichment['critical_low']: crit.append(f"Low {enrichment['critical_low']}")
            if enrichment['critical_high']: crit.append(f"High {enrichment['critical_high']}")
            print(f"  Critical: {', '.join(crit)}")
        
        if enrichment['description']:
            print(f"  Description: {enrichment['description'][:150]}...")
    else:
        print(f"\n{test_name}: NOT FOUND")
    
    resolver.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Translate LIS test names to LOINC codes with enrichment',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python translate.py test-list.txt
  python translate.py test-list.txt -o results.csv
  python translate.py --single "Sodium Level"
  python translate.py --single "HbA1c"
        '''
    )
    
    parser.add_argument('input_file', nargs='?', help='Input file with test names (one per line)')
    parser.add_argument('-o', '--output', help='Output CSV file')
    parser.add_argument('-c', '--confidence', type=float, default=0.5, help='Minimum confidence (0-1)')
    parser.add_argument('-s', '--single', help='Translate a single test name')
    
    args = parser.parse_args()
    
    if args.single:
        translate_single(args.single)
    elif args.input_file:
        translate_file(args.input_file, args.output, args.confidence)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
