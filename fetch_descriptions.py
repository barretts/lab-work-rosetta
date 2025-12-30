#!/usr/bin/env python3
"""
Clinical Rosetta Stone - MedlinePlus Description Fetcher
Long-running script to fetch consumer descriptions for all LOINC codes.

Features:
- Rate limited (respects MedlinePlus 100 req/min limit)
- Resumable (tracks progress, skips already-fetched)
- Batched commits (doesn't lose progress on crash)
- Progress logging

Usage:
    python fetch_descriptions.py              # Fetch all missing
    python fetch_descriptions.py --limit 100  # Fetch 100 at a time
    python fetch_descriptions.py --resume     # Resume from last run
"""

import sqlite3
import requests
import time
import argparse
import logging
import json
import re
from pathlib import Path
from datetime import datetime
from html import unescape

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "clinical_rosetta.db"
PROGRESS_FILE = Path(__file__).parent / ".fetch_progress.json"
CACHE_DIR = Path(__file__).parent / "raw_data" / "medlineplus_cache"

# MedlinePlus Connect API
MEDLINEPLUS_URL = "https://connect.medlineplus.gov/service"
LOINC_OID = "2.16.840.1.113883.6.1"

# Rate limiting: 100 requests per minute = ~0.6 sec between requests
REQUEST_DELAY = 0.7  # seconds between requests
BATCH_SIZE = 50      # commit to DB every N fetches


def strip_html(text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not text:
        return ''
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def load_progress() -> dict:
    """Load progress from file."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {'fetched': [], 'no_result': [], 'errors': [], 'last_run': None}


def save_progress(progress: dict):
    """Save progress to file."""
    progress['last_run'] = datetime.now().isoformat()
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def save_to_cache(loinc_code: str, data: dict):
    """Save raw API response to cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{loinc_code}.json"
    with open(cache_file, 'w') as f:
        json.dump(data, f, indent=2)


def load_from_cache(loinc_code: str) -> dict:
    """Load cached API response if exists."""
    cache_file = CACHE_DIR / f"{loinc_code}.json"
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            return json.load(f)
    return None


def fetch_description(loinc_code: str, use_cache: bool = True) -> dict:
    """Fetch description from MedlinePlus Connect API (with caching)."""
    
    # Check cache first
    if use_cache:
        cached = load_from_cache(loinc_code)
        if cached:
            return parse_medlineplus_response(cached, from_cache=True)
    
    params = {
        'mainSearchCriteria.v.cs': LOINC_OID,
        'mainSearchCriteria.v.c': loinc_code,
        'knowledgeResponseType': 'application/json',
    }
    
    try:
        response = requests.get(MEDLINEPLUS_URL, params=params, timeout=30)
        
        if response.status_code != 200:
            return {'error': f'HTTP {response.status_code}'}
        
        data = response.json()
        
        # Save raw payload to cache
        save_to_cache(loinc_code, data)
        
        return parse_medlineplus_response(data)
        
    except requests.exceptions.Timeout:
        return {'error': 'timeout'}
    except requests.exceptions.RequestException as e:
        return {'error': str(e)}
    except Exception as e:
        return {'error': str(e)}


def parse_medlineplus_response(data: dict, from_cache: bool = False) -> dict:
    """Parse MedlinePlus API response."""
    feed = data.get('feed', {})
    entries = feed.get('entry', [])
    
    if not entries:
        return {'no_result': True, 'from_cache': from_cache}
    
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
    
    if not summary:
        return {'no_result': True, 'from_cache': from_cache}
    
    # Clean HTML
    clean_summary = strip_html(summary)
    
    return {
        'title': title,
        'description': clean_summary,
        'url': f"https://medlineplus.gov/lab-tests/{title.lower().replace(' ', '-')}/",
        'from_cache': from_cache
    }


def get_codes_to_fetch(conn: sqlite3.Connection, progress: dict, limit: int = None) -> list:
    """Get LOINC codes that need descriptions fetched."""
    
    # Get all lab-related LOINC codes
    cursor = conn.execute('''
        SELECT loinc_code, long_common_name 
        FROM loinc_concept 
        WHERE status = 'ACTIVE'
        ORDER BY loinc_code
    ''')
    all_codes = [(r[0], r[1]) for r in cursor.fetchall()]
    
    # Filter out already fetched (in DB)
    cursor = conn.execute('''
        SELECT DISTINCT loinc_code FROM concept_description 
        WHERE description_type = 'consumer'
    ''')
    in_db = set(r[0] for r in cursor.fetchall())
    
    # Filter out known no-results and errors from progress
    skip = set(progress.get('no_result', []) + progress.get('fetched', []))
    
    # Get codes to fetch
    to_fetch = [(code, name) for code, name in all_codes 
                if code not in in_db and code not in skip]
    
    if limit:
        to_fetch = to_fetch[:limit]
    
    return to_fetch


def run_fetcher(limit: int = None, resume: bool = True):
    """Main fetcher loop."""
    conn = sqlite3.connect(str(DB_PATH))
    
    # Load or reset progress
    if resume:
        progress = load_progress()
        logger.info(f"Resuming from previous run. Already processed: {len(progress.get('fetched', []))} fetched, {len(progress.get('no_result', []))} no results")
    else:
        progress = {'fetched': [], 'no_result': [], 'errors': [], 'last_run': None}
    
    # Get codes to fetch
    codes_to_fetch = get_codes_to_fetch(conn, progress, limit)
    
    if not codes_to_fetch:
        logger.info("No codes to fetch - all done!")
        conn.close()
        return
    
    logger.info(f"Fetching descriptions for {len(codes_to_fetch)} LOINC codes...")
    logger.info(f"Estimated time: {len(codes_to_fetch) * REQUEST_DELAY / 60:.1f} minutes")
    
    fetched_count = 0
    no_result_count = 0
    error_count = 0
    batch_count = 0
    
    start_time = time.time()
    
    try:
        for i, (loinc_code, name) in enumerate(codes_to_fetch):
            # Fetch from API
            result = fetch_description(loinc_code)
            
            if 'description' in result:
                # Success - save to DB
                try:
                    conn.execute('''
                        INSERT OR REPLACE INTO concept_description 
                        (loinc_code, source, language, description_type, description_text, 
                         source_url, retrieved_date)
                        VALUES (?, ?, ?, ?, ?, ?, date('now'))
                    ''', (
                        loinc_code, 'MedlinePlus', 'en', 'consumer',
                        result['description'][:2000],
                        result.get('url', '')
                    ))
                    fetched_count += 1
                    batch_count += 1
                    progress['fetched'].append(loinc_code)
                    logger.info(f"[{i+1}/{len(codes_to_fetch)}] ✓ {loinc_code}: {result['title'][:40]}")
                except Exception as e:
                    logger.error(f"[{i+1}/{len(codes_to_fetch)}] DB error for {loinc_code}: {e}")
                    error_count += 1
                    
            elif result.get('no_result'):
                no_result_count += 1
                progress['no_result'].append(loinc_code)
                if i % 20 == 0:  # Log every 20th no-result
                    logger.info(f"[{i+1}/{len(codes_to_fetch)}] - {loinc_code}: no MedlinePlus entry")
                    
            else:
                error_count += 1
                progress['errors'].append({'code': loinc_code, 'error': result.get('error', 'unknown')})
                logger.warning(f"[{i+1}/{len(codes_to_fetch)}] ✗ {loinc_code}: {result.get('error')}")
            
            # Batch commit
            if batch_count >= BATCH_SIZE:
                conn.commit()
                save_progress(progress)
                batch_count = 0
                
                # Progress summary
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed * 60
                remaining = (len(codes_to_fetch) - i - 1) / rate if rate > 0 else 0
                logger.info(f"Progress: {i+1}/{len(codes_to_fetch)} | Rate: {rate:.0f}/min | ETA: {remaining:.1f} min")
            
            # Rate limiting
            time.sleep(REQUEST_DELAY)
            
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user. Saving progress...")
    finally:
        # Final commit and save
        conn.commit()
        save_progress(progress)
        conn.close()
    
    # Summary
    elapsed = time.time() - start_time
    print("\n" + "=" * 50)
    print("FETCH COMPLETE")
    print("=" * 50)
    print(f"  Fetched: {fetched_count}")
    print(f"  No result: {no_result_count}")
    print(f"  Errors: {error_count}")
    print(f"  Time: {elapsed/60:.1f} minutes")
    print(f"  Progress saved to: {PROGRESS_FILE}")


def reprocess_cache():
    """Reprocess all cached responses into database."""
    if not CACHE_DIR.exists():
        print("No cache directory found.")
        return
    
    cache_files = list(CACHE_DIR.glob("*.json"))
    if not cache_files:
        print("No cached files found.")
        return
    
    print(f"Reprocessing {len(cache_files)} cached responses...")
    
    conn = sqlite3.connect(str(DB_PATH))
    processed = 0
    
    for cache_file in cache_files:
        loinc_code = cache_file.stem
        
        with open(cache_file, 'r') as f:
            data = json.load(f)
        
        result = parse_medlineplus_response(data)
        
        if 'description' in result:
            try:
                conn.execute('''
                    INSERT OR REPLACE INTO concept_description 
                    (loinc_code, source, language, description_type, description_text, 
                     source_url, retrieved_date)
                    VALUES (?, ?, ?, ?, ?, ?, date('now'))
                ''', (
                    loinc_code, 'MedlinePlus', 'en', 'consumer',
                    result['description'][:2000],
                    result.get('url', '')
                ))
                processed += 1
            except Exception as e:
                logger.error(f"Error processing {loinc_code}: {e}")
    
    conn.commit()
    conn.close()
    print(f"Reprocessed {processed} descriptions from cache.")


def status():
    """Show current status."""
    conn = sqlite3.connect(str(DB_PATH))
    progress = load_progress()
    
    cursor = conn.execute('SELECT COUNT(*) FROM loinc_concept WHERE status = "ACTIVE"')
    total = cursor.fetchone()[0]
    
    cursor = conn.execute('SELECT COUNT(DISTINCT loinc_code) FROM concept_description WHERE description_type = "consumer"')
    in_db = cursor.fetchone()[0]
    
    no_result = len(progress.get('no_result', []))
    
    # Count cached files
    cached = len(list(CACHE_DIR.glob("*.json"))) if CACHE_DIR.exists() else 0
    
    print("DESCRIPTION FETCH STATUS")
    print("=" * 40)
    print(f"  Total LOINC codes: {total:,}")
    print(f"  With descriptions: {in_db:,}")
    print(f"  Cached responses: {cached:,}")
    print(f"  Known no-result: {no_result:,}")
    print(f"  Remaining to try: {total - in_db - no_result:,}")
    
    if progress.get('last_run'):
        print(f"  Last run: {progress['last_run']}")
    
    if cached > 0:
        cache_size = sum(f.stat().st_size for f in CACHE_DIR.glob("*.json"))
        print(f"  Cache size: {cache_size / 1024 / 1024:.1f} MB")
    
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Fetch MedlinePlus descriptions for LOINC codes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python fetch_descriptions.py              # Fetch all missing
  python fetch_descriptions.py --limit 500  # Fetch 500 at a time
  python fetch_descriptions.py --status     # Show current status
  python fetch_descriptions.py --reprocess  # Reprocess cached responses into DB
  python fetch_descriptions.py --reset      # Reset progress and start fresh
        '''
    )
    
    parser.add_argument('--limit', type=int, help='Maximum number of codes to fetch')
    parser.add_argument('--status', action='store_true', help='Show current status')
    parser.add_argument('--reprocess', action='store_true', help='Reprocess cached responses into database')
    parser.add_argument('--reset', action='store_true', help='Reset progress tracking')
    parser.add_argument('--no-resume', action='store_true', help='Do not resume from previous run')
    parser.add_argument('--no-cache', action='store_true', help='Skip cache, always fetch from API')
    
    args = parser.parse_args()
    
    if args.status:
        status()
    elif args.reprocess:
        reprocess_cache()
    elif args.reset:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
            print("Progress reset.")
        else:
            print("No progress file found.")
    else:
        run_fetcher(limit=args.limit, resume=not args.no_resume)


if __name__ == "__main__":
    main()
