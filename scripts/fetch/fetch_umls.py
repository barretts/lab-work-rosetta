#!/usr/bin/env python3
"""
Clinical Rosetta Stone - UMLS/RxNorm/SNOMED Fetcher
Fetches terminology mappings and drug-lab interactions via NLM APIs.

APIs used:
- UMLS UTS REST API: https://documentation.uts.nlm.nih.gov/rest/home.html
- RxNorm API: https://lhncbc.nlm.nih.gov/RxNav/APIs/index.html

Features:
- Caches all API responses
- Resumable
- Rate limited (respects NLM limits)
"""

import sqlite3
import requests
import time
import json
import argparse
import logging
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Project root is two levels up from scripts/fetch/
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Load API key from .env
load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(PROJECT_ROOT / "raw_data" / "NCI_Thesaurus.FLAT" / ".env")

UMLS_API_KEY = os.getenv("UMLS_API_KEY")

DB_PATH = PROJECT_ROOT / "clinical_rosetta.db"
CACHE_DIR = PROJECT_ROOT / "raw_data" / "umls_cache"
PROGRESS_FILE = PROJECT_ROOT / ".umls_progress.json"

# API endpoints
UMLS_BASE = "https://uts-ws.nlm.nih.gov/rest"
RXNORM_BASE = "https://rxnav.nlm.nih.gov/REST"

# Rate limiting
REQUEST_DELAY = 0.25  # 4 req/sec for UMLS
BATCH_SIZE = 50

# Parallel processing defaults
DEFAULT_WORKERS = 4   # UMLS allows ~20 req/sec with API key
MAX_WORKERS = 8


def ensure_api_key():
    """Check that API key is configured."""
    if not UMLS_API_KEY:
        print("ERROR: UMLS_API_KEY not found.")
        print("Set it in .env file: UMLS_API_KEY=your-key-here")
        print("Get a free key at: https://uts.nlm.nih.gov/uts/")
        return False
    return True


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {'loinc_snomed': [], 'rxnorm_interactions': [], 'last_run': None}


def save_progress(progress: dict):
    progress['last_run'] = datetime.now().isoformat()
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def cache_path(category: str, code: str) -> Path:
    """Get cache file path for a code."""
    subdir = CACHE_DIR / category
    subdir.mkdir(parents=True, exist_ok=True)
    return subdir / f"{code.replace('/', '_')}.json"


def save_to_cache(category: str, code: str, data: dict):
    path = cache_path(category, code)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


def load_from_cache(category: str, code: str) -> dict:
    path = cache_path(category, code)
    if path.exists():
        with open(path, 'r') as f:
            return json.load(f)
    return None


# ============================================================
# UMLS UTS API - LOINC to SNOMED CT mappings
# ============================================================

def get_umls_tgt():
    """Get a Ticket Granting Ticket (TGT) for UMLS API auth."""
    url = "https://utslogin.nlm.nih.gov/cas/v1/api-key"
    response = requests.post(url, data={'apikey': UMLS_API_KEY})
    
    if response.status_code != 201:
        logger.error(f"Failed to get TGT: {response.status_code}")
        return None
    
    # Extract TGT URL from response
    import re
    match = re.search(r'action="([^"]+)"', response.text)
    if match:
        return match.group(1)
    return None


def get_umls_ticket(tgt_url: str) -> str:
    """Get a single-use Service Ticket from TGT."""
    response = requests.post(tgt_url, data={'service': 'http://umlsks.nlm.nih.gov'})
    if response.status_code == 200:
        return response.text
    return None


def fetch_loinc_snomed_mapping(loinc_code: str, tgt_url: str) -> dict:
    """Fetch SNOMED CT mapping for a LOINC code via UMLS."""
    
    # Check cache first
    cached = load_from_cache("loinc_snomed", loinc_code)
    if cached:
        return cached
    
    # Get service ticket
    ticket = get_umls_ticket(tgt_url)
    if not ticket:
        return {'error': 'Failed to get service ticket'}
    
    # Search for LOINC code in UMLS
    url = f"{UMLS_BASE}/search/current"
    params = {
        'string': loinc_code,
        'sabs': 'LNC',  # LOINC source
        'ticket': ticket,
        'returnIdType': 'code'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            return {'error': f'HTTP {response.status_code}'}
        
        data = response.json()
        results = data.get('result', {}).get('results', [])
        
        if not results:
            save_to_cache("loinc_snomed", loinc_code, {'no_result': True})
            return {'no_result': True}
        
        # Get CUI (Concept Unique Identifier)
        cui = results[0].get('ui')
        
        if cui:
            # Get SNOMED CT atoms for this CUI
            ticket2 = get_umls_ticket(tgt_url)
            atoms_url = f"{UMLS_BASE}/content/current/CUI/{cui}/atoms"
            atoms_params = {
                'sabs': 'SNOMEDCT_US',
                'ticket': ticket2
            }
            
            atoms_response = requests.get(atoms_url, params=atoms_params, timeout=30)
            if atoms_response.status_code == 200:
                atoms_data = atoms_response.json()
                snomed_atoms = atoms_data.get('result', [])
                
                result = {
                    'loinc_code': loinc_code,
                    'cui': cui,
                    'snomed_codes': [
                        {
                            'code': a.get('code'),
                            'name': a.get('name'),
                            'tty': a.get('termType')
                        }
                        for a in snomed_atoms if a.get('code')
                    ]
                }
                save_to_cache("loinc_snomed", loinc_code, result)
                return result
        
        save_to_cache("loinc_snomed", loinc_code, {'no_result': True, 'cui': cui})
        return {'no_result': True}
        
    except Exception as e:
        return {'error': str(e)}


# ============================================================
# RxNorm API - Drug-Lab Interactions
# ============================================================

def fetch_rxnorm_interactions(drug_name: str) -> dict:
    """Fetch drug interactions that might affect lab tests."""
    
    # Check cache first
    cached = load_from_cache("rxnorm", drug_name.lower().replace(' ', '_'))
    if cached:
        return cached
    
    # First, get RxCUI for drug name
    url = f"{RXNORM_BASE}/rxcui.json"
    params = {'name': drug_name, 'search': 1}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            return {'error': f'HTTP {response.status_code}'}
        
        data = response.json()
        rxcuis = data.get('idGroup', {}).get('rxnormId', [])
        
        if not rxcuis:
            save_to_cache("rxnorm", drug_name.lower().replace(' ', '_'), {'no_result': True})
            return {'no_result': True}
        
        rxcui = rxcuis[0]
        
        # Get drug properties
        props_url = f"{RXNORM_BASE}/rxcui/{rxcui}/properties.json"
        props_response = requests.get(props_url, timeout=30)
        props = props_response.json().get('properties', {}) if props_response.status_code == 200 else {}
        
        # Get drug-drug interactions (which can affect lab values)
        interactions_url = f"{RXNORM_BASE}/interaction/interaction.json"
        int_response = requests.get(interactions_url, params={'rxcui': rxcui}, timeout=30)
        interactions = []
        
        if int_response.status_code == 200:
            int_data = int_response.json()
            for group in int_data.get('interactionTypeGroup', []):
                for itype in group.get('interactionType', []):
                    for pair in itype.get('interactionPair', []):
                        interactions.append({
                            'severity': pair.get('severity'),
                            'description': pair.get('description'),
                            'drug': pair.get('interactionConcept', [{}])[1].get('minConceptItem', {}).get('name')
                        })
        
        result = {
            'drug_name': drug_name,
            'rxcui': rxcui,
            'properties': props,
            'interactions': interactions[:20]  # Limit to 20
        }
        
        save_to_cache("rxnorm", drug_name.lower().replace(' ', '_'), result)
        return result
        
    except Exception as e:
        return {'error': str(e)}


# ============================================================
# Batch Fetchers
# ============================================================

def fetch_all_loinc_snomed(limit: int = None, workers: int = 1):
    """Fetch SNOMED mappings for all LOINC codes."""
    if not ensure_api_key():
        return
    
    conn = sqlite3.connect(str(DB_PATH))
    progress = load_progress()
    
    # Get LOINC codes to process
    cursor = conn.execute('''
        SELECT loinc_code FROM loinc_concept 
        WHERE status = 'ACTIVE'
        ORDER BY loinc_code
    ''')
    all_codes = [r[0] for r in cursor.fetchall()]
    
    # Filter already processed
    done = set(progress.get('loinc_snomed', []))
    to_process = [c for c in all_codes if c not in done]
    
    if limit:
        to_process = to_process[:limit]
    
    if not to_process:
        print("All LOINC codes already processed!")
        conn.close()
        return
    
    # Use parallel mode if workers > 1
    if workers > 1:
        conn.close()
        fetch_loinc_snomed_parallel(to_process, progress, workers)
        return
    
    print(f"Fetching SNOMED mappings for {len(to_process)} LOINC codes...")
    print(f"Estimated time: {len(to_process) * REQUEST_DELAY / 60:.1f} minutes")
    
    # Get TGT (valid for ~8 hours)
    tgt_url = get_umls_tgt()
    if not tgt_url:
        print("Failed to authenticate with UMLS")
        conn.close()
        return
    
    mapped = 0
    no_mapping = 0
    
    try:
        for i, loinc_code in enumerate(to_process):
            result = fetch_loinc_snomed_mapping(loinc_code, tgt_url)
            
            if result.get('snomed_codes'):
                mapped += 1
                # Store in DB
                for snomed in result['snomed_codes']:
                    try:
                        conn.execute('''
                            INSERT OR IGNORE INTO concept_synonym 
                            (loinc_code, source, synonym_type, synonym_text)
                            VALUES (?, ?, ?, ?)
                        ''', (loinc_code, 'SNOMED_CT', 'snomed_code', snomed['code']))
                    except:
                        pass
                
                if i % 20 == 0:
                    logger.info(f"[{i+1}/{len(to_process)}] ✓ {loinc_code}: {len(result['snomed_codes'])} SNOMED codes")
            else:
                no_mapping += 1
                if i % 100 == 0:
                    logger.info(f"[{i+1}/{len(to_process)}] - {loinc_code}: no SNOMED mapping")
            
            progress['loinc_snomed'].append(loinc_code)
            
            if i % BATCH_SIZE == 0:
                conn.commit()
                save_progress(progress)
            
            time.sleep(REQUEST_DELAY)
            
    except KeyboardInterrupt:
        print("\nInterrupted. Saving progress...")
    finally:
        conn.commit()
        save_progress(progress)
        conn.close()
    
    print(f"\nComplete: {mapped} mapped, {no_mapping} no mapping")


def fetch_loinc_snomed_parallel(to_process: list, progress: dict, workers: int):
    """Parallel SNOMED fetcher using asyncio with semaphore-controlled concurrency."""
    workers = min(workers, MAX_WORKERS)
    logger.info(f"Fetching SNOMED mappings for {len(to_process)} codes with {workers} workers...")
    
    effective_rate = workers / REQUEST_DELAY
    est_minutes = len(to_process) / effective_rate / 60
    logger.info(f"Estimated time: {est_minutes:.1f} minutes (parallel mode)")
    
    # Get TGT (valid for ~8 hours)
    tgt_url = get_umls_tgt()
    if not tgt_url:
        print("Failed to authenticate with UMLS")
        return
    
    state = {
        'mapped': 0,
        'no_mapping': 0,
        'processed': 0,
        'total': len(to_process),
        'start_time': time.time(),
        'progress': progress,
        'lock': asyncio.Lock(),
    }
    
    async def fetch_single(loinc_code: str, semaphore: asyncio.Semaphore, 
                          executor: ThreadPoolExecutor):
        """Fetch a single code with semaphore control."""
        async with semaphore:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                executor, 
                fetch_loinc_snomed_mapping, 
                loinc_code, 
                tgt_url
            )
            await asyncio.sleep(REQUEST_DELAY)
            return (loinc_code, result)
    
    async def process_results(results: list, conn: sqlite3.Connection):
        """Process batch of results and update DB."""
        async with state['lock']:
            for loinc_code, result in results:
                state['processed'] += 1
                
                if result.get('snomed_codes'):
                    state['mapped'] += 1
                    for snomed in result['snomed_codes']:
                        try:
                            conn.execute('''
                                INSERT OR IGNORE INTO concept_synonym 
                                (loinc_code, source, synonym_type, synonym_text)
                                VALUES (?, ?, ?, ?)
                            ''', (loinc_code, 'SNOMED_CT', 'snomed_code', snomed['code']))
                        except:
                            pass
                else:
                    state['no_mapping'] += 1
                
                state['progress']['loinc_snomed'].append(loinc_code)
            
            conn.commit()
            save_progress(state['progress'])
            
            elapsed = time.time() - state['start_time']
            rate = state['processed'] / elapsed * 60 if elapsed > 0 else 0
            remaining = (state['total'] - state['processed']) / rate if rate > 0 else 0
            logger.info(f"Progress: {state['processed']}/{state['total']} | "
                       f"✓{state['mapped']} -{state['no_mapping']} | "
                       f"Rate: {rate:.0f}/min | ETA: {remaining:.1f} min")
    
    async def run_all():
        """Main async entry point."""
        semaphore = asyncio.Semaphore(workers)
        conn = sqlite3.connect(str(DB_PATH))
        
        try:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                for batch_start in range(0, len(to_process), BATCH_SIZE):
                    batch = to_process[batch_start:batch_start + BATCH_SIZE]
                    
                    tasks = [
                        fetch_single(code, semaphore, executor)
                        for code in batch
                    ]
                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    valid_results = []
                    for i, r in enumerate(results):
                        if isinstance(r, Exception):
                            logger.warning(f"Exception for {batch[i]}: {r}")
                        else:
                            valid_results.append(r)
                    
                    await process_results(valid_results, conn)
                    
        except KeyboardInterrupt:
            logger.info("\nInterrupted. Saving progress...")
        finally:
            conn.commit()
            save_progress(state['progress'])
            conn.close()
    
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        pass
    
    elapsed = time.time() - state['start_time']
    print("\n" + "=" * 50)
    print("SNOMED FETCH COMPLETE (PARALLEL MODE)")
    print("=" * 50)
    print(f"  Workers: {workers}")
    print(f"  Mapped: {state['mapped']}")
    print(f"  No mapping: {state['no_mapping']}")
    print(f"  Time: {elapsed/60:.1f} minutes")


def fetch_common_drug_interactions():
    """Fetch interactions for common drugs that affect lab tests."""
    
    # Drugs known to affect common lab tests
    DRUGS_AFFECTING_LABS = [
        # Affects glucose
        "metformin", "insulin", "prednisone", "dexamethasone",
        # Affects liver enzymes
        "acetaminophen", "atorvastatin", "simvastatin", "amiodarone",
        # Affects kidney function
        "ibuprofen", "naproxen", "lisinopril", "methotrexate",
        # Affects thyroid tests
        "levothyroxine", "amiodarone", "lithium", "biotin",
        # Affects coagulation
        "warfarin", "aspirin", "clopidogrel", "heparin", "rivaroxaban",
        # Affects electrolytes
        "furosemide", "hydrochlorothiazide", "spironolactone",
        # Affects lipids
        "atorvastatin", "niacin", "fenofibrate",
        # Common antibiotics
        "amoxicillin", "ciprofloxacin", "azithromycin",
        # Affects PSA
        "finasteride", "dutasteride",
        # Miscellaneous
        "omeprazole", "gabapentin", "sertraline", "lisinopril"
    ]
    
    # Remove duplicates
    drugs = list(set(DRUGS_AFFECTING_LABS))
    
    print(f"Fetching RxNorm data for {len(drugs)} common drugs...")
    
    conn = sqlite3.connect(str(DB_PATH))
    
    # Ensure drug_lab_interaction table exists
    conn.execute('''
        CREATE TABLE IF NOT EXISTS drug_lab_interaction (
            id INTEGER PRIMARY KEY,
            drug_name TEXT,
            rxcui TEXT,
            affected_test TEXT,
            effect_description TEXT,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    for i, drug in enumerate(drugs):
        result = fetch_rxnorm_interactions(drug)
        
        if result.get('rxcui'):
            logger.info(f"[{i+1}/{len(drugs)}] ✓ {drug}: RxCUI {result['rxcui']}, {len(result.get('interactions', []))} interactions")
        else:
            logger.info(f"[{i+1}/{len(drugs)}] - {drug}: not found")
        
        time.sleep(0.1)  # RxNorm is more lenient
    
    conn.close()
    print("Done!")


def status():
    """Show current status."""
    progress = load_progress()
    
    loinc_done = len(progress.get('loinc_snomed', []))
    rxnorm_done = len(progress.get('rxnorm_interactions', []))
    
    # Count cache files
    snomed_cached = len(list((CACHE_DIR / "loinc_snomed").glob("*.json"))) if (CACHE_DIR / "loinc_snomed").exists() else 0
    rxnorm_cached = len(list((CACHE_DIR / "rxnorm").glob("*.json"))) if (CACHE_DIR / "rxnorm").exists() else 0
    
    print("UMLS FETCH STATUS")
    print("=" * 40)
    print(f"  LOINC→SNOMED processed: {loinc_done:,}")
    print(f"  SNOMED cache files: {snomed_cached:,}")
    print(f"  RxNorm cache files: {rxnorm_cached:,}")
    
    if progress.get('last_run'):
        print(f"  Last run: {progress['last_run']}")
    
    if not UMLS_API_KEY:
        print("\n  ⚠️  UMLS_API_KEY not set!")


def main():
    parser = argparse.ArgumentParser(
        description='Fetch UMLS/RxNorm/SNOMED data for lab tests',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f'''
Examples:
  python fetch_umls.py --status              # Show status
  python fetch_umls.py --snomed              # Fetch LOINC→SNOMED mappings
  python fetch_umls.py --snomed --workers 4  # Parallel with 4 workers
  python fetch_umls.py --snomed --limit 100  # Fetch 100 mappings
  python fetch_umls.py --rxnorm              # Fetch drug-lab interactions

Parallel mode:
  Default workers: {DEFAULT_WORKERS}, Max workers: {MAX_WORKERS}
  UMLS allows ~20 req/sec with API key, so 4 workers is safe.
        '''
    )
    
    parser.add_argument('--status', action='store_true', help='Show current status')
    parser.add_argument('--snomed', action='store_true', help='Fetch LOINC to SNOMED CT mappings')
    parser.add_argument('--rxnorm', action='store_true', help='Fetch drug-lab interactions')
    parser.add_argument('--limit', type=int, help='Limit number of codes to process')
    parser.add_argument('--workers', '-w', type=int, default=1,
                       help=f'Number of parallel workers (default: 1, max: {MAX_WORKERS})')
    
    args = parser.parse_args()
    
    if args.status:
        status()
    elif args.snomed:
        workers = min(max(1, args.workers), MAX_WORKERS)
        fetch_all_loinc_snomed(limit=args.limit, workers=workers)
    elif args.rxnorm:
        fetch_common_drug_interactions()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
