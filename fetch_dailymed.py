#!/usr/bin/env python3
"""
Clinical Rosetta Stone - DailyMed Drug Label Fetcher
Fetches FDA drug labels to extract drug-lab test interactions.

API: https://dailymed.nlm.nih.gov/dailymed/app-support-web-services.cfm

Drug labels contain sections like:
- "7 DRUG INTERACTIONS" 
- "7.2 Drug/Laboratory Test Interactions"

Features:
- Caches all API responses
- Resumable
- Extracts lab-relevant sections from labels
"""

import sqlite3
import requests
import time
import json
import argparse
import logging
import re
from lxml import etree
from pathlib import Path
from datetime import datetime
from html import unescape

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "clinical_rosetta.db"
CACHE_DIR = Path(__file__).parent / "raw_data" / "dailymed_cache"
PROGRESS_FILE = Path(__file__).parent / ".dailymed_progress.json"

# DailyMed API
BASE_URL = "https://dailymed.nlm.nih.gov/dailymed/services/v2"

# Rate limiting - be nice to NLM
REQUEST_DELAY = 0.3
BATCH_SIZE = 50

# Common drugs that affect lab tests
PRIORITY_DRUGS = [
    # Affects glucose
    "metformin", "insulin", "glipizide", "glyburide", "prednisone", 
    "dexamethasone", "hydrocortisone",
    # Affects liver enzymes (AST, ALT, ALP)
    "acetaminophen", "atorvastatin", "simvastatin", "rosuvastatin",
    "amiodarone", "methotrexate", "isoniazid",
    # Affects kidney function (BUN, creatinine, eGFR)
    "ibuprofen", "naproxen", "celecoxib", "lisinopril", "losartan",
    "metformin", "vancomycin", "gentamicin",
    # Affects thyroid tests (TSH, T3, T4)
    "levothyroxine", "methimazole", "propylthiouracil", "amiodarone", 
    "lithium", "biotin",
    # Affects coagulation (PT, INR, PTT)
    "warfarin", "heparin", "enoxaparin", "rivaroxaban", "apixaban",
    "aspirin", "clopidogrel",
    # Affects electrolytes (Na, K, Ca, Mg)
    "furosemide", "hydrochlorothiazide", "spironolactone", "lisinopril",
    "amlodipine",
    # Affects lipid panel
    "atorvastatin", "simvastatin", "fenofibrate", "niacin", "ezetimibe",
    # Affects CBC
    "methotrexate", "azathioprine", "chemotherapy",
    # Affects PSA
    "finasteride", "dutasteride",
    # Affects urine tests
    "rifampin", "phenazopyridine", "nitrofurantoin",
    # Common meds
    "omeprazole", "pantoprazole", "metoprolol", "amlodipine",
    "gabapentin", "pregabalin", "sertraline", "fluoxetine",
    "amoxicillin", "azithromycin", "ciprofloxacin", "doxycycline"
]


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {'drugs_processed': [], 'setids_fetched': [], 'last_run': None}


def save_progress(progress: dict):
    progress['last_run'] = datetime.now().isoformat()
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def cache_path(category: str, name: str) -> Path:
    subdir = CACHE_DIR / category
    subdir.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r'[^\w\-]', '_', name.lower())
    return subdir / f"{safe_name}.json"


def save_to_cache(category: str, name: str, data: dict):
    path = cache_path(category, name)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


def load_from_cache(category: str, name: str) -> dict:
    path = cache_path(category, name)
    if path.exists():
        with open(path, 'r') as f:
            return json.load(f)
    return None


def search_drug(drug_name: str) -> list:
    """Search for a drug and get its SPL setids."""
    cached = load_from_cache("search", drug_name)
    if cached:
        return cached.get('results', [])
    
    url = f"{BASE_URL}/spls.json"
    params = {'drug_name': drug_name, 'pagesize': 10}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            return []
        
        data = response.json()
        results = data.get('data', [])
        
        save_to_cache("search", drug_name, {'drug_name': drug_name, 'results': results})
        return results
        
    except Exception as e:
        logger.error(f"Error searching {drug_name}: {e}")
        return []


def fetch_label(setid: str) -> dict:
    """Fetch full SPL label for a setid (XML format, parsed to dict)."""
    cached = load_from_cache("labels", setid)
    if cached:
        return cached
    
    # DailyMed returns XML for full labels
    url = f"{BASE_URL}/spls/{setid}.xml"
    
    try:
        response = requests.get(url, timeout=60)
        if response.status_code != 200:
            return {'error': f'HTTP {response.status_code}'}
        
        # Parse XML and extract sections
        data = parse_spl_xml(response.text, setid)
        save_to_cache("labels", setid, data)
        return data
        
    except Exception as e:
        logger.error(f"Error fetching label {setid}: {e}")
        return {'error': str(e)}


def parse_spl_xml(xml_text: str, setid: str) -> dict:
    """Parse SPL XML using lxml and extract relevant sections."""
    result = {
        'setid': setid,
        'title': '',
        'sections': []
    }
    
    try:
        # Parse with lxml (handles namespaces automatically)
        root = etree.fromstring(xml_text.encode('utf-8'))
        
        # Define namespace
        ns = {'hl7': 'urn:hl7-org:v3'}
        
        # Get main title
        title_elem = root.find('.//hl7:title', ns)
        if title_elem is not None:
            result['title'] = ''.join(title_elem.itertext()).strip()[:200]
        
        # Find all sections
        for section in root.findall('.//hl7:section', ns):
            # Get section name from code displayName
            code_elem = section.find('hl7:code', ns)
            section_name = ''
            if code_elem is not None:
                section_name = code_elem.get('displayName', '')
            
            # Also check title element
            title_elem = section.find('hl7:title', ns)
            if title_elem is not None:
                title_text = ''.join(title_elem.itertext()).strip()
                if title_text:
                    section_name = title_text
            
            # Get section text content
            text_elem = section.find('hl7:text', ns)
            section_text = ''
            if text_elem is not None:
                section_text = ''.join(text_elem.itertext()).strip()
            
            if section_name and section_text:
                result['sections'].append({
                    'name': section_name,
                    'text': section_text[:5000]
                })
        
    except etree.XMLSyntaxError as e:
        logger.debug(f"XML syntax error: {e}")
    except Exception as e:
        logger.debug(f"Error parsing SPL: {e}")
    
    return result


def extract_lab_interactions(label_data: dict) -> dict:
    """Extract lab test interaction info from label."""
    result = {
        'drug_name': '',
        'setid': '',
        'lab_interactions': [],
        'drug_interactions_text': '',
        'warnings_text': ''
    }
    
    if not label_data or 'error' in label_data:
        return result
    
    result['setid'] = label_data.get('setid', '')
    result['drug_name'] = label_data.get('title', '')[:100]
    
    sections = label_data.get('sections', [])
    
    # Parse sections - look for drug interactions and lab test mentions
    
    lab_keywords = [
        'laboratory', 'lab test', 'test result', 'false positive', 'false negative',
        'interference', 'glucose', 'creatinine', 'liver function', 'thyroid',
        'coagulation', 'INR', 'PT', 'PTT', 'electrolyte', 'potassium', 'sodium',
        'calcium', 'magnesium', 'lipid', 'cholesterol', 'triglyceride',
        'hemoglobin', 'hematocrit', 'platelet', 'white blood cell', 'WBC',
        'ALT', 'AST', 'bilirubin', 'alkaline phosphatase', 'GGT',
        'BUN', 'GFR', 'urine', 'urinalysis', 'blood glucose', 'HbA1c',
        'TSH', 'T3', 'T4', 'PSA', 'biotin'
    ]
    
    for section in sections:
        name = section.get('name', '').lower()
        text = section.get('text', '')
        
        # Clean HTML
        clean_text = re.sub(r'<[^>]+>', ' ', text)
        clean_text = unescape(clean_text)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        # Check for lab-related sections
        if 'drug interaction' in name or 'interaction' in name:
            result['drug_interactions_text'] = clean_text[:2000]
            
            # Look for lab test mentions
            for keyword in lab_keywords:
                if keyword.lower() in clean_text.lower():
                    # Extract sentence containing keyword
                    sentences = re.split(r'[.!?]', clean_text)
                    for sent in sentences:
                        if keyword.lower() in sent.lower() and len(sent) > 20:
                            result['lab_interactions'].append({
                                'keyword': keyword,
                                'text': sent.strip()[:500]
                            })
                            break
        
        if 'warning' in name or 'precaution' in name:
            # Also check warnings for lab mentions
            for keyword in lab_keywords:
                if keyword.lower() in clean_text.lower():
                    sentences = re.split(r'[.!?]', clean_text)
                    for sent in sentences:
                        if keyword.lower() in sent.lower() and len(sent) > 20:
                            result['lab_interactions'].append({
                                'keyword': keyword,
                                'text': sent.strip()[:500],
                                'source': 'warnings'
                            })
                            break
    
    # Deduplicate
    seen = set()
    unique = []
    for item in result['lab_interactions']:
        key = item['text'][:100]
        if key not in seen:
            seen.add(key)
            unique.append(item)
    result['lab_interactions'] = unique
    
    return result


def fetch_drug_labels(drugs: list = None, limit: int = None):
    """Fetch and process drug labels for lab interactions."""
    if drugs is None:
        drugs = PRIORITY_DRUGS
    
    # Remove duplicates
    drugs = list(dict.fromkeys(drugs))
    
    if limit:
        drugs = drugs[:limit]
    
    progress = load_progress()
    done = set(progress.get('drugs_processed', []))
    to_process = [d for d in drugs if d.lower() not in done]
    
    if not to_process:
        print("All drugs already processed!")
        return
    
    print(f"Fetching DailyMed labels for {len(to_process)} drugs...")
    
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    
    # Ensure table exists
    conn.execute('''
        CREATE TABLE IF NOT EXISTS drug_lab_interaction (
            id INTEGER PRIMARY KEY,
            drug_name TEXT NOT NULL,
            setid TEXT,
            keyword TEXT,
            interaction_text TEXT,
            source TEXT DEFAULT 'DailyMed',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_drug_lab_drug ON drug_lab_interaction(drug_name)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_drug_lab_keyword ON drug_lab_interaction(keyword)')
    
    found_interactions = 0
    
    try:
        for i, drug in enumerate(to_process):
            # Search for drug
            results = search_drug(drug)
            time.sleep(REQUEST_DELAY)
            
            if not results:
                logger.info(f"[{i+1}/{len(to_process)}] - {drug}: not found")
                progress['drugs_processed'].append(drug.lower())
                continue
            
            # Get first label
            setid = results[0].get('setid')
            if not setid:
                continue
            
            # Fetch full label
            label = fetch_label(setid)
            time.sleep(REQUEST_DELAY)
            
            # Extract lab interactions
            extracted = extract_lab_interactions(label)
            
            if extracted['lab_interactions']:
                found_interactions += len(extracted['lab_interactions'])
                
                # Store in DB
                for interaction in extracted['lab_interactions']:
                    conn.execute('''
                        INSERT INTO drug_lab_interaction 
                        (drug_name, setid, keyword, interaction_text)
                        VALUES (?, ?, ?, ?)
                    ''', (
                        drug,
                        setid,
                        interaction.get('keyword', ''),
                        interaction.get('text', '')
                    ))
                
                logger.info(f"[{i+1}/{len(to_process)}] ✓ {drug}: {len(extracted['lab_interactions'])} lab interactions")
            else:
                logger.info(f"[{i+1}/{len(to_process)}] - {drug}: no lab interactions found")
            
            progress['drugs_processed'].append(drug.lower())
            
            if i % BATCH_SIZE == 0:
                conn.commit()
                save_progress(progress)
                
    except KeyboardInterrupt:
        print("\nInterrupted. Saving progress...")
    finally:
        conn.commit()
        save_progress(progress)
        conn.close()
    
    print(f"\nComplete: {found_interactions} lab interactions found")


def status():
    """Show current status."""
    progress = load_progress()
    
    drugs_done = len(progress.get('drugs_processed', []))
    
    # Count cache files
    search_cached = len(list((CACHE_DIR / "search").glob("*.json"))) if (CACHE_DIR / "search").exists() else 0
    labels_cached = len(list((CACHE_DIR / "labels").glob("*.json"))) if (CACHE_DIR / "labels").exists() else 0
    
    # Count DB entries
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        cursor = conn.execute('SELECT COUNT(*) FROM drug_lab_interaction')
        db_count = cursor.fetchone()[0]
    except:
        db_count = 0
    conn.close()
    
    print("DAILYMED FETCH STATUS")
    print("=" * 40)
    print(f"  Priority drugs: {len(PRIORITY_DRUGS)}")
    print(f"  Drugs processed: {drugs_done}")
    print(f"  Search cache: {search_cached} files")
    print(f"  Labels cache: {labels_cached} files")
    print(f"  Lab interactions in DB: {db_count}")
    
    if progress.get('last_run'):
        print(f"  Last run: {progress['last_run']}")


def show_interactions(drug_name: str = None):
    """Show stored lab interactions."""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    
    if drug_name:
        cursor = conn.execute('''
            SELECT drug_name, keyword, interaction_text 
            FROM drug_lab_interaction 
            WHERE LOWER(drug_name) LIKE LOWER(?)
        ''', (f'%{drug_name}%',))
    else:
        cursor = conn.execute('''
            SELECT drug_name, keyword, interaction_text 
            FROM drug_lab_interaction 
            ORDER BY drug_name
            LIMIT 50
        ''')
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print("No interactions found.")
        return
    
    print(f"Found {len(rows)} interactions:")
    print("=" * 60)
    
    current_drug = None
    for drug, keyword, text in rows:
        if drug != current_drug:
            print(f"\n{drug.upper()}")
            current_drug = drug
        print(f"  [{keyword}] {text[:100]}...")


def main():
    parser = argparse.ArgumentParser(
        description='Fetch drug-lab interactions from DailyMed FDA labels',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python fetch_dailymed.py --status           # Show status
  python fetch_dailymed.py --fetch            # Fetch all priority drugs
  python fetch_dailymed.py --fetch --limit 10 # Fetch 10 drugs
  python fetch_dailymed.py --show             # Show stored interactions
  python fetch_dailymed.py --show warfarin    # Show warfarin interactions
        '''
    )
    
    parser.add_argument('--status', action='store_true', help='Show current status')
    parser.add_argument('--fetch', action='store_true', help='Fetch drug labels')
    parser.add_argument('--limit', type=int, help='Limit number of drugs to process')
    parser.add_argument('--show', nargs='?', const='', help='Show stored interactions (optionally filter by drug)')
    
    args = parser.parse_args()
    
    if args.status:
        status()
    elif args.fetch:
        fetch_drug_labels(limit=args.limit)
    elif args.show is not None:
        show_interactions(args.show if args.show else None)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
