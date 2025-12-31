#!/usr/bin/env python3
"""
Clinical Rosetta Stone - Data Downloader
Downloads all freely accessible datasets for the translation database.

Sources:
- NHANES 2017-2020 (Biochemistry, CBC, Demographics)
- NCI Thesaurus (FLAT format for definitions)
- MedlinePlus XML (Health topics bulk download)
- CDC LIVD (LOINC mappings for IVD tests)
- CTCAE v5.0 (Adverse event grading)
- CMS CCS (Clinical Classifications Software)

Note: LOINC Core Table requires manual download from loinc.org (free registration)
"""

import os
import sys
import requests
import zipfile
import logging
import hashlib
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List, Dict, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configuration
DATA_DIR = Path("./raw_data")
DOWNLOADS_DIR = Path("./downloads")
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# Parallel processing defaults
DEFAULT_WORKERS = 3   # Conservative for downloads
MAX_WORKERS = 6       # Don't overwhelm servers

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


@dataclass
class DataSource:
    """Represents a downloadable data source."""
    name: str
    url: str
    filename: str
    description: str
    layer: str  # identity, statistical, knowledge, severity
    extract_zip: bool = False
    required: bool = True
    post_download: Optional[Callable] = None


# ============================================================================
# DATA SOURCE DEFINITIONS
# ============================================================================

NHANES_BASE = "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2017/DataFiles"

DATA_SOURCES: List[DataSource] = [
    # -------------------------------------------------------------------------
    # LAYER 1: IDENTITY (Mapping Standards)
    # -------------------------------------------------------------------------
    DataSource(
        name="NCI Thesaurus (FLAT)",
        url="https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Thesaurus.FLAT.zip",
        filename="NCI_Thesaurus.FLAT.zip",
        description="Medical terminology with definitions, synonyms, hierarchies",
        layer="identity",
        extract_zip=True,
    ),
    DataSource(
        name="CDC LIVD - SARS-CoV-2",
        url="https://www.cdc.gov/csels/dls/documents/livd_files/SARS-CoV-2-LIVD-Test-Codes.xlsx",
        filename="LIVD_SARS_CoV2.xlsx",
        description="LOINC mappings for COVID-19 diagnostic tests",
        layer="identity",
        required=False,
    ),
    DataSource(
        name="CMS CCS Single-Level Diagnosis",
        url="https://hcup-us.ahrq.gov/toolssoftware/ccs/Single_Level_CCS_2015.zip",
        filename="CCS_SingleLevel_2015.zip",
        description="ICD-9/10 to clinical category mappings",
        layer="identity",
        extract_zip=True,
        required=False,
    ),
    
    # -------------------------------------------------------------------------
    # LAYER 2: STATISTICAL (Reference Ranges from NHANES)
    # -------------------------------------------------------------------------
    # NHANES 2017-2020 Pre-pandemic (P_ prefix files) - Updated URL format
    DataSource(
        name="NHANES 2017-2020 Biochemistry",
        url=f"{NHANES_BASE}/P_BIOPRO.xpt",
        filename="NHANES_P_BIOPRO.XPT",
        description="Standard biochemistry panel (glucose, liver enzymes, electrolytes)",
        layer="statistical",
    ),
    DataSource(
        name="NHANES 2017-2020 CBC",
        url=f"{NHANES_BASE}/P_CBC.xpt",
        filename="NHANES_P_CBC.XPT",
        description="Complete blood count with differential",
        layer="statistical",
    ),
    DataSource(
        name="NHANES 2017-2020 Demographics",
        url=f"{NHANES_BASE}/P_DEMO.xpt",
        filename="NHANES_P_DEMO.XPT",
        description="Participant demographics (age, sex, race/ethnicity)",
        layer="statistical",
    ),
    # Additional NHANES lab panels
    DataSource(
        name="NHANES 2017-2020 Lipids",
        url=f"{NHANES_BASE}/P_TRIGLY.xpt",
        filename="NHANES_P_TRIGLY.XPT",
        description="Cholesterol and triglycerides",
        layer="statistical",
        required=False,
    ),
    DataSource(
        name="NHANES 2017-2020 Glycohemoglobin",
        url=f"{NHANES_BASE}/P_GHB.xpt",
        filename="NHANES_P_GHB.XPT",
        description="HbA1c for diabetes monitoring",
        layer="statistical",
        required=False,
    ),
    DataSource(
        name="NHANES 2017-2020 Iron Status",
        url=f"{NHANES_BASE}/P_FETIB.xpt",
        filename="NHANES_P_FETIB.XPT",
        description="Ferritin, iron, TIBC",
        layer="statistical",
        required=False,
    ),
    DataSource(
        name="NHANES 2017-2020 Thyroid",
        url=f"{NHANES_BASE}/P_THYROD.xpt",
        filename="NHANES_P_THYROD.XPT",
        description="TSH and thyroid hormones",
        layer="statistical",
        required=False,
    ),
    DataSource(
        name="NHANES 2017-2020 Kidney Function",
        url=f"{NHANES_BASE}/P_ALB_CR.xpt",
        filename="NHANES_P_ALB_CR.XPT",
        description="Albumin/creatinine ratio",
        layer="statistical",
        required=False,
    ),
    
    # -------------------------------------------------------------------------
    # LAYER 3: KNOWLEDGE (Human-readable descriptions)
    # -------------------------------------------------------------------------
    DataSource(
        name="MedlinePlus Health Topics (Compressed)",
        url="https://medlineplus.gov/xml/mplus_topics.xml.zip",
        filename="MedlinePlus_Topics.zip",
        description="Consumer health information XML",
        layer="knowledge",
        extract_zip=True,
        required=False,
    ),
    
    # -------------------------------------------------------------------------
    # LAYER 4: SEVERITY (Clinical thresholds)
    # -------------------------------------------------------------------------
    DataSource(
        name="CTCAE v5.0",
        url="https://ctep.cancer.gov/protocoldevelopment/electronic_applications/docs/CTCAE_v5_Quick_Reference_5x7.pdf",
        filename="CTCAE_v5_QuickRef.pdf",
        description="Adverse event grading criteria (PDF reference)",
        layer="severity",
        required=False,
    ),
    DataSource(
        name="CTCAE v5.0 Excel",
        url="https://ctep.cancer.gov/protocoldevelopment/electronic_applications/docs/CTCAE_v5.0.xlsx",
        filename="CTCAE_v5.0.xlsx",
        description="Adverse event grading criteria (full Excel)",
        layer="severity",
    ),
]


# ============================================================================
# DOWNLOADER CLASS
# ============================================================================

class RosettaDownloader:
    """Downloads and manages data sources for Clinical Rosetta Stone."""
    
    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ClinicalRosettaStone/1.0 (Research; https://github.com/clinical-rosetta)'
        })
        
    def download_file(self, source: DataSource) -> Dict:
        """
        Download a single data source.
        
        Returns dict with status, path, size, and any errors.
        """
        filepath = self.data_dir / source.filename
        result = {
            'name': source.name,
            'layer': source.layer,
            'filepath': str(filepath),
            'success': False,
            'skipped': False,
            'error': None,
            'size_bytes': 0,
        }
        
        # Check if already exists
        if filepath.exists():
            result['success'] = True
            result['skipped'] = True
            result['size_bytes'] = filepath.stat().st_size
            logger.info(f"✓ {source.name} - already exists ({self._format_size(result['size_bytes'])})")
            return result
        
        logger.info(f"⬇ Downloading {source.name}...")
        
        try:
            response = self.session.get(source.url, stream=True, timeout=120)
            response.raise_for_status()
            
            # Get total size if available
            total_size = int(response.headers.get('content-length', 0))
            
            # Download with progress
            downloaded = 0
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size:
                            pct = (downloaded / total_size) * 100
                            print(f"\r  Progress: {pct:.1f}% ({self._format_size(downloaded)})", end='', flush=True)
            
            print()  # Newline after progress
            
            result['success'] = True
            result['size_bytes'] = filepath.stat().st_size
            
            # Extract if ZIP
            if source.extract_zip and filepath.suffix.lower() == '.zip':
                self._extract_zip(filepath, source.name)
            
            logger.info(f"✓ {source.name} - downloaded ({self._format_size(result['size_bytes'])})")
            
        except requests.exceptions.HTTPError as e:
            result['error'] = f"HTTP {e.response.status_code}: {e.response.reason}"
            if source.required:
                logger.error(f"✗ {source.name} - {result['error']}")
            else:
                logger.warning(f"⚠ {source.name} - {result['error']} (optional)")
                
        except requests.exceptions.RequestException as e:
            result['error'] = str(e)
            if source.required:
                logger.error(f"✗ {source.name} - {result['error']}")
            else:
                logger.warning(f"⚠ {source.name} - {result['error']} (optional)")
                
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"✗ {source.name} - Unexpected error: {result['error']}")
            
        return result
    
    def _extract_zip(self, filepath: Path, source_name: str):
        """Extract ZIP file to data directory."""
        extract_dir = self.data_dir / filepath.stem
        
        try:
            with zipfile.ZipFile(filepath, 'r') as zf:
                zf.extractall(extract_dir)
            logger.info(f"  → Extracted to {extract_dir}")
        except zipfile.BadZipFile:
            logger.warning(f"  → Could not extract {filepath.name} (not a valid ZIP)")
    
    def download_all(self, layers: Optional[List[str]] = None, parallel: bool = False, 
                     workers: int = DEFAULT_WORKERS) -> List[Dict]:
        """
        Download all data sources.
        
        Args:
            layers: Optional filter for specific layers (identity, statistical, knowledge, severity)
            parallel: Use parallel downloads (be careful with rate limits)
            workers: Number of parallel workers (default: 3, max: 6)
            
        Returns:
            List of result dictionaries
        """
        sources = DATA_SOURCES
        if layers:
            sources = [s for s in sources if s.layer in layers]
        
        logger.info(f"Starting download of {len(sources)} data sources...")
        logger.info(f"Data directory: {self.data_dir.absolute()}")
        if parallel:
            workers = min(max(1, workers), MAX_WORKERS)
            logger.info(f"Parallel mode: {workers} workers")
        print("-" * 60)
        
        results = []
        
        if parallel:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(self.download_file, src): src for src in sources}
                for future in as_completed(futures):
                    results.append(future.result())
        else:
            for source in sources:
                results.append(self.download_file(source))
                time.sleep(0.5)  # Be nice to servers
        
        return results
    
    def print_summary(self, results: List[Dict]):
        """Print download summary."""
        print("\n" + "=" * 60)
        print("DOWNLOAD SUMMARY")
        print("=" * 60)
        
        by_layer = {}
        for r in results:
            layer = r['layer']
            if layer not in by_layer:
                by_layer[layer] = {'success': 0, 'failed': 0, 'skipped': 0, 'size': 0}
            
            if r['success']:
                by_layer[layer]['success'] += 1
                by_layer[layer]['size'] += r['size_bytes']
                if r['skipped']:
                    by_layer[layer]['skipped'] += 1
            else:
                by_layer[layer]['failed'] += 1
        
        total_size = sum(r['size_bytes'] for r in results if r['success'])
        
        for layer, stats in sorted(by_layer.items()):
            print(f"\n{layer.upper()} LAYER:")
            print(f"  ✓ Downloaded: {stats['success']} ({stats['skipped']} cached)")
            print(f"  ✗ Failed: {stats['failed']}")
            print(f"  Size: {self._format_size(stats['size'])}")
        
        print(f"\nTOTAL SIZE: {self._format_size(total_size)}")
        
        # List any failures
        failures = [r for r in results if not r['success']]
        if failures:
            print("\n⚠ FAILED DOWNLOADS:")
            for r in failures:
                print(f"  - {r['name']}: {r['error']}")
        
        # Remind about manual downloads
        print("\n" + "-" * 60)
        print("MANUAL DOWNLOADS REQUIRED:")
        print("-" * 60)
        print("""
1. LOINC Core Table (Free registration required)
   → https://loinc.org/downloads/
   → Download: "LOINC Table Core" (LoincTableCore.zip)
   → Place in: ./raw_data/
   
2. LOINC Top 2000 Lab Observations (Same account)
   → Download: "LOINC Top 2000+ Lab Observations"
   → Place in: ./raw_data/
   
3. ARUP Lab Test Directory (Optional - for additional mappings)
   → https://www.aruplab.com/testing
   → Export test catalog
   → Place in: ./raw_data/
""")
    
    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format bytes as human-readable string."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"
    
    def verify_downloads(self) -> Dict[str, bool]:
        """Verify all expected files exist."""
        status = {}
        for source in DATA_SOURCES:
            filepath = self.data_dir / source.filename
            exists = filepath.exists()
            status[source.name] = exists
            
            # Check for extracted content if ZIP
            if source.extract_zip and exists:
                extract_dir = self.data_dir / filepath.stem
                status[f"{source.name} (extracted)"] = extract_dir.exists()
        
        return status


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Run the downloader."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Download Clinical Rosetta Stone data sources")
    parser.add_argument('--layer', choices=['identity', 'statistical', 'knowledge', 'severity'],
                        help='Download only specific layer')
    parser.add_argument('--verify', action='store_true', help='Verify existing downloads')
    parser.add_argument('--parallel', action='store_true', help='Use parallel downloads')
    parser.add_argument('--workers', '-w', type=int, default=DEFAULT_WORKERS,
                       help=f'Number of parallel workers (default: {DEFAULT_WORKERS}, max: {MAX_WORKERS})')
    parser.add_argument('--data-dir', type=Path, default=DATA_DIR, help='Data directory')
    
    args = parser.parse_args()
    
    downloader = RosettaDownloader(args.data_dir)
    
    if args.verify:
        print("Verifying downloads...")
        status = downloader.verify_downloads()
        for name, exists in status.items():
            symbol = "✓" if exists else "✗"
            print(f"  {symbol} {name}")
        return
    
    layers = [args.layer] if args.layer else None
    workers = min(max(1, args.workers), MAX_WORKERS)
    results = downloader.download_all(layers=layers, parallel=args.parallel, workers=workers)
    downloader.print_summary(results)


if __name__ == "__main__":
    main()
