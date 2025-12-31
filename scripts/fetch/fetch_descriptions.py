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

import argparse
import asyncio
import json
import logging
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from html import unescape
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Project root is two levels up from scripts/fetch/
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "clinical_rosetta.db"
PROGRESS_FILE = PROJECT_ROOT / ".fetch_progress.json"
CACHE_DIR = PROJECT_ROOT / "raw_data" / "medlineplus_cache"

# MedlinePlus Connect API
MEDLINEPLUS_URL = "https://connect.medlineplus.gov/service"
LOINC_OID = "2.16.840.1.113883.6.1"

# Rate limiting: 100 requests per minute = ~0.6 sec between requests
REQUEST_DELAY = 0.7  # seconds between requests (for sequential mode)
BATCH_SIZE = 50  # commit to DB every N fetches

# Parallel processing defaults
DEFAULT_WORKERS = 5  # Concurrent requests (conservative for API limits)
MAX_WORKERS = 10  # Maximum allowed workers


def strip_html(text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def load_progress() -> dict:
    """Load progress from file."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"fetched": [], "no_result": [], "errors": [], "last_run": None}


def save_progress(progress: dict):
    """Save progress to file."""
    progress["last_run"] = datetime.now().isoformat()
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def save_to_cache(loinc_code: str, data: dict):
    """Save raw API response to cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{loinc_code}.json"
    with open(cache_file, "w") as f:
        json.dump(data, f, indent=2)


def load_from_cache(loinc_code: str) -> dict:
    """Load cached API response if exists."""
    cache_file = CACHE_DIR / f"{loinc_code}.json"
    if cache_file.exists():
        with open(cache_file, "r") as f:
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
        "mainSearchCriteria.v.cs": LOINC_OID,
        "mainSearchCriteria.v.c": loinc_code,
        "knowledgeResponseType": "application/json",
    }

    try:
        response = requests.get(MEDLINEPLUS_URL, params=params, timeout=30)

        if response.status_code != 200:
            return {"error": f"HTTP {response.status_code}"}

        data = response.json()

        # Save raw payload to cache
        save_to_cache(loinc_code, data)

        return parse_medlineplus_response(data)

    except requests.exceptions.Timeout:
        return {"error": "timeout"}
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": str(e)}


def parse_medlineplus_response(data: dict, from_cache: bool = False) -> dict:
    """Parse MedlinePlus API response."""
    feed = data.get("feed", {})
    entries = feed.get("entry", [])

    if not entries:
        return {"no_result": True, "from_cache": from_cache}

    entry = entries[0]
    title = entry.get("title", {}).get("_value", "")

    # Extract summary
    summary_data = entry.get("summary", {})
    summary = ""
    if isinstance(summary_data, dict):
        summary = summary_data.get("_value", "")
    elif isinstance(summary_data, list):
        for s in summary_data:
            if isinstance(s, dict) and "_value" in s:
                summary = s.get("_value", "")
                break

    if not summary:
        return {"no_result": True, "from_cache": from_cache}

    # Clean HTML
    clean_summary = strip_html(summary)

    return {
        "title": title,
        "description": clean_summary,
        "url": f"https://medlineplus.gov/lab-tests/{title.lower().replace(' ', '-')}/",
        "from_cache": from_cache,
    }


def get_codes_to_fetch(conn: sqlite3.Connection, progress: dict, limit: int = None) -> list:
    """Get LOINC codes that need descriptions fetched."""

    # Get all lab-related LOINC codes
    cursor = conn.execute(
        """
        SELECT loinc_code, long_common_name 
        FROM loinc_concept 
        WHERE status = 'ACTIVE'
        ORDER BY loinc_code
    """
    )
    all_codes = [(r[0], r[1]) for r in cursor.fetchall()]

    # Filter out already fetched (in DB)
    cursor = conn.execute(
        """
        SELECT DISTINCT loinc_code FROM concept_description 
        WHERE description_type = 'consumer'
    """
    )
    in_db = set(r[0] for r in cursor.fetchall())

    # Filter out known no-results and errors from progress
    skip = set(progress.get("no_result", []) + progress.get("fetched", []))

    # Get codes to fetch
    to_fetch = [(code, name) for code, name in all_codes if code not in in_db and code not in skip]

    if limit:
        to_fetch = to_fetch[:limit]

    return to_fetch


def run_fetcher(limit: int = None, resume: bool = True, workers: int = 1):
    """Main fetcher loop with optional parallel processing."""
    conn = sqlite3.connect(str(DB_PATH))

    # Load or reset progress
    if resume:
        progress = load_progress()
        logger.info(
            f"Resuming from previous run. Already processed: {len(progress.get('fetched', []))} fetched, {len(progress.get('no_result', []))} no results"
        )
    else:
        progress = {"fetched": [], "no_result": [], "errors": [], "last_run": None}

    # Get codes to fetch
    codes_to_fetch = get_codes_to_fetch(conn, progress, limit)

    if not codes_to_fetch:
        logger.info("No codes to fetch - all done!")
        conn.close()
        return

    # Use parallel mode if workers > 1
    if workers > 1:
        conn.close()
        run_fetcher_parallel(codes_to_fetch, progress, workers)
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

            if "description" in result:
                # Success - save to DB
                try:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO concept_description 
                        (loinc_code, source, language, description_type, description_text, 
                         source_url, retrieved_date)
                        VALUES (?, ?, ?, ?, ?, ?, date('now'))
                    """,
                        (
                            loinc_code,
                            "MedlinePlus",
                            "en",
                            "consumer",
                            result["description"][:2000],
                            result.get("url", ""),
                        ),
                    )
                    fetched_count += 1
                    batch_count += 1
                    progress["fetched"].append(loinc_code)
                    logger.info(
                        f"[{i+1}/{len(codes_to_fetch)}] ✓ {loinc_code}: {result['title'][:40]}"
                    )
                except Exception as e:
                    logger.error(f"[{i+1}/{len(codes_to_fetch)}] DB error for {loinc_code}: {e}")
                    error_count += 1

            elif result.get("no_result"):
                no_result_count += 1
                progress["no_result"].append(loinc_code)
                if i % 20 == 0:  # Log every 20th no-result
                    logger.info(
                        f"[{i+1}/{len(codes_to_fetch)}] - {loinc_code}: no MedlinePlus entry"
                    )

            else:
                error_count += 1
                progress["errors"].append(
                    {"code": loinc_code, "error": result.get("error", "unknown")}
                )
                logger.warning(
                    f"[{i+1}/{len(codes_to_fetch)}] ✗ {loinc_code}: {result.get('error')}"
                )

            # Batch commit
            if batch_count >= BATCH_SIZE:
                conn.commit()
                save_progress(progress)
                batch_count = 0

                # Progress summary
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed * 60
                remaining = (len(codes_to_fetch) - i - 1) / rate if rate > 0 else 0
                logger.info(
                    f"Progress: {i+1}/{len(codes_to_fetch)} | Rate: {rate:.0f}/min | ETA: {remaining:.1f} min"
                )

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


def run_fetcher_parallel(codes_to_fetch: list, progress: dict, workers: int):
    """Parallel fetcher using asyncio with semaphore-controlled concurrency."""
    workers = min(workers, MAX_WORKERS)
    logger.info(
        f"Fetching descriptions for {len(codes_to_fetch)} LOINC codes with {workers} workers..."
    )

    # Estimate time (parallel is faster but still rate-limited per worker)
    effective_rate = workers / REQUEST_DELAY  # requests per second
    est_minutes = len(codes_to_fetch) / effective_rate / 60
    logger.info(f"Estimated time: {est_minutes:.1f} minutes (parallel mode)")

    # Shared state for tracking
    state = {
        "fetched_count": 0,
        "no_result_count": 0,
        "error_count": 0,
        "processed": 0,
        "total": len(codes_to_fetch),
        "start_time": time.time(),
        "progress": progress,
        "lock": asyncio.Lock(),
    }

    async def fetch_single(
        loinc_code: str, name: str, semaphore: asyncio.Semaphore, executor: ThreadPoolExecutor
    ):
        """Fetch a single code with semaphore control."""
        async with semaphore:
            loop = asyncio.get_event_loop()
            # Run blocking request in thread pool
            result = await loop.run_in_executor(executor, fetch_description, loinc_code)

            # Rate limiting per worker
            await asyncio.sleep(REQUEST_DELAY)

            return (loinc_code, result)

    async def process_results(results: list, conn: sqlite3.Connection):
        """Process batch of results and update DB."""
        async with state["lock"]:
            for loinc_code, result in results:
                state["processed"] += 1

                if "description" in result:
                    try:
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO concept_description 
                            (loinc_code, source, language, description_type, description_text, 
                             source_url, retrieved_date)
                            VALUES (?, ?, ?, ?, ?, ?, date('now'))
                        """,
                            (
                                loinc_code,
                                "MedlinePlus",
                                "en",
                                "consumer",
                                result["description"][:2000],
                                result.get("url", ""),
                            ),
                        )
                        state["fetched_count"] += 1
                        state["progress"]["fetched"].append(loinc_code)
                    except Exception as e:
                        logger.error(f"DB error for {loinc_code}: {e}")
                        state["error_count"] += 1

                elif result.get("no_result"):
                    state["no_result_count"] += 1
                    state["progress"]["no_result"].append(loinc_code)
                else:
                    state["error_count"] += 1
                    state["progress"]["errors"].append(
                        {"code": loinc_code, "error": result.get("error", "unknown")}
                    )

            conn.commit()
            save_progress(state["progress"])

            # Progress log
            elapsed = time.time() - state["start_time"]
            rate = state["processed"] / elapsed * 60 if elapsed > 0 else 0
            remaining = (state["total"] - state["processed"]) / rate if rate > 0 else 0
            logger.info(
                f"Progress: {state['processed']}/{state['total']} | "
                f"✓{state['fetched_count']} -{state['no_result_count']} ✗{state['error_count']} | "
                f"Rate: {rate:.0f}/min | ETA: {remaining:.1f} min"
            )

    async def run_all():
        """Main async entry point."""
        semaphore = asyncio.Semaphore(workers)
        conn = sqlite3.connect(str(DB_PATH))

        try:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                # Process in batches to manage memory and provide progress
                batch_size = BATCH_SIZE
                for batch_start in range(0, len(codes_to_fetch), batch_size):
                    batch = codes_to_fetch[batch_start : batch_start + batch_size]

                    # Create tasks for this batch
                    tasks = [fetch_single(code, name, semaphore, executor) for code, name in batch]

                    # Run batch with exception handling
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Filter out exceptions and process
                    valid_results = []
                    for i, r in enumerate(results):
                        if isinstance(r, Exception):
                            code = batch[i][0]
                            logger.warning(f"Exception for {code}: {r}")
                            state["error_count"] += 1
                            state["progress"]["errors"].append({"code": code, "error": str(r)})
                        else:
                            valid_results.append(r)

                    await process_results(valid_results, conn)

        except KeyboardInterrupt:
            logger.info("\nInterrupted by user. Saving progress...")
        finally:
            conn.commit()
            save_progress(state["progress"])
            conn.close()

    # Run the async code
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        pass

    # Summary
    elapsed = time.time() - state["start_time"]
    print("\n" + "=" * 50)
    print("FETCH COMPLETE (PARALLEL MODE)")
    print("=" * 50)
    print(f"  Workers: {workers}")
    print(f"  Fetched: {state['fetched_count']}")
    print(f"  No result: {state['no_result_count']}")
    print(f"  Errors: {state['error_count']}")
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

        with open(cache_file, "r") as f:
            data = json.load(f)

        result = parse_medlineplus_response(data)

        if "description" in result:
            try:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO concept_description 
                    (loinc_code, source, language, description_type, description_text, 
                     source_url, retrieved_date)
                    VALUES (?, ?, ?, ?, ?, ?, date('now'))
                """,
                    (
                        loinc_code,
                        "MedlinePlus",
                        "en",
                        "consumer",
                        result["description"][:2000],
                        result.get("url", ""),
                    ),
                )
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

    cursor = conn.execute(
        'SELECT COUNT(DISTINCT loinc_code) FROM concept_description WHERE description_type = "consumer"'
    )
    in_db = cursor.fetchone()[0]

    no_result = len(progress.get("no_result", []))

    # Count cached files
    cached = len(list(CACHE_DIR.glob("*.json"))) if CACHE_DIR.exists() else 0

    print("DESCRIPTION FETCH STATUS")
    print("=" * 40)
    print(f"  Total LOINC codes: {total:,}")
    print(f"  With descriptions: {in_db:,}")
    print(f"  Cached responses: {cached:,}")
    print(f"  Known no-result: {no_result:,}")
    print(f"  Remaining to try: {total - in_db - no_result:,}")

    if progress.get("last_run"):
        print(f"  Last run: {progress['last_run']}")

    if cached > 0:
        cache_size = sum(f.stat().st_size for f in CACHE_DIR.glob("*.json"))
        print(f"  Cache size: {cache_size / 1024 / 1024:.1f} MB")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Fetch MedlinePlus descriptions for LOINC codes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  python fetch_descriptions.py              # Fetch all missing (sequential)
  python fetch_descriptions.py --workers 5  # Fetch with 5 parallel workers
  python fetch_descriptions.py --limit 500  # Fetch 500 at a time
  python fetch_descriptions.py --status     # Show current status
  python fetch_descriptions.py --reprocess  # Reprocess cached responses into DB
  python fetch_descriptions.py --reset      # Reset progress and start fresh

Parallel mode:
  Default workers: {DEFAULT_WORKERS}, Max workers: {MAX_WORKERS}
  MedlinePlus allows ~100 req/min, so 5 workers is a safe default.
        """,
    )

    parser.add_argument("--limit", type=int, help="Maximum number of codes to fetch")
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=1,
        help=f"Number of parallel workers (default: 1, max: {MAX_WORKERS})",
    )
    parser.add_argument("--status", action="store_true", help="Show current status")
    parser.add_argument(
        "--reprocess", action="store_true", help="Reprocess cached responses into database"
    )
    parser.add_argument("--reset", action="store_true", help="Reset progress tracking")
    parser.add_argument("--no-resume", action="store_true", help="Do not resume from previous run")
    parser.add_argument("--no-cache", action="store_true", help="Skip cache, always fetch from API")

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
        workers = min(max(1, args.workers), MAX_WORKERS)
        run_fetcher(limit=args.limit, resume=not args.no_resume, workers=workers)


if __name__ == "__main__":
    main()
