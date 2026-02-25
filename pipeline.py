"""
pipeline.py — Orchestrates the full IDEAM radar data pipeline:

    For each radar site × each hour in [START_DATE, END_DATE]:
        1. List available .RAW files on S3
        2. Download them to RAW_DATA_ROOT (skip if already present)
        3. Convert each RAW → HDF5 ODIM (skip if already present)

Parallelism: sites are processed concurrently using a ThreadPoolExecutor.
Within each site, files are processed sequentially to keep memory stable.

Usage
-----
    python pipeline.py                          # uses config.py defaults
    python pipeline.py --sites Guaviare Bogota  # override site list
    python pipeline.py --start 2022-10-06 --end 2022-10-07
    python pipeline.py --discover               # auto-discover sites from S3
    python pipeline.py --download-only          # skip conversion step
    python pipeline.py --convert-only           # skip download, convert existing RAWs
"""

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from config import (
    RADAR_SITES,
    START_DATE,
    END_DATE,
    PARALLEL_WORKERS,
    LOG_FILE,
)
from downloader import download_site_daterange, discover_all_sites
from converter  import convert_files
  
# Logging setup ##############################################################################

def setup_logging(verbose : bool =False):
    """
    Configurates the log output

    -----------
    Parameters:

        verbose : bool =False
    """
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)  # creates/handles properly the ./logs directory to store the logs
    level = logging.DEBUG if verbose else logging.INFO  # determines the level of the log
    fmt   = "%(asctime)s [%(levelname)-8s] %(name)s — %(message)s"  # defines the format of the log 

    logging.basicConfig( 
        level=level,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),  # defines to which file does the stream aims to log (terminal)
            logging.FileHandler(str(LOG_FILE)),  # open the specified filepath and use it as the stream for logging
        ],
    )

logger = logging.getLogger("pipeline")  # creates a logger which inherits the configuration from logging.basicConfig
#---------------------------------------------------------------------------------------------

# Per-site worker ############################################################################

def process_site(radar_site : str, start : datetime, end : datetime, download_only : bool =False, convert_only :  bool =False) -> dict:
    """
    Downloads and converts the data from specific radar site, among a starting and ending date
    Returns a summary dict with counts.

    -----------
    Parameters:

    radar_site : str
        radar's name desired to extract data from

    start : datetime
        start-date for obtaining radar data

    end : datetime
        end-date for obtaining radar data

    download_only : bool =False
        only downloads files
    
    convert_only : bool =False
        only converts files
    
    --------
    Returns:

    summary : dict
        stores the counts which defines the state of the process

    """
    summary = {
        "site":       radar_site,  # name of the radar's site
        "downloaded": 0,  # totally of the files downloaded
        "converted":  0,  # totally of the files converted
        "errors":     0,  # totally of the exception raised
    }

    # ── Download ──────────────────────────────────────────────────────────────
    if not convert_only:
        logger.info(f"══ [{radar_site}] Starting download ══")
        try:
            raw_paths = download_site_daterange(radar_site, start=start, end=end)
            summary["downloaded"] = len(raw_paths)
        except Exception as exc:
            logger.error(f"[{radar_site}] Download error: {exc}", exc_info=True)
            summary["errors"] += 1
            return summary
    else:
        # Convert-only mode: scan the local RAW directory for existing files
        from config import RAW_DATA_ROOT
        raw_dir   = RAW_DATA_ROOT / radar_site
        raw_paths = list(raw_dir.rglob("*.RAW*")) if raw_dir.exists() else []
        logger.info(f"[{radar_site}] Convert-only: found {len(raw_paths)} local RAW files")

    if not raw_paths:
        logger.warning(f"[{radar_site}] No RAW files to process.")
        return summary

    # ── Convert ───────────────────────────────────────────────────────────────
    if not download_only:
        logger.info(f"══ [{radar_site}] Starting conversion ({len(raw_paths)} files) ══")
        try:
            hdf5_paths = convert_files(raw_paths, radar_site)
            summary["converted"] = len(hdf5_paths)
        except Exception as exc:
            logger.error(f"[{radar_site}] Conversion error: {exc}", exc_info=True)
            summary["errors"] += 1

    return summary
#---------------------------------------------------------------------------------------------


# main #######################################################################################

def main():
    """
    Main workflow
    """
    # flags ##########################################

    parser = argparse.ArgumentParser(  # allows creating the flags to call in a command line interface (CLI)
        description="Downloads and stores radar data from colombian IDEAM's network "
        "(.RAW*, .nc, .gz) from an Amazon Web Service (AWS) S3 bucket + converts to " \
        "HDF5-OPERA Data Information Model (ODIM)"
    )
    parser.add_argument(
        "--sites", nargs="+", default=None,
        help="Radar site names to process (default: all in config.py)"
    )
    parser.add_argument(
        "--start", type=str, default=None,
        help="Start datetime ISO format YYYY-MM-DDTHH (default: config.START_DATE)"
    )
    parser.add_argument(
        "--end", type=str, default=None,
        help="End datetime ISO format YYYY-MM-DDTHH (default: config.END_DATE)"
    )
    parser.add_argument(
        "--discover", action="store_true",
        help="Auto-discover available radar sites from S3 for the START_DATE"
    )
    parser.add_argument(
        "--download-only", action="store_true",
        help="Only download RAW files, skip HDF5 conversion"
    )
    parser.add_argument(
        "--convert-only", action="store_true",
        help="Only convert already-downloaded RAW files, skip S3 download"
    )
    parser.add_argument(
        "--workers", type=int, default=PARALLEL_WORKERS,
        help=f"Number of parallel site workers (default: {PARALLEL_WORKERS})"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable DEBUG logging"
    )
    args = parser.parse_args()
    #-------------------------------------------------

    setup_logging(verbose=args.verbose)

    # ── Resolve parameters ────────────────────────────────────────────────────
    start = datetime.fromisoformat(args.start) if args.start else START_DATE
    end   = datetime.fromisoformat(args.end)   if args.end   else END_DATE

    if args.discover:
        sites = discover_all_sites(start)
        if not sites:
            logger.error("Auto-discovery returned no sites. Check your date or S3 access.")
            sys.exit(1)
    elif args.sites:
        sites = args.sites
    else:
        sites = RADAR_SITES

    logger.info("=" * 60)
    logger.info("  IDEAM RADAR PIPELINE")
    logger.info(f"  Sites    : {len(sites)} → {sites}")
    logger.info(f"  Range    : {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}")
    logger.info(f"  Workers  : {args.workers}")
    logger.info(f"  Mode     : {'download-only' if args.download_only else 'convert-only' if args.convert_only else 'download + convert'}")
    logger.info("=" * 60)

    # ── Parallel site processing ───────────────────────────────────────────────
    summaries = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                process_site,
                site, start, end,
                download_only=args.download_only,
                convert_only=args.convert_only,
            ): site
            for site in sites
        }
        for future in as_completed(futures):
            site = futures[future]
            try:
                summary = future.result()
                summaries.append(summary)
            except Exception as exc:
                logger.error(f"[{site}] Unhandled exception: {exc}", exc_info=True)
                summaries.append({"site": site, "downloaded": 0, "converted": 0, "errors": 1})

    # ── Final report ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("  PIPELINE COMPLETE — SUMMARY")
    logger.info("=" * 60)
    total_dl  = sum(s["downloaded"] for s in summaries)
    total_hdf = sum(s["converted"]  for s in summaries)
    total_err = sum(s["errors"]     for s in summaries)

    for s in sorted(summaries, key=lambda x: x["site"]):
        logger.info(
            f"  {s['site']:<22} ↓ {s['downloaded']:>4} RAW   "
            f"→ {s['converted']:>4} HDF5   ✗ {s['errors']} errors"
        )

    logger.info("-" * 60)
    logger.info(f"  TOTAL: {total_dl} downloaded, {total_hdf} converted, {total_err} errors")
    logger.info("=" * 60)
#---------------------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
