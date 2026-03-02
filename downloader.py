"""
downloader.py — Lists and downloads IDEAM .RAW, .nc, or .nc.gz files from S3.

Key behaviours
--------------
* Prefix strategy:
    - IRIS sites and standard NetCDF sites whose filenames begin with
      {SIT}{YY}{MM}{DD} use the narrowed filename-prefix query (fast, few
      results per call).
    - Sites listed in FOLDER_LEVEL_SITES (e.g. Bogota, santa_elena) use a
      folder-level prefix — l2_data/YYYY/MM/DD/site/ — because their
      filenames don't embed the site code + date and therefore can't be
      narrowed further.

* Midnight duplication fix:
    Full-day ranges issue one day-level query per calendar day.
    Partial-day ranges issue per-hour queries; hour=0 is always treated as
    day-level.  All collected URIs are deduped in a set before downloading.

* .nc.gz decompression:
    After downloading a .gz file the pipeline decompresses it in-place,
    leaving only the plain .nc file on disk.  The converter therefore always
    receives a plain .nc and needs no special handling.

* Availability cache:
    RAW_DATA_ROOT/.site_availability.json records (site, date) → True/False.
    Empty pairs are skipped on subsequent runs.  Pass force_refresh=True to
    bypass.
"""

import gzip
import json
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import botocore
from botocore.client import Config

from config import (
    S3_BUCKET_NAME,
    S3_PREFIX,
    RAW_DATA_ROOT,
    RADAR_SITES,
    RADAR_FORMAT,
    FOLDER_LEVEL_SITES,
    START_DATE,
    END_DATE,
)

logger = logging.getLogger(__name__)

_CACHE_FILE = RAW_DATA_ROOT / ".site_availability.json"


# Low-level client creator ###################################################################
def _get_s3_client():
    """
    Creates a low-level client with the AWS S3 bucket
    """
    return boto3.client(  # Stablishes a low-level connection with the AWS S3 bucket
        "s3",
        config=Config(signature_version=botocore.UNSIGNED,
                      user_agent_extra="ideam-radar-pipeline"),
    )
#---------------------------------------------------------------------------------------------

# High-level client creator ##################################################################
def _get_s3_resource():
    """
    Creates a high-level client with the AWS S3 bucket
    """
    return boto3.resource(  # Stablishes a high-level connection with the AWS S3 bucket
        "s3",
        config=Config(signature_version=botocore.UNSIGNED,
                      user_agent_extra="ideam-radar-pipeline"),
    )
#---------------------------------------------------------------------------------------------

# Format helpers #############################################################################

def get_site_format(radar_site : str) -> str:
    """
    Searches, in dict (from config.py file) the format in which data is stored for the 
    specified radar_site. 

    -----------
    Parameters:

    radar_site : str
        desired radar's name to extract data from
    """
    return RADAR_FORMAT.get(radar_site, "iris")


def raw_glob_pattern(radar_site: str) -> str:
    """
    Glob pattern for scanning local directories for already-downloaded files.
    Note: netcdf_gz sites are stored as .nc after decompression, same as netcdf.

    -----------
    Parameters:

    radar_site : str
        desired radar's name to extract data from

    """
    fmt = get_site_format(radar_site)
    if fmt in ("netcdf", "netcdf_gz"):
        return "*.nc"
    return "*.RAW*"


def uses_folder_prefix(radar_site: str) -> bool:
    """
    Returns True if this site's files must be listed at the folder level
    (i.e. their filenames don't follow the {SIT}{YY}{MM}{DD} prefix convention).
    """
    return radar_site in FOLDER_LEVEL_SITES
#---------------------------------------------------------------------------------------------

# Cache handler ##############################################################################
# Improves the latency of the data request

def _load_cache() -> dict:
    """
    Loads, if exists, the cache (.json) into a dict.

    --------
    Returns:

     : dict
        Stores the cahe-related data
    """
    if _CACHE_FILE.exists():
        try:
            return json.loads(_CACHE_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save_cache(cache : dict) -> None:
    """
    Ensures a proper cache-saving method.

    -----------
    Parameters:

    cache : dict
        contains the cache data
    """
    _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _CACHE_FILE.write_text(json.dumps(cache, indent=2, sort_keys=True))


def _cache_key(radar_site : str, date : datetime) -> str:
    """
    Defines how the cache key is constructed {name of the radar}|{ISO 8601-format date}.

    -----------
    Parameters:

    radar_site : str
        name of the site where the radar is located

    date : datetime
        mesure date of the data requested

    --------
    Returns:

     : str
        key for each entrance in the dictionary
    """
    return f"{radar_site}|{date:%Y-%m-%d}"


def mark_site_date(radar_site : str, date : datetime, has_data : bool) -> None:
    """
    Handles the cache (loading/building/saving...)

    -----------
    Parameters:

    radar_site : str
        name of the site where the radar is located

    date : datetime
        mesure date of the data requested

    has_data : bool
        Whether if the S3 bucket contains data for the specified radar_site and date
    """

    cache = _load_cache()
    cache[_cache_key(radar_site, date)] = has_data
    _save_cache(cache)


def site_date_cached(radar_site: str, date: datetime) -> bool | None:
    """Returns True/False if cached, None if unknown (must query S3)."""
    return _load_cache().get(_cache_key(radar_site, date))
#---------------------------------------------------------------------------------------------

# ─────────────────────────────────────────────────────────────────────────────
#  S3 prefix builder
# ─────────────────────────────────────────────────────────────────────────────

def build_s3_prefix(date: datetime, radar_site: str) -> str:
    """
    Returns the S3 prefix for a given datetime and site.

    For FOLDER_LEVEL_SITES the prefix always ends at the site folder,
    regardless of the time component, because the filenames don't embed
    the date/time in a queryable prefix form.

    For all other sites:
        hour=0, minute=0  → day-level  (no time suffix)
        hour!=0, minute=0 → hour-level
        hour!=0, minute!=0 → minute-level
    """
    day_prefix = (
        f"{S3_PREFIX}/{date:%Y}/{date:%m}/{date:%d}/{radar_site}/"
    )

    if uses_folder_prefix(radar_site):
        return day_prefix

    site_code = radar_site[:3].upper()
    if date.hour != 0 and date.minute != 0:
        time_suffix = f"{date:%H%M}"
    elif date.hour != 0:
        time_suffix = f"{date:%H}"
    else:
        time_suffix = ""

    return f"{day_prefix}{site_code}{date:%y%m%d}{time_suffix}"


# ─────────────────────────────────────────────────────────────────────────────
#  S3 listing
# ─────────────────────────────────────────────────────────────────────────────

def list_s3_files(radar_site: str, date: datetime, s3_resource=None) -> list[str]:
    """Lists all S3 keys under the prefix for (site, date). Returns s3:// URIs."""
    if s3_resource is None:
        s3_resource = _get_s3_resource()

    prefix = build_s3_prefix(date, radar_site)
    bucket = s3_resource.Bucket(S3_BUCKET_NAME)
    keys = [
        f"s3://{S3_BUCKET_NAME}/{obj.key}"
        for obj in bucket.objects.filter(Prefix=prefix)
    ]
    logger.debug(
        f"[{radar_site}] {date:%Y-%m-%d %H:%M} → prefix={prefix} → {len(keys)} files"
    )
    return keys


def discover_all_sites(date: datetime) -> list[str]:
    """
    Auto-discovers radar sites with data for a given date by listing
    top-level site folders in l2_data/{YYYY}/{MM}/{DD}/.
    """
    s3 = _get_s3_client()
    prefix = f"{S3_PREFIX}/{date:%Y}/{date:%m}/{date:%d}/"
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix, Delimiter="/")
    sites = [
        cp["Prefix"].rstrip("/").split("/")[-1]
        for cp in response.get("CommonPrefixes", [])
    ]
    logger.info(f"Auto-discovered {len(sites)} sites for {date:%Y-%m-%d}: {sites}")
    return sites


# ─────────────────────────────────────────────────────────────────────────────
#  Local path builder
# ─────────────────────────────────────────────────────────────────────────────

def local_raw_path(s3_uri: str, radar_site: str) -> Path:
    """
    Maps an S3 URI → local path under RAW_DATA_ROOT, preserving the original
    filename and extension (.RAWxxxx, .nc, or .nc.gz).

    s3://bucket/l2_data/2023/09/22/Bogota/1399BOG-20230922-022931-PPIVol-9296.nc.gz
    →  RAW_DATA_ROOT/Bogota/2023/09/22/1399BOG-20230922-022931-PPIVol-9296.nc.gz
    """
    key = s3_uri.replace(f"s3://{S3_BUCKET_NAME}/", "")
    parts = key.split("/")   # ['l2_data', 'YYYY', 'MM', 'DD', 'Site', 'filename']
    year, month, day, filename = parts[1], parts[2], parts[3], parts[-1]
    return RAW_DATA_ROOT / radar_site / year / month / day / filename


# ─────────────────────────────────────────────────────────────────────────────
#  Decompression
# ─────────────────────────────────────────────────────────────────────────────

def decompress_gz(gz_path: Path) -> Path | None:
    """
    Decompresses a .gz file to the same directory, then deletes the .gz.
    Returns the path of the decompressed file, or None on failure.

    1399BOG-20230922-022931-PPIVol-9296.nc.gz
    →  1399BOG-20230922-022931-PPIVol-9296.nc
    """
    # Strip the final .gz suffix to get the output path
    if gz_path.suffix != ".gz":
        logger.warning(f"decompress_gz called on non-.gz file: {gz_path.name}")
        return gz_path

    out_path = gz_path.with_suffix("")   # removes .gz, e.g. .nc.gz → .nc

    if out_path.exists():
        logger.debug(f"Already decompressed, skipping: {out_path.name}")
        gz_path.unlink()
        return out_path

    try:
        logger.info(f"  ⊡ Decompressing {gz_path.name} ...")
        with gzip.open(gz_path, "rb") as f_in, open(out_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        gz_path.unlink()
        logger.info(f"  ✓ Decompressed → {out_path.name}")
        return out_path
    except Exception as exc:
        logger.error(f"  ✗ Decompression failed for {gz_path.name}: {exc}")
        if out_path.exists():
            out_path.unlink()
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  Single-file downloader
# ─────────────────────────────────────────────────────────────────────────────

def download_file(s3_uri: str, local_path: Path, s3_client=None) -> Path | None:
    """
    Downloads one file from S3.

    For .nc.gz files: if the decompressed .nc already exists locally the
    download is skipped entirely (no need to re-download the compressed copy).
    After downloading a .gz the file is decompressed immediately.

    Returns the final local path (.nc for gz files, original path otherwise),
    or None on failure.
    """
    is_gz = local_path.suffix == ".gz"
    final_path = local_path.with_suffix("") if is_gz else local_path

    # Skip if the final (possibly decompressed) file already exists
    if final_path.exists():
        logger.debug(f"Already exists, skipping: {final_path.name}")
        return final_path

    # Also skip if the .gz itself is already downloaded but not yet decompressed
    if is_gz and local_path.exists():
        return decompress_gz(local_path)

    local_path.parent.mkdir(parents=True, exist_ok=True)

    if s3_client is None:
        s3_client = _get_s3_client()

    key = s3_uri.replace(f"s3://{S3_BUCKET_NAME}/", "")
    try:
        logger.info(f"↓ Downloading {s3_uri.split('/')[-1]} ...")
        s3_client.download_file(S3_BUCKET_NAME, key, str(local_path))
        logger.info(f"  ✓ Saved to {local_path}")
    except Exception as exc:
        logger.error(f"  ✗ Failed to download {s3_uri}: {exc}")
        return None

    # Decompress immediately if this was a .gz file
    if is_gz:
        return decompress_gz(local_path)

    return local_path


# ─────────────────────────────────────────────────────────────────────────────
#  Date-range helpers
# ─────────────────────────────────────────────────────────────────────────────

def daily_range(start: datetime, end: datetime):
    """Yields midnight datetimes from start-day to end-day inclusive."""
    current = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = end.replace(  hour=0, minute=0, second=0, microsecond=0)
    while current <= end_day:
        yield current
        current += timedelta(days=1)


def hourly_range(start: datetime, end: datetime):
    """Yields datetimes from start to end inclusive in hourly steps."""
    current = start
    while current <= end:
        yield current
        current += timedelta(hours=1)


# ─────────────────────────────────────────────────────────────────────────────
#  Main download routine
# ─────────────────────────────────────────────────────────────────────────────

def download_site_daterange(radar_site : str, start : datetime =START_DATE, end : datetime =END_DATE, force_refresh : bool =False) -> list[Path]:
    """
    Downloads all files for a single radar site over a date/time range.

    For FOLDER_LEVEL_SITES a single folder-level query per calendar day is
    always used (narrowing by hour is not possible since filenames don't
    embed the hour).  For all other sites the same day-vs-hour strategy
    from before applies.

    Returns a list of local Paths (always .nc for gz sites, never .gz).
    """
    fmt = get_site_format(radar_site)
    logger.info(f"[{radar_site}] Format: {fmt}")

    s3_resource = _get_s3_resource()
    s3_client   = _get_s3_client()

    all_uris: set[str] = set()

    # FOLDER_LEVEL_SITES must always use day-level queries
    force_day_level = uses_folder_prefix(radar_site)

    start_day    = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day      = end.replace(  hour=0, minute=0, second=0, microsecond=0)
    spans_full_day = (start.hour == 0 and end.hour >= 23) or (end_day > start_day)

    if force_day_level or spans_full_day:
        query_dates  = list(daily_range(start, end))
        is_day_level = [True] * len(query_dates)
    else:
        query_dates  = list(hourly_range(start, end))
        is_day_level = [dt.hour == 0 for dt in query_dates]

    for dt, is_day in zip(query_dates, is_day_level):
        day_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)

        if not force_refresh and is_day:
            cached = site_date_cached(radar_site, day_dt)
            if cached is False:
                logger.debug(
                    f"[{radar_site}] {day_dt:%Y-%m-%d} skipped (cached: no data)"
                )
                continue

        uris = list_s3_files(radar_site, dt, s3_resource=s3_resource)

        if not uris:
            if is_day:
                mark_site_date(radar_site, day_dt, False)
            logger.debug(f"[{radar_site}] No files for {dt:%Y-%m-%d %H:%M}")
            continue

        if is_day:
            mark_site_date(radar_site, day_dt, True)

        all_uris.update(uris)

    if not all_uris:
        logger.info(
            f"[{radar_site}] No files found "
            f"({start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M})"
        )
        return []

    logger.info(f"[{radar_site}] {len(all_uris)} unique files to download")

    downloaded_paths: list[Path] = []
    for uri in sorted(all_uris):
        local_path = local_raw_path(uri, radar_site)
        result = download_file(uri, local_path, s3_client=s3_client)
        if result:
            downloaded_paths.append(result)

    logger.info(
        f"[{radar_site}] Download complete: {len(downloaded_paths)} files "
        f"({start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M})"
    )
    return downloaded_paths