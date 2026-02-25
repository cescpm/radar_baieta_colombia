"""
downloader.py — Lists and downloads IDEAM .RAW files from the S3 bucket.

Uses anonymous (public) access via boto3, exactly as in the original
IDEAM access pattern. Files are cached locally under RAW_DATA_ROOT.
"""

import logging
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
    START_DATE,
    END_DATE,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  S3 client (anonymous / public access — no credentials needed)
# ─────────────────────────────────────────────────────────────────────────────

def _get_s3_client():
    return boto3.client(
        "s3",
        config=Config(
            signature_version=botocore.UNSIGNED,
            user_agent_extra="ideam-radar-pipeline",
        ),
    )


def _get_s3_resource():
    return boto3.resource(
        "s3",
        config=Config(
            signature_version=botocore.UNSIGNED,
            user_agent_extra="ideam-radar-pipeline",
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Key / prefix building — mirrors the original create_query() logic exactly
# ─────────────────────────────────────────────────────────────────────────────

def build_s3_prefix(date: datetime, radar_site: str) -> str:
    """
    Returns the S3 prefix for a given datetime and radar site name.
    The file naming convention observed in the IDEAM bucket:

        l2_data/{YYYY}/{MM}/{DD}/{site}/{SIT}{YY}{MM}{DD}[{HH}[{MM}]]

    where {SIT} is the first 3 characters of the site name, upper-cased.
    """
    site_code = radar_site[:3].upper()

    if date.hour != 0 and date.minute != 0:
        time_suffix = f"{date:%H%M}"
    elif date.hour != 0 and date.minute == 0:
        time_suffix = f"{date:%H}"
    else:
        time_suffix = ""

    prefix = (
        f"{S3_PREFIX}/{date:%Y}/{date:%m}/{date:%d}/"
        f"{radar_site}/{site_code}{date:%y%m%d}{time_suffix}"
    )
    return prefix


def list_s3_files(radar_site: str, date: datetime, s3_resource=None) -> list[str]:
    """
    Lists all S3 keys under the prefix for a given site and date (full day or
    specific hour, depending on the precision of `date`).

    Returns a list of full s3:// URIs.
    """
    if s3_resource is None:
        s3_resource = _get_s3_resource()

    prefix = build_s3_prefix(date, radar_site)
    bucket = s3_resource.Bucket(S3_BUCKET_NAME)

    keys = [
        f"s3://{S3_BUCKET_NAME}/{obj.key}"
        for obj in bucket.objects.filter(Prefix=prefix)
    ]

    logger.debug(f"[{radar_site}] {date:%Y-%m-%d %H:%M} → prefix={prefix} → {len(keys)} files")
    return keys


def discover_all_sites(date: datetime) -> list[str]:
    """
    Auto-discovers which radar sites actually have data for a given date
    by listing the top-level site folders inside l2_data/{YYYY}/{MM}/{DD}/.
    Useful when the list of active sites is uncertain.
    """
    s3 = _get_s3_client()
    prefix = f"{S3_PREFIX}/{date:%Y}/{date:%m}/{date:%d}/"

    response = s3.list_objects_v2(
        Bucket=S3_BUCKET_NAME,
        Prefix=prefix,
        Delimiter="/",
    )

    sites = []
    for cp in response.get("CommonPrefixes", []):
        # cp["Prefix"] looks like "l2_data/2022/10/06/Guaviare/"
        site = cp["Prefix"].rstrip("/").split("/")[-1]
        sites.append(site)

    logger.info(f"Auto-discovered {len(sites)} sites for {date:%Y-%m-%d}: {sites}")
    return sites


# ─────────────────────────────────────────────────────────────────────────────
#  Local path builder
# ─────────────────────────────────────────────────────────────────────────────

def local_raw_path(s3_uri: str, radar_site: str) -> Path:
    """
    Maps an S3 URI to a local file path under RAW_DATA_ROOT.

    S3:  s3://s3-radaresideam/l2_data/2022/10/06/Guaviare/GUA221006.RAW...
    →  RAW_DATA_ROOT/Guaviare/2022/10/06/GUA221006.RAW...
    """
    key = s3_uri.replace(f"s3://{S3_BUCKET_NAME}/", "")
    # key: l2_data/YYYY/MM/DD/Site/filename
    parts = key.split("/")
    # parts: ['l2_data', 'YYYY', 'MM', 'DD', 'Site', 'filename']
    year, month, day, filename = parts[1], parts[2], parts[3], parts[-1]
    return RAW_DATA_ROOT / radar_site / year / month / day / filename


# ─────────────────────────────────────────────────────────────────────────────
#  Downloader
# ─────────────────────────────────────────────────────────────────────────────

def download_file(s3_uri: str, local_path: Path, s3_client=None) -> Path | None:
    """
    Downloads a single file from S3 to local_path.
    Skips if the file already exists (resumable behaviour).
    Returns the local path on success, None on failure.
    """
    if local_path.exists():
        logger.debug(f"Already exists, skipping: {local_path.name}")
        return local_path

    local_path.parent.mkdir(parents=True, exist_ok=True)

    if s3_client is None:
        s3_client = _get_s3_client()

    key = s3_uri.replace(f"s3://{S3_BUCKET_NAME}/", "")

    try:
        logger.info(f"↓ Downloading {s3_uri.split('/')[-1]} ...")
        s3_client.download_file(S3_BUCKET_NAME, key, str(local_path))
        logger.info(f"  ✓ Saved to {local_path}")
        return local_path
    except Exception as exc:
        logger.error(f"  ✗ Failed to download {s3_uri}: {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  Hourly date range generator
# ─────────────────────────────────────────────────────────────────────────────

def hourly_range(start: datetime, end: datetime):
    """Yields datetimes from start to end (inclusive) in hourly steps."""
    current = start
    while current <= end:
        yield current
        current += timedelta(hours=1)


# ─────────────────────────────────────────────────────────────────────────────
#  Main download routine (called by pipeline.py)
# ─────────────────────────────────────────────────────────────────────────────

def download_site_daterange(
    radar_site: str,
    start: datetime = START_DATE,
    end: datetime = END_DATE,
    auto_discover: bool = False,
) -> list[Path]:
    """
    Downloads all .RAW files for a single radar site over a date/time range.

    Parameters
    ----------
    radar_site     : site name as it appears in S3 (e.g. "Guaviare")
    start / end    : datetime range (inclusive, hourly resolution)
    auto_discover  : if True, verify the site exists on each date before listing

    Returns a list of local Paths of successfully downloaded files.
    """
    s3_resource = _get_s3_resource()
    s3_client   = _get_s3_client()

    downloaded_paths = []

    for dt in hourly_range(start, end):
        s3_uris = list_s3_files(radar_site, dt, s3_resource=s3_resource)

        if not s3_uris:
            logger.debug(f"[{radar_site}] No files for {dt:%Y-%m-%d %H:%M}")
            continue

        for uri in s3_uris:
            local_path = local_raw_path(uri, radar_site)
            result = download_file(uri, local_path, s3_client=s3_client)
            if result:
                downloaded_paths.append(result)

    logger.info(
        f"[{radar_site}] Download complete: {len(downloaded_paths)} files "
        f"from {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M}"
    )
    return downloaded_paths
