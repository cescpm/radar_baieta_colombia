"""
discover_dates.py — Inspect the IDEAM S3 bucket for specific dates.

Shows exactly which sites have data, what the folder names are,
how many files each site has, and what file extensions are present.

Usage
-----
    python discover_dates.py 2023-09-22 2025-10-16
    python discover_dates.py 2023-09-22          # single date
"""

import argparse
import boto3
import botocore
from botocore.client import Config
from datetime import datetime

S3_BUCKET = "s3-radaresideam"
S3_PREFIX = "l2_data"

# Fallback dates used when none are passed on the command line
DEFAULT_DATES = [
    datetime(2023, 9, 22),
    datetime(2025, 10, 16),
]


def parse_args() -> list[datetime]:
    parser = argparse.ArgumentParser(
        description="Inspect the IDEAM S3 bucket for one or more dates."
    )
    parser.add_argument(
        "dates",
        nargs="*",
        metavar="YYYY-MM-DD",
        help="One or more dates to inspect (default: hardcoded list in script)",
    )
    args = parser.parse_args()

    if not args.dates:
        return DEFAULT_DATES

    parsed = []
    for s in args.dates:
        try:
            parsed.append(datetime.strptime(s, "%Y-%m-%d"))
        except ValueError:
            parser.error(f"Invalid date '{s}' — expected format YYYY-MM-DD")
    return parsed


s3 = boto3.client(
    "s3",
    config=Config(signature_version=botocore.UNSIGNED),
)


def list_sites_for_date(date: datetime) -> list[str]:
    """Returns the site folder names that exist under l2_data/YYYY/MM/DD/."""
    prefix = f"{S3_PREFIX}/{date:%Y}/{date:%m}/{date:%d}/"
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=prefix,
        Delimiter="/",
    )
    return [
        cp["Prefix"].rstrip("/").split("/")[-1]
        for cp in response.get("CommonPrefixes", [])
    ]


def inspect_site(date: datetime, site: str) -> dict:
    """
    Lists all files for a site on a given date.
    Returns a summary: file count, extensions present, first/last filename.
    """
    prefix = f"{S3_PREFIX}/{date:%Y}/{date:%m}/{date:%d}/{site}/"
    paginator = s3.get_paginator("list_objects_v2")

    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            files.append(obj["key"] if "key" in obj else obj["Key"])

    if not files:
        return {"count": 0, "extensions": [], "sample": []}

    # Collect unique extensions
    extensions = set()
    for f in files:
        name = f.split("/")[-1]
        # RAW files have multi-part extensions like .RAW1A3B — capture the type
        if ".RAW" in name.upper():
            extensions.add(".RAW*")
        else:
            ext = "." + name.split(".")[-1] if "." in name else "(no ext)"
            extensions.add(ext)

    # Show a few sample filenames
    sample_names = [f.split("/")[-1] for f in files[:3]]

    return {
        "count":      len(files),
        "extensions": sorted(extensions),
        "sample":     sample_names,
    }


def main():
    for date in parse_args():
        print(f"\n{'='*60}")
        print(f"  Date: {date:%Y-%m-%d}")
        print(f"{'='*60}")

        sites = list_sites_for_date(date)

        if not sites:
            print("  ✗ No site folders found for this date.")
            continue

        print(f"  Found {len(sites)} site(s): {sites}\n")

        for site in sites:
            info = inspect_site(date, site)
            if info["count"] == 0:
                print(f"  [{site}]  (empty)")
                continue

            print(f"  [{site}]")
            print(f"    Files      : {info['count']}")
            print(f"    Extensions : {', '.join(info['extensions'])}")
            print(f"    Samples    : {info['sample']}")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()