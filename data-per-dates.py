"""
Inspect the IDEAM S3 bucket for specific dates and shows...

...exactly which sites have data, what the folder names are,
how many files each site has, and what file extensions are present.

    Usage
    -----
        python discover_dates.py 2023-09-22 2025-10-16
        python discover_dates.py 2023-09-22          # single date

    TO be implemented with the already existing pipeline
"""

import argparse
import boto3
import botocore
from botocore.client import Config
from datetime import datetime

S3_BUCKET = "s3-radaresideam"
S3_PREFIX = "l2_data"

def parse_args() -> list[datetime]:
    parser = argparse.ArgumentParser(
        description="Inspect the IDEAM S3 bucket for one or more dates."
    )
    parser.add_argument(
        "dates",
        nargs="*",
        metavar="YYYY-MM-DD",
        help="One or more dates to inspect",
    )
    args = parser.parse_args()

    parsed = []
    for s in args.dates:
        try:
            parsed.append(datetime.strptime(s, "%Y-%m-%d"))
        except ValueError:
            parser.error(f"Invalid date '{s}' — expected format YYYY-MM-DD")
    return parsed

# Stablishes the relationship with the AWS S3 bucket
s3 = boto3.client(
    "s3",
    config=Config(signature_version=botocore.UNSIGNED), # Do not search for credentials
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
            print(page)

    if not files:
        return {"count": 0, "extensions": [], "sample": []}

    extensions = set()
    for f in files:
        name = f.split("/")[-1]
        if ".RAW" in name.upper():
            extensions.add(".RAW*")
        else:
            ext = "." + name.split(".")[-1] if "." in name else "(no ext)"
            extensions.add(ext)

    sample_names = [f.split("/")[-1] for f in files[:3]]

    return {
        "count":      len(files),
        "extensions": sorted(extensions),
        "sample":     sample_names,
    }


def main():
    for date in parse_args():

        date_output = [
            f"\n{'='*60}\n",
            f"  Date: {date:%Y-%m-%d}",
            f"\n{'='*60}\n",
        ]
        print(*date_output)

        sites = list_sites_for_date(date)

        if not sites:
            print("No site folders found for this date.")
            continue

        print(f"  Found {len(sites)} site(s): {sites}\n")

        for site in sites:
            info = inspect_site(date, site)
            if info["count"] == 0:
                print(f"  [{site}]  (empty)")
                continue

            data_output = [
                f"  [{site}]\n",
                f"    Files      : {info['count']}\n",
                f"    Extensions : {', '.join(info['extensions'])}\n",
                f"    Samples    : {info['sample']}\n"
            ]
            print(*data_output)

    print(f"\n{'_'*60}\n")


if __name__ == "__main__":
    main()