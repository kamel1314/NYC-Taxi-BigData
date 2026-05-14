"""
Data Ingestion Script — NYC Taxi Fare Prediction
=================================================
Category : Data Ingestion — API Integration
Course   : CSCI461 Introduction to Big Data (Spring 2026)

This script programmatically downloads all 12 monthly NYC Yellow Taxi
Parquet files for 2024 from the NYC Taxi & Limousine Commission (TLC)
official CloudFront data endpoint.

This satisfies the "API Integration" data ingestion requirement.
Source URL: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""

import os
import sys
import requests
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

BASE_URL   = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEAR       = 2024
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "yellow_2024"

MONTHS = {
    1: "January",   2: "February",  3: "March",     4: "April",
    5: "May",       6: "June",      7: "July",       8: "August",
    9: "September", 10: "October",  11: "November",  12: "December",
}

# ── Download Logic ─────────────────────────────────────────────────────────────

def download_parquet(month: int, dest_dir: Path) -> Path:
    """
    Download one monthly Parquet file from the NYC TLC endpoint.
    Skips the download if the file already exists locally.

    Parameters
    ----------
    month    : int   — month number (1–12)
    dest_dir : Path  — local directory to save the file

    Returns
    -------
    Path to the downloaded (or existing) file.
    """
    filename = f"yellow_tripdata_{YEAR}-{month:02d}.parquet"
    dest     = dest_dir / filename
    url      = f"{BASE_URL}/{filename}"

    if dest.exists():
        size_mb = dest.stat().st_size / (1024 * 1024)
        print(f"  [SKIP]  {filename}  ({size_mb:.1f} MB — already downloaded)")
        return dest

    print(f"  [GET]   {url}")
    try:
        response = requests.get(url, stream=True, timeout=180)
        response.raise_for_status()

        total_bytes   = int(response.headers.get("content-length", 0))
        received      = 0
        chunk_size    = 4 * 1024 * 1024  # 4 MB chunks

        with open(dest, "wb") as fh:
            for chunk in response.iter_content(chunk_size=chunk_size):
                fh.write(chunk)
                received += len(chunk)
                if total_bytes:
                    pct = received / total_bytes * 100
                    print(f"\r          {pct:5.1f}%  ({received / 1e6:.1f} / {total_bytes / 1e6:.1f} MB)", end="", flush=True)

        print()  # newline after progress
        size_mb = dest.stat().st_size / (1024 * 1024)
        print(f"  [DONE]  {filename}  ({size_mb:.1f} MB)\n")

    except requests.HTTPError as e:
        print(f"\n  [ERROR] HTTP {e.response.status_code} — {url}")
        if dest.exists():
            dest.unlink()  # remove partial file
        raise

    except Exception as e:
        print(f"\n  [ERROR] {e}")
        if dest.exists():
            dest.unlink()
        raise

    return dest


def verify_downloads(dest_dir: Path) -> None:
    """Print a summary table of all downloaded files."""
    files = sorted(dest_dir.glob("*.parquet"))
    if not files:
        print("  No files found.")
        return

    print(f"\n{'File':<45}  {'Size (MB)':>10}")
    print("-" * 57)
    total = 0
    for f in files:
        size_mb = f.stat().st_size / (1024 * 1024)
        total  += size_mb
        print(f"  {f.name:<43}  {size_mb:>10.1f}")
    print("-" * 57)
    print(f"  {'TOTAL':<43}  {total:>10.1f} MB")
    print(f"  Files: {len(files)} / 12\n")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  NYC Taxi Data Ingestion — API Integration")
    print(f"  Source  : {BASE_URL}")
    print(f"  Dataset : Yellow Taxi Trip Records {YEAR}")
    print(f"  Output  : {OUTPUT_DIR}")
    print("=" * 60)
    print()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    errors = []
    for month, name in MONTHS.items():
        print(f"[{month:02d}/12] {name} {YEAR}")
        try:
            download_parquet(month, OUTPUT_DIR)
        except Exception as e:
            errors.append((month, str(e)))

    print("\n" + "=" * 60)
    print("  Download Summary")
    print("=" * 60)
    verify_downloads(OUTPUT_DIR)

    if errors:
        print("Errors encountered:")
        for month, msg in errors:
            print(f"  Month {month:02d}: {msg}")
        sys.exit(1)
    else:
        print("All 12 files ready.")
        print(f"Total dataset size satisfies the 500 MB+ course requirement.")


if __name__ == "__main__":
    main()
