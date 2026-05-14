"""
HDFS Setup Script — NYC Taxi Fare Prediction
============================================
Category : Storage / Data Management — HDFS
Course   : CSCI461 Introduction to Big Data (Spring 2026)

This script copies the raw NYC Taxi Parquet files into HDFS
using the Docker-based Hadoop cluster (docker-hadoop-spark-jupyter).

Prerequisites:
  1. Docker Desktop is running
  2. The Hadoop cluster is up:
       cd "D:\\UNI\\Third_second\\CSCI461 Introduction to Big Data\\docker-hadoop-spark-jupyter\\docker-hadoop-spark-jupyter"
       docker-compose up -d
  3. Wait ~30 seconds for the namenode to fully start

HDFS paths used:
  /taxi/raw/       — 12 raw monthly Parquet files
  /taxi/processed/ — cleaned Parquet output (optional)
"""

import subprocess
import sys
import time
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

NAMENODE_CONTAINER = "namenode"
RAW_DATA_DIR       = Path(__file__).resolve().parent.parent / "data" / "raw" / "yellow_2024"
PROCESSED_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "processed" / "cleaned_yellow_2024"

HDFS_RAW_DIR       = "/taxi/raw"
HDFS_PROCESSED_DIR = "/taxi/processed"

# ── Helper Functions ───────────────────────────────────────────────────────────

def run(cmd: list, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"  [ERROR] {' '.join(cmd)}")
        print(f"          {result.stderr.strip()}")
        sys.exit(1)
    return result


def hdfs(args: list, check: bool = True) -> subprocess.CompletedProcess:
    """Run an hdfs dfs command inside the namenode container."""
    return run(["docker", "exec", NAMENODE_CONTAINER, "hdfs", "dfs"] + args, check=check)


def docker_cp(local_path: Path, container_tmp: str) -> None:
    """Copy a local file into the namenode container's /tmp directory."""
    run(["docker", "cp", str(local_path), f"{NAMENODE_CONTAINER}:{container_tmp}"])


def check_namenode_ready() -> bool:
    """Check if the namenode container is running and HDFS is accessible."""
    result = run(
        ["docker", "exec", NAMENODE_CONTAINER, "hdfs", "dfsadmin", "-report"],
        check=False
    )
    return result.returncode == 0

# ── Main Steps ─────────────────────────────────────────────────────────────────

def step1_wait_for_namenode():
    print("[1/5] Waiting for namenode to be ready...")
    for attempt in range(12):  # up to 60 seconds
        if check_namenode_ready():
            print("      Namenode is ready.\n")
            return
        print(f"      Not ready yet, retrying in 5s... ({attempt + 1}/12)")
        time.sleep(5)
    print("  [ERROR] Namenode did not start in time.")
    print("  Make sure docker-compose is up:")
    print('  cd "...\\docker-hadoop-spark-jupyter\\docker-hadoop-spark-jupyter"')
    print("  docker-compose up -d")
    sys.exit(1)


def step2_create_hdfs_dirs():
    print("[2/5] Creating HDFS directories...")
    hdfs(["-mkdir", "-p", HDFS_RAW_DIR])
    print(f"      Created: {HDFS_RAW_DIR}")
    hdfs(["-mkdir", "-p", HDFS_PROCESSED_DIR])
    print(f"      Created: {HDFS_PROCESSED_DIR}\n")


def step3_upload_raw_files():
    print("[3/5] Uploading raw Parquet files to HDFS...")

    parquet_files = sorted(RAW_DATA_DIR.glob("*.parquet"))
    if not parquet_files:
        print(f"  [ERROR] No Parquet files found in: {RAW_DATA_DIR}")
        print("  Run data_ingestion.py first to download the files.")
        sys.exit(1)

    for i, f in enumerate(parquet_files, 1):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  [{i:02d}/12] {f.name}  ({size_mb:.1f} MB)")

        tmp_path = f"/tmp/{f.name}"
        hdfs_path = f"{HDFS_RAW_DIR}/{f.name}"

        # Check if already in HDFS
        exists = hdfs(["-test", "-e", hdfs_path], check=False)
        if exists.returncode == 0:
            print(f"         Already in HDFS — skipping")
            continue

        # Copy to container then put into HDFS
        docker_cp(f, tmp_path)
        hdfs(["-put", tmp_path, HDFS_RAW_DIR])
        run(["docker", "exec", NAMENODE_CONTAINER, "rm", tmp_path])
        print(f"         Uploaded to {hdfs_path}")

    print()


def step4_verify():
    print("[4/5] Verifying HDFS upload...")
    result = hdfs(["-ls", HDFS_RAW_DIR])
    print(result.stdout)

    result = hdfs(["-du", "-h", HDFS_RAW_DIR])
    print("  Space used:")
    print(f"  {result.stdout.strip()}\n")


def step5_print_spark_path():
    print("[5/5] Done. Use this path in your Spark notebook:")
    print()
    print('  # Read raw data from HDFS')
    print('  HDFS_RAW = "hdfs://namenode:9000/taxi/raw"')
    print('  df = spark.read.parquet(HDFS_RAW)')
    print()
    print('  # Save cleaned output to HDFS')
    print('  HDFS_PROCESSED = "hdfs://namenode:9000/taxi/processed"')
    print('  clean_df.write.mode("overwrite").parquet(HDFS_PROCESSED)')
    print()
    print("  HDFS Web UI: http://localhost:9870")
    print("  YARN UI    : http://localhost:8088")


# ── Entry Point ────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  HDFS Setup — NYC Taxi Fare Prediction")
    print(f"  Raw data : {RAW_DATA_DIR}")
    print(f"  HDFS raw : hdfs://namenode:9000{HDFS_RAW_DIR}")
    print("=" * 60)
    print()

    step1_wait_for_namenode()
    step2_create_hdfs_dirs()
    step3_upload_raw_files()
    step4_verify()
    step5_print_spark_path()


if __name__ == "__main__":
    main()
