#!/usr/bin/env python3
"""
Debug script to check GCS paths and list CSV files
"""

import json
import logging
from sys import path

from google.cloud import storage

path.append("..")
from src.CSV_reader import CSVReader

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    # Load configuration
    with open("../config.json", "r") as f:
        config = json.load(f)

    bucket_name = config.get("gcs_bucket")
    service_account_path = config.get("service_account_path")
    gcs_base_path_template = config.get("gcs_base_path_template")

    # Fix the service account path to be relative to the main directory
    if service_account_path and not service_account_path.startswith("/"):
        service_account_path = f"../{service_account_path}"

    print(f"Bucket: {bucket_name}")
    print(f"GCS Path Template: {gcs_base_path_template}")
    print(f"Service Account: {service_account_path}")

    # Initialize CSV Reader
    csv_reader = CSVReader(
        gcs_bucket=bucket_name, service_account_path=service_account_path
    )

    # List all blobs in the bucket to see the structure
    print("\n=== Listing bucket structure ===")
    blobs = csv_reader.bucket.list_blobs()
    paths = set()
    for blob in blobs:
        path_parts = blob.name.split("/")
        if len(path_parts) >= 3:
            paths.add("/".join(path_parts[:3]))

    for path in sorted(paths):
        print(f"Path found: {path}")

    # Check specific paths for question-bank-service
    service = "question-bank-service"
    date = "20251201"
    service_gcs_path = gcs_base_path_template.format(date=date, service=service)

    print(f"\n=== Checking configured service path: {service_gcs_path} ===")

    # List all files in the service path
    blobs = csv_reader.bucket.list_blobs(prefix=service_gcs_path)
    all_files = []
    csv_files = []

    for blob in blobs:
        all_files.append(blob.name)
        if blob.name.lower().endswith(".csv"):
            csv_files.append(blob.name)

    print(f"All files in path: {len(all_files)}")
    for file in all_files[:5]:  # Show first 5 files
        print(f"  - {file}")
    if len(all_files) > 5:
        print(f"  ... and {len(all_files) - 5} more")

    print(f"\nCSV files in path: {len(csv_files)}")
    for file in csv_files[:5]:  # Show first 5 CSV files
        print(f"  - {file}")
    if len(csv_files) > 5:
        print(f"  ... and {len(csv_files) - 5} more")

    # Check the actual path that seems to exist in GCS
    actual_path = f"csvextract/dev_question_bank_service"
    print(f"\n=== Checking actual path: {actual_path} ===")

    blobs = csv_reader.bucket.list_blobs(prefix=actual_path)
    all_files = []
    csv_files = []

    for blob in blobs:
        all_files.append(blob.name)
        if blob.name.lower().endswith(".csv"):
            csv_files.append(blob.name)

    print(f"All files in path: {len(all_files)}")
    for file in all_files[:5]:  # Show first 5 files
        print(f"  - {file}")
    if len(all_files) > 5:
        print(f"  ... and {len(all_files) - 5} more")

    print(f"\nCSV files in path: {len(csv_files)}")
    for file in csv_files[:5]:  # Show first 5 CSV files
        print(f"  - {file}")
    if len(csv_files) > 5:
        print(f"  ... and {len(csv_files) - 5} more")

    # Check the path you mentioned
    mentioned_path = f"sql-exports/20251201/csvextract"
    print(f"\n=== Checking path you mentioned: {mentioned_path} ===")

    # List all directories under this path
    blobs = csv_reader.bucket.list_blobs(prefix=mentioned_path)
    paths = set()
    for blob in blobs:
        path_parts = blob.name.split("/")
        if len(path_parts) >= 4:
            paths.add("/".join(path_parts[:4]))

    print(f"Directories under {mentioned_path}:")
    for path in sorted(paths):
        print(f"  - {path}")

    # Check specifically for dev-question-bank-service under the mentioned path
    question_bank_path = f"{mentioned_path}/dev-question-bank-service"
    print(
        f"\n=== Checking specifically for dev-question-bank-service under {mentioned_path} ==="
    )

    blobs = csv_reader.bucket.list_blobs(prefix=question_bank_path)
    all_files = []
    csv_files = []

    for blob in blobs:
        all_files.append(blob.name)
        if blob.name.lower().endswith(".csv"):
            csv_files.append(blob.name)

    print(f"All files in path: {len(all_files)}")
    for file in all_files[:5]:  # Show first 5 files
        print(f"  - {file}")
    if len(all_files) > 5:
        print(f"  ... and {len(all_files) - 5} more")

    print(f"\nCSV files in path: {len(csv_files)}")
    for file in csv_files[:5]:  # Show first 5 CSV files
        print(f"  - {file}")
    if len(csv_files) > 5:
        print(f"  ... and {len(csv_files) - 5} more")

    # Check if the path has any variations
    print("\n=== Checking variations of the path ===")
    variations = [
        f"sql-exports/{date}/csvextract/{service}/",
        f"sql-exports/{date}/csvextract/{service}",
        f"sql-exports/{date}/csvextract/{service}/*",
        f"sql-exports/{date}/csvextract/{service}/*.csv",
        # Try different naming conventions
        f"sql-exports/{date}/csvextract/dev-{service}",
        f"csvextract/dev_question_bank_service",
        f"csvextract/question_bank_service",
        f"csvextract/question-bank-service",
    ]

    for variation in variations:
        print(f"\nChecking: {variation}")
        blobs = csv_reader.bucket.list_blobs(prefix=variation.rstrip("/*"))
        count = sum(1 for blob in blobs if blob.name.lower().endswith(".csv"))
        print(f"CSV files found: {count}")


if __name__ == "__main__":
    main()
