import os
from pathlib import Path
import urllib.request

from fineweb_spark.path import RAW_DATA_DIR


def download_fineweb_sample():
    """Download the FineWeb-Edu 10BT sample parquet shards."""

    data_dir = RAW_DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)

    base_url = "https://huggingface.co/datasets/HuggingFaceFW/fineweb-edu/resolve/main/sample/10BT"

    for i in range(14):
        filename = f"{i:03d}_00000.parquet"
        save_path = data_dir / filename

        if not save_path.exists():
            print(f"Downloading {filename}...")
            urllib.request.urlretrieve(f"{base_url}/{filename}", save_path)
        else:
            print(f"Skipping {filename} (already exists)")

    print("All 14 sample files downloaded.")