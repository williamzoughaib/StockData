"""
Data Sync Script

Copies CSV files from HistoricalStockData to StockAnomalyDetector.
Syncs all timeframes: 1yr, 5yr, 10yr, all_data
"""

import shutil
from pathlib import Path

# Source and destination base directories
SOURCE_BASE = Path(r"E:\FinTechTradingPlatform\StockData\HistoricalStockData")
DEST_BASE = Path(r"E:\FinTechTradingPlatform\StockData\StockAnomalyDetector")

# Timeframe folders to sync
TIMEFRAMES = ['1yr_data', '5yr_data', '10yr_data', 'all_data']


def sync_folder(source_folder, dest_folder):
    """Sync all CSV files from source to destination."""
    # Create destination if it doesn't exist
    dest_folder.mkdir(parents=True, exist_ok=True)

    # Get all CSV files
    csv_files = list(source_folder.glob("*.csv"))

    copied = 0
    for csv_file in csv_files:
        dest_file = dest_folder / csv_file.name
        shutil.copy2(csv_file, dest_file)  # copy2 preserves metadata
        copied += 1

        if copied % 500 == 0:
            print(f"    {copied} files...")

    return copied


def main():
    """Sync all timeframes."""
    print("=" * 60)
    print("Data Sync: HistoricalStockData → StockAnomalyDetector")
    print("=" * 60)

    total_copied = 0

    for timeframe in TIMEFRAMES:
        source_folder = SOURCE_BASE / timeframe
        dest_folder = DEST_BASE / timeframe

        if not source_folder.exists():
            print(f"\n{timeframe}: Source folder not found, skipping")
            continue

        print(f"\n{timeframe}:")
        print(f"  Source: {source_folder}")
        print(f"  Dest:   {dest_folder}")

        copied = sync_folder(source_folder, dest_folder)
        total_copied += copied

        print(f"  ✓ Copied {copied} files")

    print("\n" + "=" * 60)
    print(f"Complete! Total files copied: {total_copied}")
    print("=" * 60)


if __name__ == "__main__":
    main()
