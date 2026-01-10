# Add Daily_Return and Log_Volume columns to existing CSV files.
## This works for both STOCKS and ETFs in their respective folders.
##This script reads existing CSV files, adds the calculated columns, and saves them.

### necessary modules
import pandas as pd
import numpy as np
import os
from pathlib import Path

#### Folders to process - now includes STOCKS and ETFs subfolders
FOLDERS = [
    'STOCKS/1yr',
    'STOCKS/5yr',
    'STOCKS/10yr',
    'STOCKS/alltime',
    'ETFs/1yr',
    'ETFs/5yr',
    'ETFs/10yr',
    'ETFs/alltime'
]

### Add Daily_Return and Log_Volume Function to add columns to a single file
def add_columns_to_file(file_path):
                            
    try:
        #### Read CSV, skipping the Ticker and Date rows (rows 1 and 2)
        df = pd.read_csv(file_path, skiprows=[1, 2])

        #### Set Date as index
        if 'Price' in df.columns:
            df = df.set_index('Price')
            df.index.name = 'Date'

        #### Check if columns already exist
        if 'Daily_Return' in df.columns and 'Log_Volume' in df.columns:
            return "already has columns"

        #### Add Daily_Return
        if 'Close' in df.columns:
            df['Daily_Return'] = pd.to_numeric(df['Close'], errors='coerce').pct_change(fill_method=None)

        #### Add Log_Volume
        if 'Volume' in df.columns:
            volume_numeric = pd.to_numeric(df['Volume'], errors='coerce')
            df['Log_Volume'] = np.log(volume_numeric.replace(0, np.nan))

        #### Save back to file
        df.to_csv(file_path)

        return "updated"

    except Exception as e:
        return f"error: {e}"

### Aggregate function to process all files in all folders
def main():
    """Process all CSV files in all folders."""
    print("=" * 60)
    print("Adding Daily_Return and Log_Volume columns to existing CSV files")
    print("=" * 60)

    for folder in FOLDERS:
        if not os.path.exists(folder):
            print(f"\nSkipping {folder} - folder doesn't exist")
            continue

        print(f"\nProcessing {folder}/...")

        csv_files = list(Path(folder).glob("*.csv"))

        updated = 0
        already_done = 0
        errors = 0

        for i, csv_file in enumerate(csv_files):
            result = add_columns_to_file(csv_file)

            if result == "updated":
                updated += 1
            elif result == "already has columns":
                already_done += 1
            else:
                errors += 1
                if errors <= 5:  # Show first 5 errors
                    print(f"  ERROR {csv_file.name}: {result}")

            # Progress indicator
            if (i + 1) % 100 == 0:
                print(f"  Processed {i + 1}/{len(csv_files)} files...")

        print(f"  {folder}: {updated} updated, {already_done} already had columns, {errors} errors")

    print("\n" + "=" * 60)
    print("Complete!")


if __name__ == "__main__":
    main()
