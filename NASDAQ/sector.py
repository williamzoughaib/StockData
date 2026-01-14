# Organize stocks into sector folders using SEC offices from SECstocks_with_offices.csv
# Copies stock data files to office-based folder structure

import os
import shutil
import pandas as pd
from pathlib import Path


# Timeframe folders to copy
TIMEFRAMES = ['1yr', '5yr', '10yr', 'alltime']


def load_stock_offices():
    """
    Load the SECstocks_with_offices.csv file

    Returns:
        DataFrame with columns: NASDAQ Symbol, Security Name, SIC, Office, CIK
    """
    csv_path = os.path.join('Data', 'SECstocks_with_offices.csv')

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Could not find {csv_path}. Please run merge.py first.")

    print(f"Loading {csv_path}...")
    df = pd.read_csv(csv_path)
    print(f"  - Loaded {len(df)} stocks")

    return df


def get_office_tickers(stocks_df):
    """
    Group tickers by their SEC office

    Args:
        stocks_df: DataFrame with stock data including Office column

    Returns:
        dict: {office_name: [list of tickers]}
    """
    # Remove stocks without office assignment
    stocks_with_office = stocks_df[stocks_df['Office'].notna()].copy()

    office_tickers = {}

    for office in stocks_with_office['Office'].unique():
        tickers = stocks_with_office[stocks_with_office['Office'] == office]['NASDAQ Symbol'].tolist()
        office_tickers[office] = tickers
        print(f"  {office}: {len(tickers)} stocks")

    # Handle unmapped stocks
    unmapped = stocks_df[stocks_df['Office'].isna()]
    if len(unmapped) > 0:
        office_tickers['Unmapped'] = unmapped['NASDAQ Symbol'].tolist()
        print(f"  Unmapped: {len(unmapped)} stocks")

    return office_tickers


def copy_office_files(office_name, tickers):
    """
    Copy stock files for a given office to the appropriate folder structure

    Args:
        office_name: Name of the SEC office
        tickers: List of stock tickers for this office

    Returns:
        dict: Statistics about files copied
    """
    tickers_set = set(tickers)  # For faster lookup

    # Create office folder structure - both CSV and Parquet in same folder
    office_base = os.path.join('SECTORS', office_name)

    for timeframe in TIMEFRAMES:
        os.makedirs(os.path.join(office_base, timeframe), exist_ok=True)

    # Track statistics
    stats = {tf: {'found': 0, 'copied_csv': 0, 'copied_parquet': 0} for tf in TIMEFRAMES}

    print(f"\nCopying files for {len(tickers)} stocks in {office_name}...")

    # Process each timeframe
    for timeframe in TIMEFRAMES:
        source_folder = os.path.join('STOCKS', timeframe)
        dest_folder = os.path.join(office_base, timeframe)

        if not os.path.exists(source_folder):
            print(f"  {timeframe}: Source folder not found, skipping")
            continue

        # Get all CSV files in source folder
        csv_files = list(Path(source_folder).glob("*.csv"))

        for csv_file in csv_files:
            ticker = csv_file.stem  # Get filename without extension

            if ticker in tickers_set:
                stats[timeframe]['found'] += 1

                # Copy CSV file to dest folder
                dest_csv_file = os.path.join(dest_folder, csv_file.name)
                try:
                    shutil.copy2(csv_file, dest_csv_file)
                    stats[timeframe]['copied_csv'] += 1
                except Exception as e:
                    print(f"    Error copying CSV {ticker}: {e}")

                # Copy Parquet file from same source folder if it exists
                source_parquet_file = Path(source_folder) / f"{ticker}.parquet"
                if source_parquet_file.exists():
                    dest_parquet_file = os.path.join(dest_folder, f"{ticker}.parquet")
                    try:
                        shutil.copy2(source_parquet_file, dest_parquet_file)
                        stats[timeframe]['copied_parquet'] += 1
                    except Exception as e:
                        print(f"    Error copying Parquet {ticker}: {e}")

        print(f"  {timeframe}: {stats[timeframe]['copied_csv']} CSV, {stats[timeframe]['copied_parquet']} Parquet files copied")

    return stats


def main():
    print("=" * 70)
    print("SEC Office Data Organization")
    print("=" * 70)
    print("Organizing stocks by SEC office into SECTORS folder...")
    print()

    # Load stock-office mappings
    stocks_df = load_stock_offices()

    # Get tickers grouped by office
    print("\nStocks per office:")
    office_tickers = get_office_tickers(stocks_df)

    # Create main SECTORS folder
    os.makedirs('SECTORS', exist_ok=True)

    results = {}

    # Process each office
    for office_name, tickers in office_tickers.items():
        if tickers:
            stats = copy_office_files(office_name, tickers)
            results[office_name] = stats
        else:
            results[office_name] = {tf: {'found': 0, 'copied_csv': 0, 'copied_parquet': 0} for tf in TIMEFRAMES}

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY BY OFFICE")
    print("=" * 70)
    for office_name, stats in results.items():
        total_csv = sum(s['copied_csv'] for s in stats.values())
        total_parquet = sum(s['copied_parquet'] for s in stats.values())
        print(f"\n{office_name}:")
        for timeframe in TIMEFRAMES:
            print(f"  {timeframe:10} {stats[timeframe]['copied_csv']} CSV, {stats[timeframe]['copied_parquet']} Parquet")
        print(f"  Total: {total_csv} CSV, {total_parquet} Parquet files")

    print("\n" + "=" * 70)
    grand_total_csv = sum(sum(s['copied_csv'] for s in stats.values()) for stats in results.values())
    grand_total_parquet = sum(sum(s['copied_parquet'] for s in stats.values()) for stats in results.values())
    print(f"Grand Total: {grand_total_csv} CSV, {grand_total_parquet} Parquet files copied across all offices")
    print("=" * 70)


if __name__ == "__main__":
    main()
