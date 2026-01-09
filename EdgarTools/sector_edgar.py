"""
Sector-Based Stock Categorization using EdgarTools

Uses SEC EDGAR data to categorize stocks by sector using SIC codes.
Then downloads historical price data organized by sector.

Run this from HistoricalStockData directory to create sector folders there,
or it will create them in the current directory.
"""

import pandas as pd
import yfinance as yf
import os
from pathlib import Path

try:
    from edgar import set_identity
    from edgar.reference.tickers import get_company_tickers
except ImportError as e:
    print("ERROR: EdgarTools not installed")
    print("Install with: pip install edgartools")
    exit(1)

# Set your identity for SEC EDGAR API
set_identity("williamzoughaib@gmail.com")

# Configuration
OFFSET = 0
LIMIT = None

# Define timeframe folders and their window sizes (trading days)
TIMEFRAMES = {
    '1yr': {'window': 252},
    '5yr': {'window': 1260},
    '10yr': {'window': 2520},
    'alltime': {'window': None}
}

# SIC Code to Sector Mapping
# Based on https://www.sec.gov/corpfin/division-of-corporation-finance-standard-industrial-classification-sic-code-list
SIC_TO_SECTOR = {
    'Technology': [(3570, 3579), (3600, 3699), (3810, 3899), (7370, 7379)],  # Computer hardware, electronics, software
    'Healthcare': [(2833, 2836), (8000, 8099)],  # Pharmaceuticals, medical devices, healthcare services
    'Financials': [(6000, 6799)],  # Banking, insurance, real estate
    'Consumer_Discretionary': [(2300, 2399), (3700, 3719), (5000, 5999)],  # Apparel, automotive, retail
    'Consumer_Staples': [(2000, 2099), (2100, 2199), (5400, 5499)],  # Food, beverages, household goods
    'Communication_Services': [(4800, 4899), (7800, 7899)],  # Telecom, media, entertainment
    'Industrials': [(3300, 3399), (3400, 3499), (3500, 3569), (3580, 3599)],  # Machinery, aerospace
    'Energy': [(1300, 1399), (2900, 2999)],  # Oil, gas, coal
    'Utilities': [(4900, 4999)],  # Electric, gas, water
    'Real_Estate': [(6500, 6599)],  # Real estate operations
    'Materials': [(1000, 1099), (2600, 2699), (2800, 2829), (3300, 3399)]  # Mining, paper, chemicals, metals
}


def get_sector_from_sic(sic_code):
    """Map SIC code to sector."""
    if pd.isna(sic_code):
        return None

    sic = int(sic_code)
    for sector, ranges in SIC_TO_SECTOR.items():
        for start, end in ranges:
            if start <= sic <= end:
                return sector
    return None


def get_stocks_by_sector():
    """Fetch all company tickers and categorize by sector using SIC codes."""
    print("Fetching company tickers from SEC EDGAR...")

    try:
        tickers_data = get_company_tickers()
    except Exception as e:
        print(f"ERROR fetching tickers: {e}")
        print("\nTroubleshooting:")
        print("1. Check your internet connection")
        print("2. Verify edgar package is installed: pip install edgartools")
        print("3. Make sure you have set your identity email")
        return None, None

    print(f"Total companies: {len(tickers_data)}")

    # Check available columns
    print(f"Available columns: {list(tickers_data.columns)}")

    # Categorize by sector
    sector_dict = {sector: [] for sector in SIC_TO_SECTOR.keys()}
    uncategorized = []

    for _, row in tickers_data.iterrows():
        ticker = row.get('ticker', None)
        sic = row.get('sic', None)

        if not ticker:
            continue

        sector = get_sector_from_sic(sic)
        if sector:
            sector_dict[sector].append(ticker)
        else:
            uncategorized.append(ticker)

    # Print summary
    print("\n" + "=" * 60)
    print("SECTOR CATEGORIZATION SUMMARY")
    print("=" * 60)
    for sector, stocks in sorted(sector_dict.items()):
        print(f"{sector:30} - {len(stocks):5} stocks")
    print(f"{'Uncategorized':30} - {len(uncategorized):5} stocks")

    return sector_dict, uncategorized


def save_sector_mappings(sector_dict, uncategorized):
    """Save sector mappings to CSV files."""
    for sector, symbols in sector_dict.items():
        if symbols:
            df = pd.DataFrame({'Symbol': symbols})
            df.to_csv(f"{sector}_symbols.csv", index=False)
            print(f"Saved {sector}_symbols.csv")

    if uncategorized:
        df = pd.DataFrame({'Symbol': uncategorized})
        df.to_csv("Uncategorized_symbols.csv", index=False)
        print("Saved Uncategorized_symbols.csv")


def create_directories(sector_dict):
    """Create directories for each sector with timeframe subfolders."""
    for sector in sector_dict.keys():
        for timeframe in TIMEFRAMES.keys():
            folder_path = os.path.join(sector, timeframe)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
                print(f"Created directory: {folder_path}")


def update_rolling_windows(symbol, sector, data_all, data_new=None):
    """Update rolling window files for a symbol."""
    for timeframe_key, config in TIMEFRAMES.items():
        if timeframe_key == 'alltime':
            continue

        window_size = config['window']
        folder_path = os.path.join(sector, timeframe_key)
        file_window = os.path.join(folder_path, f"{symbol}.csv")

        if data_new is not None and not data_new.empty and os.path.exists(file_window):
            existing_window = pd.read_csv(file_window, index_col=0, parse_dates=True)
            updated_window = pd.concat([existing_window, data_new])
            updated_window = updated_window.tail(window_size)
            updated_window.to_csv(file_window)
        else:
            data_window = data_all.tail(window_size)
            data_window.to_csv(file_window)


def process_symbol(symbol, sector, symbols_count, current_index):
    """Process a single symbol: download data and create timeframe files."""
    file_all = os.path.join(sector, 'alltime', f"{symbol}.csv")

    try:
        print(f"[{current_index + 1}/{symbols_count}] {symbol} ({sector})...", end=" ")

        data_new = None
        new_data_added = False

        if os.path.exists(file_all):
            existing_data = pd.read_csv(file_all, index_col=0, parse_dates=True)
            last_date = existing_data.index[-1]
            data_new = yf.download(symbol, start=last_date, progress=False)

            if data_new.empty or len(data_new) <= 1:
                print("✓ No new data")
                return True

            data_new = data_new.iloc[1:]
            data_all = pd.concat([existing_data, data_new])
            new_data_added = True
            print(f"Added {len(data_new)} rows", end=" ")
        else:
            data_all = yf.download(symbol, period='max', progress=False)

            if data_all.empty:
                print("✗ No data")
                return False

            new_data_added = True
            print(f"New ({len(data_all)} rows)", end=" ")

        data_all.to_csv(file_all)

        if new_data_added:
            update_rolling_windows(symbol, sector, data_all, data_new)
            print("✓")

        return True

    except Exception as e:
        print(f"✗ {e}")
        return False


def main():
    """Main execution function."""
    print("=" * 60)
    print("Sector-Based Stock Data Downloader (SEC EDGAR)")
    print("=" * 60)

    # Get stocks categorized by sector
    sector_dict, uncategorized = get_stocks_by_sector()

    if not sector_dict:
        print("Failed to fetch sector data. Exiting.")
        return

    # Save sector mappings
    save_sector_mappings(sector_dict, uncategorized)

    # Create directories
    create_directories(sector_dict)

    # Process each sector
    for sector, sector_symbols in sector_dict.items():
        if not sector_symbols:
            continue

        print(f"\n{'=' * 60}")
        print(f"PROCESSING {sector.upper()}")
        print("=" * 60)

        success_count = 0
        total = len(sector_symbols)

        for i, symbol in enumerate(sector_symbols):
            if process_symbol(symbol, sector, total, i):
                success_count += 1

        print(f"\n{sector}: {success_count}/{total} processed")

    print("\n" + "=" * 60)
    print("COMPLETE!")
    print("=" * 60)


if __name__ == "__main__":
    main()
