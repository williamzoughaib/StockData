"""
Sector-Based Stock Data Downloader

Downloads historical stock data organized by sector using yfinance.
Creates sector-specific folders with timeframe subfolders (1yr, 5yr, 10yr, alltime).
"""

import pandas as pd
import yfinance as yf
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
OFFSET = 0
LIMIT = None  # Set to None to process all symbols, or a number to limit
MAX_WORKERS = 5  # Number of parallel threads for fetching sector info

# Define timeframe folders and their window sizes (trading days)
TIMEFRAMES = {
    '1yr': {'window': 252},
    '5yr': {'window': 1260},
    '10yr': {'window': 2520},
    'alltime': {'window': None}
}

# Sector mapping
SECTORS = {
    'Technology': 'Technology',
    'Healthcare': 'Healthcare',
    'Financial Services': 'Financials',
    'Financials': 'Financials',
    'Consumer Cyclical': 'Consumer_Discretionary',
    'Consumer Defensive': 'Consumer_Staples',
    'Communication Services': 'Communication_Services',
    'Industrials': 'Industrials',
    'Energy': 'Energy',
    'Utilities': 'Utilities',
    'Real Estate': 'Real_Estate',
    'Basic Materials': 'Materials'
}


def get_nasdaq_symbols():
    """Fetch all NASDAQ stock symbols (excluding ETFs)."""
    url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    data = pd.read_csv(url, sep='|')

    # Filter: only stocks (not ETFs), not test issues
    data_clean = data[(data['Test Issue'] == 'N') & (data['ETF'] == 'N')]
    symbols = data_clean['NASDAQ Symbol'].tolist()

    print(f'Total stock symbols: {len(symbols)}')
    return symbols


def get_stock_sector(symbol):
    """Fetch sector information for a single stock using yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        sector = info.get('sector', None)
        if sector and sector in SECTORS:
            return symbol, SECTORS[sector]
        return symbol, None
    except Exception as e:
        return symbol, None


def categorize_stocks_by_sector(symbols):
    """Categorize stocks by sector using parallel processing."""
    print("\nFetching sector information for stocks...")
    print("This may take a while for all stocks...")

    sector_dict = {sector: [] for sector in set(SECTORS.values())}
    uncategorized = []

    processed = 0
    total = len(symbols)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_stock_sector, symbol): symbol for symbol in symbols}

        for future in as_completed(futures):
            symbol, sector = future.result()
            processed += 1

            if sector:
                sector_dict[sector].append(symbol)
            else:
                uncategorized.append(symbol)

            # Progress update every 100 stocks
            if processed % 100 == 0:
                print(f"Processed {processed}/{total} stocks...")

    # Print summary
    print("\n" + "=" * 60)
    print("SECTOR CATEGORIZATION SUMMARY")
    print("=" * 60)
    for sector, stocks in sorted(sector_dict.items()):
        print(f"{sector:30} - {len(stocks):5} stocks")
    print(f"{'Uncategorized':30} - {len(uncategorized):5} stocks")

    return sector_dict, uncategorized


def create_directories(sector_dict):
    """Create all necessary directories for each sector with timeframe subfolders."""
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
            # Incremental update: append new data and trim to window size
            existing_window = pd.read_csv(file_window, index_col=0, parse_dates=True)
            updated_window = pd.concat([existing_window, data_new])
            updated_window = updated_window.tail(window_size)
            updated_window.to_csv(file_window)
        else:
            # Create new window file from all data
            data_window = data_all.tail(window_size)
            data_window.to_csv(file_window)


def process_symbol(symbol, sector, symbols_count, current_index):
    """Process a single symbol: download data and create timeframe files."""
    file_all = os.path.join(sector, 'alltime', f"{symbol}.csv")

    try:
        print(f"[{current_index + 1}/{symbols_count}] Processing {symbol} ({sector})...", end=" ")

        data_new = None
        new_data_added = False

        # Check if we have existing data
        if os.path.exists(file_all):
            # Load existing all-time data
            existing_data = pd.read_csv(file_all, index_col=0, parse_dates=True)
            last_date = existing_data.index[-1]

            # Download only new data since last date
            data_new = yf.download(symbol, start=last_date, progress=False)

            if data_new.empty or len(data_new) <= 1:
                # No new data available
                print("✓ No new data")
                return True

            # Remove overlapping date and append new data
            data_new = data_new.iloc[1:]
            data_all = pd.concat([existing_data, data_new])
            new_data_added = True
            print(f"Added {len(data_new)} rows", end=" ")
        else:
            # Download maximum available data for new symbol
            data_all = yf.download(symbol, period='max', progress=False)

            if data_all.empty:
                print("✗ No data available")
                return False

            new_data_added = True
            print(f"New symbol ({len(data_all)} rows)", end=" ")

        # Save all-time data
        data_all.to_csv(file_all)

        # Update rolling window files
        if new_data_added:
            update_rolling_windows(symbol, sector, data_all, data_new)
            print("✓ Updated")

        return True

    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def save_sector_mappings(sector_dict, uncategorized):
    """Save sector mappings to CSV files for reference."""
    for sector, symbols in sector_dict.items():
        if symbols:
            df = pd.DataFrame({'Symbol': symbols})
            df.to_csv(f"{sector}_symbols.csv", index=False)

    if uncategorized:
        df = pd.DataFrame({'Symbol': uncategorized})
        df.to_csv("Uncategorized_symbols.csv", index=False)

    print("\nSaved sector mapping CSV files")


def main():
    """Main execution function."""
    print("=" * 60)
    print("Sector-Based Stock Data Downloader")
    print("=" * 60)

    # Get symbols
    symbols = get_nasdaq_symbols()

    # Categorize by sector
    sector_dict, uncategorized = categorize_stocks_by_sector(symbols)

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
        for i, symbol in enumerate(sector_symbols):
            if process_symbol(symbol, sector, len(sector_symbols), i):
                success_count += 1

        print(f"{sector}: {success_count}/{len(sector_symbols)} processed successfully")

    print("\n" + "=" * 60)
    print("COMPLETE!")
    print("=" * 60)


if __name__ == "__main__":
    main()
