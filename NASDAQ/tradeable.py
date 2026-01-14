# NASDAQ Stock Data Downloader
## Downloads historical stock data for all NASDAQ-traded symbols using yfinance.
## Creates rolling window datasets (1yr, 5yr, 10yr) and maintains all-time data.

### necessary modules
import pandas as pd
import numpy as np
import yfinance as yf
import os

LISTINGS_DIR = "NASDAQ Listings"

### Configuration
OFFSET = 0
LIMIT = None  # Set to None to process all symbols, or a number to limit

### Define timeframe folders and their window sizes (trading days)
TIMEFRAMES = {
    '1yr': {'window': 252},
    '5yr': {'window': 1260},
    '10yr': {'window': 2520},
    'alltime': {'window': None}
}

### Asset type base folders
ASSET_TYPES = {
    'stock': 'STOCKS',
    'etf': 'ETFs'
}

### Source URL for NASDAQ symbols and separate stocks and ETFs
def get_nasdaq_symbols(stocks_file="Listings Data/SECstocks.parquet", etfs_file="Listings Data/etfs.parquet"):
    stocks_df = pd.read_parquet(stocks_file)
    etfs_df = pd.read_parquet(etfs_file)

    stocks = stocks_df["NASDAQ Symbol"].astype(str).str.strip().dropna().tolist()
    etfs = etfs_df["NASDAQ Symbol"].astype(str).str.strip().dropna().tolist()

    print(f"Total stocks: {len(stocks)}")
    print(f"Total ETFs: {len(etfs)}")
    print(f"Total symbols: {len(stocks) + len(etfs)}")

    return stocks, etfs

### Create necessary directories
def create_directories():
    for asset_folder in ASSET_TYPES.values():
        for timeframe in TIMEFRAMES.keys():
            folder_path = os.path.join(asset_folder, timeframe)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
                print(f"Created directory: {folder_path}")

def save_both(df, parquet_path):
    df.to_parquet(parquet_path)
    df.to_csv(parquet_path.replace(".parquet", ".csv"))

### This script updates after reruns. Except for all-time data, it appends new data. 
def update_rolling_windows(symbol, asset_type, data_all, data_new=None):
    asset_folder = ASSET_TYPES[asset_type]

    for timeframe_key, config in TIMEFRAMES.items():
        if timeframe_key == 'alltime':
            continue

        window_size = config['window']
        folder_path = os.path.join(asset_folder, timeframe_key)
        file_window = os.path.join(folder_path, f"{symbol}.parquet")

        if data_new is not None and not data_new.empty and os.path.exists(file_window):
            # Incremental update: append new data and trim to window size
            existing_window = pd.read_parquet(file_window)
            updated_window = pd.concat([existing_window, data_new])
            updated_window = updated_window.tail(window_size)
            save_both(updated_window, file_window)
        else:
            # Create new window file from all data
            data_window = data_all.tail(window_size)
            save_both(data_window, file_window)

def process_symbol(symbol, asset_type, symbols_count, current_index):
    asset_folder = ASSET_TYPES[asset_type]
    file_all = os.path.join(asset_folder, 'alltime', f"{symbol}.parquet")

    try:
        print(f"[{current_index + 1}/{symbols_count}] Processing {symbol}...", end=" ")

        data_new = None
        new_data_added = False

        # Check if we have existing data
        if os.path.exists(file_all):
            # Load existing all-time data
            existing_data = pd.read_parquet(file_all)
            last_date = existing_data.index[-1]

            # Download only new data since last date
            data_new = yf.download(symbol, start=last_date, progress=False)

            if data_new.empty or len(data_new) <= 1:
                # No new data available
                print("No new data")
                return True

            # Remove overlapping date and append new data
            data_new = data_new.iloc[1:]

            if isinstance(data_new.columns, pd.MultiIndex):
                data_new.columns = data_new.columns.get_level_values(0)

            data_all = pd.concat([existing_data, data_new])
            new_data_added = True
            print(f"Added {len(data_new)} rows", end=" ")
        else:
            # Download maximum available data for new symbol
            data_all = yf.download(symbol, period='max', progress=False)

            if data_all.empty:
                print("No data available")
                return False

            new_data_added = True
            print(f"New symbol ({len(data_all)} rows)", end=" ")

        if isinstance(data_all.columns, pd.MultiIndex):
            data_all.columns = data_all.columns.get_level_values(0)

        # Save all-time data
        save_both(data_all, file_all)

        # Update rolling window files
        if new_data_added:
            update_rolling_windows(symbol, asset_type, data_all, data_new)
            print("Updated")

        return True

    except Exception as e:
        print(f"Error: {e}")
        return False

### Print summary of processing
def print_summary(stock_success, stock_total, etf_success, etf_total):
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f'\nStocks processed: {stock_success}/{stock_total}')
    print(f'ETFs processed:   {etf_success}/{etf_total}')
    print(f'Total processed:  {stock_success + etf_success}/{stock_total + etf_total}')
    print("\nData saved to:")

    for asset_folder in ASSET_TYPES.values():
        print(f"\n  {asset_folder.upper()}:")
        for timeframe in TIMEFRAMES.keys():
            folder_path = os.path.join(asset_folder, timeframe)
            if os.path.exists(folder_path):
                file_count = len([f for f in os.listdir(folder_path) if f.endswith('.parquet')])
                print(f"    {timeframe:10} - {file_count:5} files")

def main():
    print("=" * 60)
    print("NASDAQ Stock & ETF Data Downloader")
    print("=" * 60)

    ### Get symbols and create directories
    stocks, etfs = get_nasdaq_symbols()
    create_directories()

    #### Process stocks
    print(f"\n{'=' * 60}")
    print("PROCESSING STOCKS")
    print("=" * 60)
    stock_limit = LIMIT if LIMIT else len(stocks)
    stock_end = min(OFFSET + stock_limit, len(stocks))
    stock_success = 0

    for i in range(OFFSET, stock_end):
        if process_symbol(stocks[i], 'stock', stock_end, i):
            stock_success += 1

    #### Process ETFs
    print(f"\n{'=' * 60}")
    print("PROCESSING ETFs")
    print("=" * 60)
    etf_limit = LIMIT if LIMIT else len(etfs)
    etf_end = min(OFFSET + etf_limit, len(etfs))
    etf_success = 0

    for i in range(OFFSET, etf_end):
        if process_symbol(etfs[i], 'etf', etf_end, i):
            etf_success += 1

if __name__ == "__main__":
    main()
