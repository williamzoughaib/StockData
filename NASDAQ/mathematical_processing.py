# Add Advanced Features: Returns, Volume, Volatility, Rolling Stats - GPU+CPU+RAM Optimized
## Reads existing Parquet files, adds calculated columns, and saves to both CSV and Parquet formats

import pandas as pd
import numpy as np
import torch
import os
import warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Suppress warnings
warnings.filterwarnings('ignore')
np.seterr(all='ignore')

# GPU setup
CUDA_AVAILABLE = torch.cuda.is_available()
DEVICE = torch.device('cuda' if CUDA_AVAILABLE else 'cpu')
print(f"GPU: {CUDA_AVAILABLE} | Device: {torch.cuda.get_device_name(0) if CUDA_AVAILABLE else 'CPU'}")

# Configuration - Optimized for NVMe SSD
PARALLEL_WORKERS = 64  # Increased for NVMe SSD
BATCH_SIZE = 100

# Asset types and timeframes
ASSET_TYPES = {
    'STOCKS': ['1yr', '5yr', '10yr', 'alltime'],
    'ETFs': ['1yr', '5yr', '10yr', 'alltime']
}

def compute_returns(prices):
    """Compute daily returns using GPU if available"""
    if CUDA_AVAILABLE and len(prices) > 1000:
        try:
            prices_t = torch.tensor(prices, dtype=torch.float32, device=DEVICE)
            returns = torch.zeros_like(prices_t)
            returns[1:] = (prices_t[1:] - prices_t[:-1]) / prices_t[:-1]
            returns[0] = float('nan')
            return returns.cpu().numpy()
        except:
            pass
    return np.concatenate([[np.nan], np.diff(prices) / prices[:-1]])

def compute_log_returns(prices):
    """Compute log returns using GPU if available"""
    if CUDA_AVAILABLE and len(prices) > 1000:
        try:
            prices_t = torch.tensor(prices, dtype=torch.float32, device=DEVICE)
            log_prices = torch.log(prices_t)
            log_returns = torch.zeros_like(log_prices)
            log_returns[1:] = log_prices[1:] - log_prices[:-1]
            log_returns[0] = float('nan')
            return log_returns.cpu().numpy()
        except:
            pass
    log_prices = np.log(prices)
    return np.concatenate([[np.nan], np.diff(log_prices)])

def compute_log_volume(volumes):
    """Compute log volume using GPU if available"""
    if CUDA_AVAILABLE and len(volumes) > 1000:
        try:
            volumes_t = torch.tensor(volumes, dtype=torch.float32, device=DEVICE)
            volumes_t = torch.where(volumes_t > 0, volumes_t, torch.tensor(float('nan'), device=DEVICE))
            return torch.log(volumes_t).cpu().numpy()
        except:
            pass
    return np.log(np.where(volumes > 0, volumes, np.nan))

def compute_z_score_global(prices):
    """Compute Z-Score using entire file's mean and std"""
    if CUDA_AVAILABLE and len(prices) > 1000:
        try:
            prices_t = torch.tensor(prices, dtype=torch.float32, device=DEVICE)
            mean = torch.nanmean(prices_t)
            std = torch.std(prices_t[~torch.isnan(prices_t)])
            z_scores = (prices_t - mean) / std
            return z_scores.cpu().numpy()
        except:
            pass
    mean = np.nanmean(prices)
    std = np.nanstd(prices)
    return (prices - mean) / std if std > 0 else np.zeros_like(prices)

def compute_price_percentile(prices):
    """Compute price percentile rank (0 to 1) for each price in the entire file"""
    if CUDA_AVAILABLE and len(prices) > 1000:
        try:
            prices_t = torch.tensor(prices, dtype=torch.float32, device=DEVICE)
            valid_mask = ~torch.isnan(prices_t)
            valid_prices = prices_t[valid_mask]

            percentiles = torch.zeros_like(prices_t)
            for i in range(len(prices_t)):
                if valid_mask[i]:
                    percentiles[i] = (valid_prices < prices_t[i]).float().mean()

            return percentiles.cpu().numpy()
        except:
            pass

    valid_prices = prices[~np.isnan(prices)]
    percentiles = np.zeros_like(prices, dtype=float)
    for i, price in enumerate(prices):
        if not np.isnan(price):
            percentiles[i] = (valid_prices < price).sum() / len(valid_prices)
    return percentiles

def compute_volume_intensity(volumes):
    """Compute volume intensity: Volume_t / mean_volume_file"""
    if CUDA_AVAILABLE and len(volumes) > 1000:
        try:
            volumes_t = torch.tensor(volumes, dtype=torch.float32, device=DEVICE)
            mean_volume = torch.nanmean(volumes_t)
            intensity = volumes_t / mean_volume
            return intensity.cpu().numpy()
        except:
            pass
    mean_volume = np.nanmean(volumes)
    return volumes / mean_volume if mean_volume > 0 else np.zeros_like(volumes)

def compute_cumulative_return(prices):
    """Compute cumulative return from first price: (Price_t / Price_start) - 1"""
    if CUDA_AVAILABLE and len(prices) > 1000:
        try:
            prices_t = torch.tensor(prices, dtype=torch.float32, device=DEVICE)
            first_valid_idx = (~torch.isnan(prices_t)).nonzero()[0]
            first_price = prices_t[first_valid_idx]
            cum_return = (prices_t / first_price) - 1.0
            return cum_return.cpu().numpy()
        except:
            pass

    # Find first valid price
    valid_mask = ~np.isnan(prices)
    if not valid_mask.any():
        return np.full_like(prices, np.nan)

    first_price = prices[valid_mask][0]
    return (prices / first_price) - 1.0

def clean_dataframe(df):
    """Clean DataFrame by removing multi-index columns and duplicates"""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.loc[:, ~df.columns.duplicated()]

    # Keep standard columns + computed columns if they exist
    standard_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    computed_cols = ['Daily Return', 'Log Return', 'Log Volume', 'Z Score Global',
                     'Price Percentile', 'Volume Intensity', 'Cumulative Return']
    keep_cols = [col for col in standard_cols + computed_cols if col in df.columns]

    # Only filter if there are extra columns beyond standard + computed
    if len(df.columns) > len(keep_cols):
        df = df[keep_cols]

    return df

def write_outputs(df, csv_path, parquet_path):
    """Write CSV and Parquet in parallel using nested ThreadPool"""
    with ThreadPoolExecutor(max_workers=2) as writer:
        csv_future = writer.submit(df.to_csv, csv_path)
        parquet_future = writer.submit(df.to_parquet, parquet_path)
        csv_future.result()
        parquet_future.result()

def process_file(file_path):
    """Process a single parquet file: clean data, add features incrementally"""
    try:
        df = pd.read_parquet(file_path)

        df = clean_dataframe(df)

        # Check if all required columns exist
        required_cols = ['Daily Return', 'Log Return', 'Log Volume', 'Z Score Global',
                        'Price Percentile', 'Volume Intensity', 'Cumulative Return']
        existing_cols = {col: col in df.columns for col in required_cols}

        # Check if LAST row (most recent data) has ALL computed features
        last_row_complete = False
        if all(existing_cols.values()) and len(df) > 0:
            last_row_complete = all(pd.notna(df[col].iloc[-1]) for col in required_cols)

        # If last row is complete, file is up to date - skip
        if last_row_complete:
            return (file_path.name, "skip", "complete")

        needs_processing = False

        # Extract price and volume data once to avoid multiple extractions
        if 'Close' in df.columns:
            close_prices = pd.to_numeric(df['Close'], errors='coerce').fillna(0).values
        if 'Volume' in df.columns:
            volumes = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).values

        # Add or recalculate Daily Return if needed
        if 'Close' in df.columns:
            if not existing_cols['Daily Return'] or pd.isna(df.get('Daily Return', pd.Series([np.nan])).iloc[-1]):
                df['Daily Return'] = compute_returns(close_prices)
                needs_processing = True

        # Add or recalculate Log Return if needed
        if 'Close' in df.columns:
            if not existing_cols['Log Return'] or pd.isna(df.get('Log Return', pd.Series([np.nan])).iloc[-1]):
                df['Log Return'] = compute_log_returns(close_prices)
                needs_processing = True

        # Add or recalculate Log Volume if needed
        if 'Volume' in df.columns:
            if not existing_cols['Log Volume'] or pd.isna(df.get('Log Volume', pd.Series([np.nan])).iloc[-1]):
                df['Log Volume'] = compute_log_volume(volumes)
                needs_processing = True

        # Add or recalculate Z Score Global if needed
        if 'Close' in df.columns:
            if not existing_cols['Z Score Global'] or pd.isna(df.get('Z Score Global', pd.Series([np.nan])).iloc[-1]):
                df['Z Score Global'] = compute_z_score_global(close_prices)
                needs_processing = True

        # Add or recalculate Price Percentile if needed
        if 'Close' in df.columns:
            if not existing_cols['Price Percentile'] or pd.isna(df.get('Price Percentile', pd.Series([np.nan])).iloc[-1]):
                df['Price Percentile'] = compute_price_percentile(close_prices)
                needs_processing = True

        # Add or recalculate Volume Intensity if needed
        if 'Volume' in df.columns:
            if not existing_cols['Volume Intensity'] or pd.isna(df.get('Volume Intensity', pd.Series([np.nan])).iloc[-1]):
                df['Volume Intensity'] = compute_volume_intensity(volumes)
                needs_processing = True

        # Add or recalculate Cumulative Return if needed
        if 'Close' in df.columns:
            if not existing_cols['Cumulative Return'] or pd.isna(df.get('Cumulative Return', pd.Series([np.nan])).iloc[-1]):
                df['Cumulative Return'] = compute_cumulative_return(close_prices)
                needs_processing = True

        if not needs_processing:
            return (file_path.name, "skip", "complete")

        # Update files in place - both CSV and Parquet
        csv_path = Path(file_path).with_suffix('.csv')
        parquet_path = Path(file_path)

        # Write CSV and Parquet in parallel
        write_outputs(df, csv_path, parquet_path)

        return (file_path.name, "success", len(df))

    except Exception as e:
        return (file_path.name, "error", str(e))

def process_folder(folder_path, folder_name):
    """Process all Parquet files in a folder using parallel workers"""
    if not os.path.exists(folder_path):
        return None

    parquet_files = list(Path(folder_path).glob("*.parquet"))

    if not parquet_files:
        return None

    print(f"\n  {folder_name} ({len(parquet_files)} files)...")

    stats = {'success': 0, 'skip': 0, 'error': 0, 'error_details': []}

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {executor.submit(process_file, parquet_file): parquet_file for parquet_file in parquet_files}

        completed = 0
        for future in as_completed(futures):
            filename, status, details = future.result()
            completed += 1

            if status == "success":
                stats['success'] += 1
            elif status == "skip":
                stats['skip'] += 1
            elif status == "error":
                stats['error'] += 1
                if len(stats['error_details']) < 5:
                    stats['error_details'].append((filename, details))

            if completed % BATCH_SIZE == 0:
                print(f"    Progress: {completed}/{len(parquet_files)} files")
            elif completed == len(parquet_files):
                print(f"    Progress: {completed}/{len(parquet_files)} files")

    print(f"\n    ✓ {stats['success']} processed | ⊘ {stats['skip']} skipped | ✗ {stats['error']} errors")

    if stats['error_details']:
        print(f"    First {len(stats['error_details'])} errors:")
        for filename, error in stats['error_details']:
            print(f"      {filename}: {error}")

    return stats

def main():
    print("=" * 70)
    print("Feature Engineering: Returns, Volume & File-Wide Statistics")
    print("=" * 70)
    print(f"Workers: {PARALLEL_WORKERS} | Batch: {BATCH_SIZE}\n")

    total_stats = {'success': 0, 'skip': 0, 'error': 0}

    for asset_type, timeframes in ASSET_TYPES.items():
        print(f"\n{'='*70}")
        print(f"{asset_type}")
        print(f"{'='*70}")

        for timeframe in timeframes:
            folder_path = os.path.join(asset_type, timeframe)
            folder_name = f"{asset_type}/{timeframe}"

            stats = process_folder(folder_path, folder_name)

            if stats:
                total_stats['success'] += stats['success']
                total_stats['skip'] += stats['skip']
                total_stats['error'] += stats['error']

    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"Processed:  {total_stats['success']}")
    print(f"Skipped:    {total_stats['skip']}")
    print(f"Errors:     {total_stats['error']}")
    print(f"\nOutput: CSV and Parquet files updated in place (STOCKS/, ETFs/)")

    if CUDA_AVAILABLE:
        torch.cuda.empty_cache()
        print(f"GPU cache cleared")

    print(f"{'='*70}")

if __name__ == "__main__":
    main()
