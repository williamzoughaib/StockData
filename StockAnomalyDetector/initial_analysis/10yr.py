"""
Graphical Analysis Utilizing Daily Returns and Log Volume
Script Analyzes 1 yr historical stock data for any given tickers.
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path
import matplotlib.pyplot as plt

# Root directory and data path (like data/raw in the example)
ROOT_DIR = Path(r"E:\FinTechTradingPlatform\StockData\HistoricalStockData")
DATA_PATH = ROOT_DIR / "10yr_data"  # Equivalent to data/raw in example

# Tickers to analyze
TICKER_NAMES = ['LLY', 'NVDA', 'AAL', 'MVIS']

# Store loaded dataframes
dfs = {}

print(f"Loading data from: {DATA_PATH}\n")

# Load each ticker
for ticker in TICKER_NAMES:
    csv_path = DATA_PATH / f"{ticker}.csv"

    if not csv_path.exists():
        print(f"{ticker}: File not found, skipping")
        continue

    # Load CSV (skip Ticker/Date rows)
    df = pd.read_csv(csv_path, skiprows=[1, 2], index_col=0, parse_dates=True)

    # Convert to numeric
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    dfs[ticker] = df
    print(f"{ticker}: Loaded {len(df)} rows from {df.index.min().date()} to {df.index.max().date()}")

print(f"\n{len(dfs)} tickers loaded successfully")

# Plot analysis for each ticker
for ticker_name, df in dfs.items():
    fig, axes = plt.subplots(3, 1, figsize=(14, 10))
    fig.suptitle(f'{ticker_name} - 10 Year Analysis', fontsize=16, fontweight='bold')

    # Plot 1: Close Price
    axes[0].plot(df.index, df['Close'], color='blue', linewidth=1.5)
    axes[0].set_title('Close Price')
    axes[0].set_ylabel('Price ($)')
    axes[0].grid(True, alpha=0.3)

    # Plot 2: Daily Return
    if 'Daily_Return' in df.columns:
        axes[1].plot(df.index, df['Daily_Return'], color='green', alpha=0.7)
        axes[1].axhline(y=0, color='red', linestyle='--', alpha=0.5)
        axes[1].set_title('Daily Returns')
        axes[1].set_ylabel('Return')
        axes[1].grid(True, alpha=0.3)

    # Plot 3: Log Volume
    if 'Log_Volume' in df.columns:
        axes[2].plot(df.index, df['Log_Volume'], color='orange', alpha=0.7)
        axes[2].set_title('Log Volume')
        axes[2].set_ylabel('Log(Volume)')
        axes[2].set_xlabel('Date')
        axes[2].grid(True, alpha=0.3)

    plt.tight_layout()

    # Save plot
    output_file = f"{ticker_name}_10yr_analysis.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()

print("\nAnalysis complete!")
