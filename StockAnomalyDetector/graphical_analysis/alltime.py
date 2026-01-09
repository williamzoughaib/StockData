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
DATA_PATH = ROOT_DIR / "all_data"  # Equivalent to data/raw in example

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

# Analyze price change from first day
plt.figure(figsize=(15, 7))
for ticker_name, df in dfs.items():
    plt.plot(df['Close'], label=ticker_name, linewidth=1.5)
plt.xlabel('Date')
plt.ylabel('Price ($)')
plt.title('Price Over All Time')
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()
plt.show()


# Analyze percentage change from first day
for ticker_name, df in dfs.items():
    pct_change = ((df['Close'] / df['Close'].iloc[0]) - 1) * 100
    plt.plot(pct_change, label=ticker_name, linewidth=1.5)
plt.xlabel('Date')
plt.ylabel('Percentage Change (%)')
plt.title('Percentage Change Over All Time')
plt.legend()
plt.grid(alpha=0.3)
plt.axhline(y=0, color='black', linestyle='--', alpha=0.5)  # Reference line at 0%
plt.tight_layout()
plt.show()