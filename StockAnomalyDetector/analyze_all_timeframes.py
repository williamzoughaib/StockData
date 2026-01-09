"""
Universal Stock Data Analysis - All Timeframes

Single script to analyze 1yr, 5yr, 10yr, or all-time data.
Usage:
    python analyze_all_timeframes.py --timeframe 5yr --tickers AAPL MSFT
    python analyze_all_timeframes.py --timeframe all  (analyzes all timeframes)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import argparse

# Base directory
BASE_DIR = Path(r"E:\FinTechTradingPlatform\StockData\HistoricalStockData")

# Timeframe configurations
TIMEFRAMES = {
    '1yr': '1yr_data',
    '5yr': '5yr_data',
    '10yr': '10yr_data',
    'all': 'all_data'
}

# Default tickers
DEFAULT_TICKERS = ['LLY', 'NVDA', 'AAL', 'MVIS']


def load_and_analyze(ticker, timeframe_name, timeframe_folder):
    """Load and analyze a single stock."""
    data_path = BASE_DIR / timeframe_folder / f"{ticker}.csv"

    if not data_path.exists():
        print(f"  {ticker}: File not found")
        return None

    # Load data (skip Ticker/Date rows)
    df = pd.read_csv(data_path, skiprows=[1, 2], index_col=0, parse_dates=True)

    # Convert to numeric
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Print stats
    print(f"  {ticker}:")
    print(f"    Records: {len(df)}")
    print(f"    Range: {df.index.min().date()} to {df.index.max().date()}")
    print(f"    Latest close: ${df['Close'].iloc[-1]:.2f}")

    if 'Daily_Return' in df.columns:
        print(f"    Avg return: {df['Daily_Return'].mean()*100:.3f}%")

    return df


def plot_analysis(ticker, df, timeframe_name):
    """Create analysis plots."""
    fig, axes = plt.subplots(3, 1, figsize=(14, 10))
    fig.suptitle(f'{ticker} - {timeframe_name.upper()} Analysis', fontsize=16, fontweight='bold')

    # Price
    axes[0].plot(df.index, df['Close'], color='blue', linewidth=1.5)
    axes[0].set_title('Close Price')
    axes[0].set_ylabel('Price ($)')
    axes[0].grid(True, alpha=0.3)

    # Daily Returns
    if 'Daily_Return' in df.columns:
        axes[1].plot(df.index, df['Daily_Return'], color='green', alpha=0.7)
        axes[1].axhline(y=0, color='red', linestyle='--', alpha=0.5)
        axes[1].set_title('Daily Returns')
        axes[1].set_ylabel('Return')
        axes[1].grid(True, alpha=0.3)

    # Log Volume
    if 'Log_Volume' in df.columns:
        axes[2].plot(df.index, df['Log_Volume'], color='orange', alpha=0.7)
        axes[2].set_title('Log Volume')
        axes[2].set_ylabel('Log(Volume)')
        axes[2].set_xlabel('Date')
        axes[2].grid(True, alpha=0.3)

    plt.tight_layout()

    # Save
    output_dir = Path('plots')
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / f"{ticker}_{timeframe_name}_analysis.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"    Saved: {output_path}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(description='Analyze stock data')
    parser.add_argument('--timeframe', choices=list(TIMEFRAMES.keys()) + ['all'],
                       default='5yr', help='Timeframe to analyze')
    parser.add_argument('--tickers', nargs='+', default=DEFAULT_TICKERS,
                       help='Tickers to analyze')
    parser.add_argument('--no-plots', action='store_true', help='Skip creating plots')

    args = parser.parse_args()

    # Determine timeframes to process
    if args.timeframe == 'all':
        timeframes_to_process = list(TIMEFRAMES.items())
    else:
        timeframes_to_process = [(args.timeframe, TIMEFRAMES[args.timeframe])]

    print("=" * 60)
    print("Stock Data Analysis")
    print("=" * 60)

    for tf_name, tf_folder in timeframes_to_process:
        print(f"\n{tf_name.upper()} Analysis")
        print(f"Source: {BASE_DIR / tf_folder}")
        print("-" * 60)

        for ticker in args.tickers:
            df = load_and_analyze(ticker, tf_name, tf_folder)

            if df is not None and not args.no_plots:
                plot_analysis(ticker, df, tf_name)

    print("\n" + "=" * 60)
    print("Complete!")


if __name__ == "__main__":
    main()
