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
DATA_PATH = ROOT_DIR / "1yr_data"  # Equivalent to data/raw in example

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


# ============================================================================
# CALCULATE TECHNICAL INDICATORS (1 YEAR TIMEFRAME)
# ============================================================================

def calculate_rsi(prices, period=14):
    """Calculate RSI indicator."""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_bollinger_bands(prices, period=20, std_dev=2):
    """Calculate Bollinger Bands."""
    sma = prices.rolling(window=period).mean()
    std = prices.rolling(window=period).std()
    upper_band = sma + (std * std_dev)
    lower_band = sma - (std * std_dev)
    return sma, upper_band, lower_band


def calculate_macd(prices, fast=12, slow=26, signal=9):
    """Calculate MACD indicator."""
    ema_fast = prices.ewm(span=fast, adjust=False).mean()
    ema_slow = prices.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    histogram = macd - signal_line
    return macd, signal_line, histogram


def calculate_stochastic(high, low, close, period=14):
    """Calculate Stochastic Oscillator."""
    lowest_low = low.rolling(window=period).min()
    highest_high = high.rolling(window=period).max()
    stoch = 100 * (close - lowest_low) / (highest_high - lowest_low)
    return stoch


def calculate_atr(high, low, close, period=14):
    """Calculate Average True Range."""
    high_low = high - low
    high_close = np.abs(high - close.shift())
    low_close = np.abs(low - close.shift())
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()
    return atr


# Add indicators to each dataframe
for ticker_name, df in dfs.items():
    print(f"\nCalculating indicators for {ticker_name}...")

    # Moving Averages (1yr: 20-day, 50-day)
    df['MA_20'] = df['Close'].rolling(window=20).mean()
    df['MA_50'] = df['Close'].rolling(window=50).mean()

    # RSI (14-day)
    df['RSI'] = calculate_rsi(df['Close'], period=14)

    # Bollinger Bands (20-day)
    df['BB_Middle'], df['BB_Upper'], df['BB_Lower'] = calculate_bollinger_bands(df['Close'], period=20)

    # MACD
    df['MACD'], df['MACD_Signal'], df['MACD_Histogram'] = calculate_macd(df['Close'])

    # Stochastic Oscillator
    df['Stochastic'] = calculate_stochastic(df['High'], df['Low'], df['Close'], period=14)

    # ATR (14-day)
    df['ATR'] = calculate_atr(df['High'], df['Low'], df['Close'], period=14)

    # Distance from 50-day MA (%)
    df['Distance_from_MA50'] = ((df['Close'] - df['MA_50']) / df['MA_50']) * 100

    # 52-week High/Low
    df['52W_High'] = df['Close'].rolling(window=252, min_periods=1).max()
    df['52W_Low'] = df['Close'].rolling(window=252, min_periods=1).min()
    df['Distance_from_52W_Low'] = ((df['Close'] - df['52W_Low']) / (df['52W_High'] - df['52W_Low'])) * 100

    # Drawdown from Peak
    df['Drawdown'] = ((df['Close'] - df['52W_High']) / df['52W_High']) * 100

    # Volume metrics
    df['Volume_MA_20'] = df['Volume'].rolling(window=20).mean()
    df['Volume_Ratio'] = df['Volume'] / df['Volume_MA_20']

    print(f"  Latest RSI: {df['RSI'].iloc[-1]:.2f}")
    print(f"  Latest Distance from MA50: {df['Distance_from_MA50'].iloc[-1]:.2f}%")
    print(f"  Latest Drawdown: {df['Drawdown'].iloc[-1]:.2f}%")
    print(f"  Latest Stochastic: {df['Stochastic'].iloc[-1]:.2f}")

    # Undervaluation signals
    signals = []
    if df['RSI'].iloc[-1] < 30:
        signals.append("OVERSOLD (RSI < 30)")
    if df['Distance_from_MA50'].iloc[-1] < -10:
        signals.append("BELOW MA50 by >10%")
    if df['Drawdown'].iloc[-1] < -30:
        signals.append("30%+ DRAWDOWN")
    if df['Stochastic'].iloc[-1] < 20:
        signals.append("OVERSOLD (Stochastic < 20)")

    if signals:
        print(f"  ⚠️  POTENTIAL UNDERVALUATION SIGNALS: {', '.join(signals)}")
    else:
        print(f"  ✓ No strong undervaluation signals")

print("\n" + "="*60)

# Analyze price change from first day
plt.figure(figsize=(15, 7))
for ticker_name, df in dfs.items():
    plt.plot(df['Close'], label=ticker_name, linewidth=1.5)
plt.xlabel('Date')
plt.ylabel('Price ($)')
plt.title('Price Over 1 Year')
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
plt.title('Percentage Change Over 1 Year')
plt.legend()
plt.grid(alpha=0.3)
plt.axhline(y=0, color='black', linestyle='--', alpha=0.5)  # Reference line at 0%
plt.tight_layout()
plt.show()
