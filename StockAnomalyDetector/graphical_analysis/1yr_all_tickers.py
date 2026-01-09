"""
Graphical Analysis Utilizing Daily Returns and Log Volume
Script Analyzes 1 yr historical stock data for ALL tickers in folder.
Identifies undervalued stocks based on technical indicators.
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path
import matplotlib.pyplot as plt

# Root directory and data path
ROOT_DIR = Path(r"E:\FinTechTradingPlatform\StockData\HistoricalStockData")
DATA_PATH = ROOT_DIR / "1yr_data"

# Store loaded dataframes
dfs = {}

print(f"Loading data from: {DATA_PATH}\n")

# Get all CSV files from folder
csv_files = list(DATA_PATH.glob("*.csv"))
TICKER_NAMES = [f.stem for f in csv_files]  # Extract ticker names from filenames
print(f"Found {len(TICKER_NAMES)} tickers in folder")
print(f"Analyzing all tickers...\n")

# Load each ticker
for ticker in TICKER_NAMES:
    csv_path = DATA_PATH / f"{ticker}.csv"

    if not csv_path.exists():
        print(f"{ticker}: File not found, skipping")
        continue

    try:
        # Load CSV (skip Ticker/Date rows)
        df = pd.read_csv(csv_path, skiprows=[1, 2], index_col=0, parse_dates=True)

        # Convert to numeric
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        dfs[ticker] = df
    except Exception as e:
        print(f"{ticker}: Error loading - {e}")

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


# Add indicators to each dataframe and collect undervalued candidates
undervalued_stocks = []

print("\n" + "="*60)
print("CALCULATING TECHNICAL INDICATORS")
print("="*60)

for ticker_name, df in dfs.items():
    try:
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

        # Check for undervaluation signals (STRINGENT CRITERIA)
        signals = []
        signal_count = 0

        # Must be oversold on RSI
        if df['RSI'].iloc[-1] < 30:
            signals.append("RSI<30")
            signal_count += 1

        # Significantly below moving average
        if df['Distance_from_MA50'].iloc[-1] < -15:
            signals.append("Below MA50 >15%")
            signal_count += 1

        # Major drawdown from peak
        if df['Drawdown'].iloc[-1] < -40:
            signals.append("40%+ Drawdown")
            signal_count += 1

        # Oversold on Stochastic
        if df['Stochastic'].iloc[-1] < 20:
            signals.append("Stoch<20")
            signal_count += 1

        # Additional filter: Price near 52-week low
        near_52w_low = df['Distance_from_52W_Low'].iloc[-1] < 15  # Within 15% of 52W low
        if near_52w_low:
            signals.append("Near 52W Low")
            signal_count += 1

        # MACD showing potential reversal (MACD > Signal)
        macd_bullish = df['MACD'].iloc[-1] > df['MACD_Signal'].iloc[-1]
        if macd_bullish and signal_count >= 2:
            signals.append("MACD Bullish")
            signal_count += 0.5  # Bonus signal

        # Store stocks with 3+ signals (more stringent)
        if signal_count >= 2:
            undervalued_stocks.append({
                'Ticker': ticker_name,
                'Price': df['Close'].iloc[-1],
                'RSI': df['RSI'].iloc[-1],
                'Dist_MA50': df['Distance_from_MA50'].iloc[-1],
                'Drawdown': df['Drawdown'].iloc[-1],
                'Stochastic': df['Stochastic'].iloc[-1],
                'Signals': ', '.join(signals),
                'Signal_Count': signal_count
            })

    except Exception as e:
        print(f"{ticker_name}: Error calculating indicators - {e}")

print(f"\nProcessed {len(dfs)} tickers")

# Display undervalued candidates
print("\n" + "="*60)
print("POTENTIALLY UNDERVALUED STOCKS (3+ signals)")
print("="*60)

if undervalued_stocks:
    # Sort by signal count (descending)
    undervalued_stocks.sort(key=lambda x: x['Signal_Count'], reverse=True)

    print(f"\nFound {len(undervalued_stocks)} potentially undervalued stocks:\n")

    for stock in undervalued_stocks:
        print(f"{stock['Ticker']:6} | Price: ${stock['Price']:7.2f} | Signals: {stock['Signal_Count']}")
        print(f"         RSI: {stock['RSI']:5.1f} | Dist MA50: {stock['Dist_MA50']:6.1f}% | Drawdown: {stock['Drawdown']:6.1f}%")
        print(f"         Stoch: {stock['Stochastic']:5.1f} | {stock['Signals']}")
        print()

    # Export to CSV
    output_df = pd.DataFrame(undervalued_stocks)
    output_file = "undervalued_stocks_1yr.csv"
    output_df.to_csv(output_file, index=False)
    print(f"Results saved to: {output_file}")
else:
    print("\nNo stocks meet the stringent undervaluation criteria (3+ signals)")

print("\n" + "="*60)
