import time
import datetime
import numpy as np
from pathlib import Path
from tqdm import tqdm
from joblib import Parallel, delayed, parallel_backend
import multiprocessing
import pandas as pd

###########################
# THIS IS THE MAIN SCRIPT #
###########################

# Configuration
MONTH_CUTTOFF = 6
DAY_CUTTOFF = 5
STD_CUTTOFF = 10
MIN_STOCK_VOLUME = 100000
MIN_PRICE = 0.25
DATA_PATH = Path(__file__).parent.parent / "HistoricalStockData" / "5yr_data"

def load_ticker_data(ticker):
    """Load and filter ticker data from CSV."""
    try:
        csv_file = DATA_PATH / f"{ticker}.csv"
        if not csv_file.exists():
            return None

        # Read CSV (skip ticker and empty rows)
        data = pd.read_csv(csv_file, skiprows=[1, 2], header=0)

        # Set first column as index and parse dates
        data.set_index(data.columns[0], inplace=True)
        data.index = pd.to_datetime(data.index, format='mixed')
        data.index.name = 'Date'

        # Lowercase columns and convert to numeric
        data.columns = data.columns.str.lower()
        for col in data.columns:
            data[col] = pd.to_numeric(data[col], errors='coerce')

        # Remove NaN rows
        data = data.dropna()

        # Filter by price
        if 'close' in data.columns and len(data) > 0:
            if data.tail(1)["close"].values[0] < MIN_PRICE:
                return None

        # Filter by date
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=MONTH_CUTTOFF * 30)
        data = data[data.index >= cutoff_date]

        # Check volume column exists
        if 'volume' not in data.columns or len(data) == 0:
            return None

        return data[['volume']].rename(columns={'volume': 'Volume'})
    except Exception as e:
        return None

def find_anomalies(data, ticker):
    """Detect volume anomalies and return recent ones."""
    if data is None or len(data) == 0:
        return []

    # Calculate anomaly threshold
    mean_vol = data['Volume'].mean()
    std_vol = data['Volume'].std()
    upper_limit = mean_vol + (std_vol * STD_CUTTOFF)

    # Find anomalies above threshold
    anomalies = data[data['Volume'] > upper_limit]
    anomalies = anomalies[anomalies['Volume'] > MIN_STOCK_VOLUME]

    if len(anomalies) == 0:
        return []

    # Filter for recent anomalies (within DAY_CUTTOFF days)
    today = datetime.datetime.now()
    recent_anomalies = []

    for date, row in anomalies.iterrows():
        days_ago = (today - date).days
        if days_ago <= DAY_CUTTOFF:
            recent_anomalies.append({
                'Ticker': ticker,
                'Date': date.strftime('%Y-%m-%d'),
                'Volume': f"{row['Volume']:.0f}",
                'DaysAgo': days_ago
            })

    return recent_anomalies

def process_ticker(ticker):
    """Process a single ticker (for parallel execution)."""
    data = load_ticker_data(ticker)
    return find_anomalies(data, ticker)

def main():
    """Main function to scan all tickers."""
    # Get ticker list
    csv_files = list(DATA_PATH.glob("*.csv"))
    tickers = [f.stem for f in csv_files]

    print(f"Scanning {len(tickers)} tickers for unusual volume...")
    print(f"Parameters: STD={STD_CUTTOFF}, Min Vol={MIN_STOCK_VOLUME:,}, Min Price=${MIN_PRICE}")
    print("-" * 60)

    start_time = time.time()

    # Process tickers in parallel
    cpu_count = multiprocessing.cpu_count()
    with parallel_backend('loky', n_jobs=cpu_count):
        results = Parallel()(delayed(process_ticker)(ticker)
                           for ticker in tqdm(tickers))

    # Flatten results
    all_anomalies = [item for sublist in results for item in sublist]

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Scan completed in {elapsed:.1f} seconds")
    print(f"Found {len(all_anomalies)} unusual volume alerts")

    # Save and display results
    if all_anomalies:
        df = pd.DataFrame(all_anomalies)
        df = df.sort_values('DaysAgo')

        # Save to CSV
        df.to_csv("unusual_volume_results.csv", index=False)
        print(f"\n✓ Results saved to unusual_volume_results.csv")

        # Display top results
        print(f"\nTop 10 Recent Alerts:")
        print(df.head(10).to_string(index=False))
    else:
        print("\nNo unusual volume detected in the specified time window.")

    return all_anomalies

if __name__ == '__main__':
    main()
