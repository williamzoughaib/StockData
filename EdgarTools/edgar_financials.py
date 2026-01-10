"""
SEC EDGAR 10-K/10-Q Extraction for Active Stocks
- Uses tickers from HistoricalStockData/STOCKS/1yr folder
- Ensures only actively traded stocks (no delisted companies)
- Extracts financials without XBRL using edgartools built-in parsing
- Supports incremental updates (only fetches new filings)
"""

from edgar import set_identity, Company
import pandas as pd
import os
from pathlib import Path

# Set identity (required by SEC)
set_identity("williamzoughaib@gmail.com")

# Configuration
OUTPUT_BASE_DIR = "SEC_Filings_NASDAQ"


def extract_financials_from_filing(filing):
    """Extract financials using edgartools built-in methods (non-XBRL)."""
    financials_data = []

    try:
        financials = filing.financials

        if financials:
            # Balance sheet
            try:
                bs = financials.balance_sheet
                if bs is not None and not bs.empty:
                    bs_df = bs.copy()
                    bs_df['statement'] = 'Balance Sheet'
                    bs_df['filing_date'] = filing.filing_date
                    bs_df['form'] = filing.form
                    bs_df['accession'] = filing.accession_number
                    financials_data.append(bs_df)
            except:
                pass

            # Income statement
            try:
                income = financials.income_statement
                if income is not None and not income.empty:
                    income_df = income.copy()
                    income_df['statement'] = 'Income Statement'
                    income_df['filing_date'] = filing.filing_date
                    income_df['form'] = filing.form
                    income_df['accession'] = filing.accession_number
                    financials_data.append(income_df)
            except:
                pass

            # Cash flow
            try:
                cf = financials.cash_flow_statement
                if cf is not None and not cf.empty:
                    cf_df = cf.copy()
                    cf_df['statement'] = 'Cash Flow'
                    cf_df['filing_date'] = filing.filing_date
                    cf_df['form'] = filing.form
                    cf_df['accession'] = filing.accession_number
                    financials_data.append(cf_df)
            except:
                pass
    except:
        pass

    if financials_data:
        return pd.concat(financials_data, ignore_index=True)
    return None


def get_existing_filings(ticker_dir, form_type):
    """Get list of already downloaded filing dates for incremental updates."""
    form_dir = os.path.join(ticker_dir, form_type)
    if not os.path.exists(form_dir):
        return set()

    existing_dates = set()
    for filename in os.listdir(form_dir):
        if filename.endswith('.csv') and not filename.startswith('all_'):
            # Extract date from filename like AAPL_10k_2023-10-27.csv
            parts = filename.split('_')
            if len(parts) >= 3:
                date = parts[2].replace('.csv', '')
                existing_dates.add(date)

    return existing_dates


def process_ticker(ticker, ticker_num, total_tickers):
    """Process all filings for a single ticker with incremental updates."""

    print(f"\n[{ticker_num}/{total_tickers}] Processing {ticker}...")

    try:
        company = Company(ticker)
        ticker_dir = os.path.join(OUTPUT_BASE_DIR, ticker)
        os.makedirs(os.path.join(ticker_dir, '10-K'), exist_ok=True)
        os.makedirs(os.path.join(ticker_dir, '10-Q'), exist_ok=True)

        stats = {'num_10k': 0, 'num_10q': 0, 'new_10k': 0, 'new_10q': 0}

        # Get existing filings to avoid re-downloading
        existing_10k = get_existing_filings(ticker_dir, '10-K')
        existing_10q = get_existing_filings(ticker_dir, '10-Q')

        # Process 10-K filings
        try:
            filings_10k = company.get_filings(form="10-K", amendments=False)
            stats['num_10k'] = len(filings_10k)

            all_10k_data = []

            for filing in filings_10k:
                filing_date = str(filing.filing_date)

                # Skip if already downloaded
                if filing_date in existing_10k:
                    # Load existing file for combined dataset
                    filename = f"{ticker}_10k_{filing_date}.csv"
                    filepath = os.path.join(ticker_dir, '10-K', filename)
                    if os.path.exists(filepath):
                        existing_df = pd.read_csv(filepath)
                        all_10k_data.append(existing_df)
                    continue

                # Download new filing
                financials = extract_financials_from_filing(filing)
                if financials is not None:
                    all_10k_data.append(financials)
                    stats['new_10k'] += 1

                    # Save individual filing
                    filename = f"{ticker}_10k_{filing_date}.csv"
                    financials.to_csv(os.path.join(ticker_dir, '10-K', filename), index=False)
                    print(f"  + New 10-K: {filing_date}")

            # Update combined file
            if all_10k_data:
                combined = pd.concat(all_10k_data, ignore_index=True)
                combined.to_csv(os.path.join(ticker_dir, '10-K', f'{ticker}_all_10k.csv'), index=False)

        except Exception as e:
            print(f"  {ticker}: Error processing 10-K: {e}")

        # Process 10-Q filings
        try:
            filings_10q = company.get_filings(form="10-Q", amendments=False)
            stats['num_10q'] = len(filings_10q)

            all_10q_data = []

            for filing in filings_10q:
                filing_date = str(filing.filing_date)

                # Skip if already downloaded
                if filing_date in existing_10q:
                    # Load existing file for combined dataset
                    filename = f"{ticker}_10q_{filing_date}.csv"
                    filepath = os.path.join(ticker_dir, '10-Q', filename)
                    if os.path.exists(filepath):
                        existing_df = pd.read_csv(filepath)
                        all_10q_data.append(existing_df)
                    continue

                # Download new filing
                financials = extract_financials_from_filing(filing)
                if financials is not None:
                    all_10q_data.append(financials)
                    stats['new_10q'] += 1

                    # Save individual filing
                    filename = f"{ticker}_10q_{filing_date}.csv"
                    financials.to_csv(os.path.join(ticker_dir, '10-Q', filename), index=False)
                    print(f"  + New 10-Q: {filing_date}")

            # Update combined file
            if all_10q_data:
                combined = pd.concat(all_10q_data, ignore_index=True)
                combined.to_csv(os.path.join(ticker_dir, '10-Q', f'{ticker}_all_10q.csv'), index=False)

        except Exception as e:
            print(f"  {ticker}: Error processing 10-Q: {e}")

        if stats['new_10k'] > 0 or stats['new_10q'] > 0:
            print(f"  ✓ {ticker}: Added {stats['new_10k']} new 10-Ks, {stats['new_10q']} new 10-Qs")
        else:
            print(f"  ✓ {ticker}: Up to date ({stats['num_10k']} 10-Ks, {stats['num_10q']} 10-Qs)")

        return {'ticker': ticker, 'status': 'success', 'stats': stats}

    except Exception as e:
        print(f"  ✗ {ticker}: Error: {e}")
        return {'ticker': ticker, 'status': 'failed', 'error': str(e)}


def get_stock_tickers_from_folder():
    """Get stock tickers from HistoricalStockData/STOCKS/1yr folder."""
    stocks_dir = Path(__file__).parent.parent / 'HistoricalStockData' / 'STOCKS' / '1yr'

    if not stocks_dir.exists():
        print(f"ERROR: Directory not found: {stocks_dir}")
        return []

    # Get all CSV files and extract ticker symbols
    tickers = []
    for file in stocks_dir.glob('*.csv'):
        ticker = file.stem  # Get filename without .csv extension
        tickers.append(ticker)

    return sorted(tickers)


def update_all_filings():
    """
    Incremental update function - checks all tickers and adds only new filings.
    Can be run repeatedly to keep data up to date.
    """
    print("=" * 80)
    print("SEC EDGAR Filings Update - Active Stocks from HistoricalStockData")
    print("=" * 80)

    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    # Get stock tickers from existing CSV files
    print("\nLoading stock tickers from HistoricalStockData/STOCKS/1yr...")
    all_symbols = get_stock_tickers_from_folder()

    if not all_symbols:
        print("ERROR: No stock tickers found!")
        return

    print(f"Found {len(all_symbols)} stocks from your existing data")

    # Process each ticker
    results = {'success': 0, 'failed': 0, 'total_new_10k': 0, 'total_new_10q': 0}

    for i, ticker in enumerate(all_symbols, 1):
        result = process_ticker(ticker, i, len(all_symbols))

        if result['status'] == 'success':
            results['success'] += 1
            results['total_new_10k'] += result['stats'].get('new_10k', 0)
            results['total_new_10q'] += result['stats'].get('new_10q', 0)
        else:
            results['failed'] += 1

        # Progress every 10 companies
        if i % 10 == 0:
            print(f"\nProgress: {i}/{len(all_symbols)} | Success: {results['success']} | Failed: {results['failed']}")
            print(f"New filings: {results['total_new_10k']} 10-Ks, {results['total_new_10q']} 10-Qs")

    # Final summary
    print("\n" + "=" * 80)
    print("UPDATE COMPLETE")
    print("=" * 80)
    print(f"Total symbols: {len(all_symbols)}")
    print(f"  Successful: {results['success']}")
    print(f"  Failed: {results['failed']}")
    print(f"\nNew filings added:")
    print(f"  10-K filings: {results['total_new_10k']}")
    print(f"  10-Q filings: {results['total_new_10q']}")
    print(f"\nOutput: {OUTPUT_BASE_DIR}/")
    print("=" * 80)


if __name__ == "__main__":
    update_all_filings()
