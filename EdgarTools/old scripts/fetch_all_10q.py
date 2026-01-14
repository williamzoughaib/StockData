"""
Fetch 10-Q and 10-K XBRL filings for all SEC stocks

This script fetches 10-Q and 10-K filings for all tickers organized by SEC office,
storing both filing types in the same ticker folder.
"""

import pandas as pd
import os
import time
from pathlib import Path
from edgar import *
from edgar.reference.tickers import sector_SEC_stocks
from edgar.xbrl import XBRLS

# Set your identity for SEC EDGAR access
set_identity("williamzoughaib@gmail.com")

# Base output directory
OUTPUT_BASE = "FinancialFilings"


def create_ticker_folder(office, ticker):
    """
    Create a folder for a ticker within its office folder

    Args:
        office: SEC office name
        ticker: Stock ticker symbol

    Returns:
        Path to the ticker folder
    """
    ticker_path = os.path.join(OUTPUT_BASE, office, ticker)
    os.makedirs(ticker_path, exist_ok=True)
    return ticker_path


def process_filing_type(ticker, company, form_type, ticker_folder):
    """
    Process a specific filing type for a ticker

    Args:
        ticker: Stock ticker symbol
        company: Company object
        form_type: Filing type ('10-Q' or '10-K')
        ticker_folder: Path to ticker folder

    Returns:
        int: Number of files saved
    """
    files_saved = 0

    try:
        # Fetch filings
        filings = company.get_filings(form=form_type)

        if filings is None or len(filings) == 0:
            print(f"    ⚠️  No {form_type} filings found")
            return 0

        print(f"    📄 Found {len(filings)} {form_type} filings")

        # Process XBRL data
        xbrls = XBRLS.from_filings(filings)

        # Extract financial statements
        statements_to_extract = {
            'income_statement': xbrls.statements.income_statement,
            'balance_sheet': xbrls.statements.balance_sheet,
            'cash_flow': xbrls.statements.cash_flow_statement,
        }

        for statement_name, statement_func in statements_to_extract.items():
            try:
                statement = statement_func()
                if statement is not None:
                    df = statement.to_dataframe()
                    if df is not None and not df.empty:
                        # Clean data
                        df = df.replace("", pd.NA)

                        # Save as parquet
                        form_label = form_type.replace("-", "")  # "10-Q" -> "10Q"
                        output_file = os.path.join(
                            ticker_folder,
                            f"{ticker}_{form_label}_{statement_name}.parquet"
                        )
                        df.to_parquet(output_file, index=False)
                        files_saved += 1
                        print(f"      ✅ Saved {statement_name}")

            except Exception as e:
                print(f"      ⚠️  Error extracting {statement_name}: {e}")

        # Save combined data
        try:
            combined_df = pd.concat([
                xbrls.statements.income_statement().to_dataframe().assign(statement="income_statement"),
                xbrls.statements.balance_sheet().to_dataframe().assign(statement="balance_sheet"),
                xbrls.statements.cash_flow_statement().to_dataframe().assign(statement="cash_flow"),
            ], ignore_index=True)

            combined_df = combined_df.replace("", pd.NA)

            form_label = form_type.replace("-", "")  # "10-Q" -> "10Q"
            combined_file = os.path.join(ticker_folder, f"{ticker}_{form_label}_combined.parquet")
            combined_df.to_parquet(combined_file, index=False)

            files_saved += 1
            print(f"      ✅ Saved combined data")

        except Exception as e:
            print(f"      ⚠️  Error saving combined data: {e}")

    except Exception as e:
        print(f"    ❌ Error processing {form_type}: {e}")

    return files_saved


def process_ticker(ticker, office, index, total):
    """
    Process a single ticker - fetch 10-Q and 10-K filings and save to folder

    Args:
        ticker: Stock ticker symbol
        office: SEC office name
        index: Current index in the list
        total: Total number of tickers

    Returns:
        dict: Status information
    """
    print(f"\n[{index}/{total}] Processing {ticker} ({office})...")

    result = {
        'ticker': ticker,
        'office': office,
        'success': False,
        'filings_10q': 0,
        'filings_10k': 0,
        'error': None
    }

    try:
        # Create ticker folder
        ticker_folder = create_ticker_folder(office, ticker)

        # Check if already processed (has files)
        existing_files = list(Path(ticker_folder).glob("*.parquet"))
        if existing_files:
            print(f"  ⏭️  Skipping {ticker} - already has {len(existing_files)} files")
            result['success'] = True
            result['filings_10q'] = len([f for f in existing_files if '10Q' in f.name])
            result['filings_10k'] = len([f for f in existing_files if '10K' in f.name])
            result['skipped'] = True
            return result

        # Get company
        company = Company(ticker)

        # Process 10-Q filings
        print(f"  📊 Processing 10-Q filings...")
        files_10q = process_filing_type(ticker, company, "10-Q", ticker_folder)
        result['filings_10q'] = files_10q

        # Process 10-K filings
        print(f"  📊 Processing 10-K filings...")
        files_10k = process_filing_type(ticker, company, "10-K", ticker_folder)
        result['filings_10k'] = files_10k

        result['success'] = True
        print(f"  ✨ Completed {ticker} - 10-Q: {result['filings_10q']} files, 10-K: {result['filings_10k']} files")

    except Exception as e:
        print(f"  ❌ Error processing {ticker}: {e}")
        result['error'] = str(e)

    return result


def save_progress(results, progress_file="filings_fetch_progress.csv"):
    """
    Save progress to a CSV file

    Args:
        results: List of result dictionaries
        progress_file: Path to progress file
    """
    df = pd.DataFrame(results)
    df.to_csv(progress_file, index=False)
    print(f"\n💾 Progress saved to {progress_file}")


def main():
    print("=" * 70)
    print("10-Q and 10-K XBRL Filings Fetcher for All SEC Stocks")
    print("=" * 70)
    print("Organizing by SEC Office")
    print()

    # Create output directory
    os.makedirs(OUTPUT_BASE, exist_ok=True)

    # Load SEC stocks organized by office
    print("Loading SEC stocks by office...")
    stocks = sector_SEC_stocks()

    # Check what columns we have
    print(f"Columns in stocks: {stocks.columns.tolist()}")

    # Get ticker-office pairs
    ticker_office_pairs = []

    if 'Office' in stocks.columns and 'Ticker' in stocks.columns:
        # Drop rows with missing values
        stocks_clean = stocks[stocks['Office'].notna() & stocks['Ticker'].notna()]
        for _, row in stocks_clean.iterrows():
            ticker_office_pairs.append((row['Ticker'], row['Office']))
    else:
        raise ValueError(f"Expected 'Office' and 'Ticker' columns. Got: {stocks.columns.tolist()}")

    total_tickers = len(ticker_office_pairs)
    print(f"Found {total_tickers} tickers across offices\n")

    # Show office distribution
    office_counts = {}
    for ticker, office in ticker_office_pairs:
        office_counts[office] = office_counts.get(office, 0) + 1

    print("Tickers per office:")
    for office, count in sorted(office_counts.items()):
        print(f"  {office}: {count} tickers")

    # Check for existing progress
    progress_file = "filings_fetch_progress.csv"
    results = []

    if os.path.exists(progress_file):
        print(f"\n📂 Found existing progress file: {progress_file}")
        progress_df = pd.read_csv(progress_file)
        results = progress_df.to_dict('records')
        processed_tickers = set(progress_df['ticker'].tolist())
        ticker_office_pairs = [(t, o) for t, o in ticker_office_pairs if t not in processed_tickers]
        print(f"✅ Already processed: {len(processed_tickers)} tickers")
        print(f"⏳ Remaining: {len(ticker_office_pairs)} tickers\n")

    if len(ticker_office_pairs) == 0:
        print("✨ All tickers already processed!")
        return

    input("Press Enter to start processing (Ctrl+C to cancel)...")

    # Process each ticker
    start_time = time.time()

    try:
        for i, (ticker, office) in enumerate(ticker_office_pairs, 1):
            result = process_ticker(ticker, office, i, len(ticker_office_pairs))
            results.append(result)

            # Save progress every 10 tickers
            if i % 10 == 0:
                save_progress(results, progress_file)

                # Print progress summary
                elapsed = time.time() - start_time
                avg_time_per_ticker = elapsed / i
                remaining = len(ticker_office_pairs) - i
                eta_seconds = remaining * avg_time_per_ticker
                print(f"\n📊 Progress: {i}/{len(ticker_office_pairs)} ({i/len(ticker_office_pairs)*100:.1f}%)")
                print(f"   Elapsed: {elapsed/60:.1f} min | ETA: {eta_seconds/60:.1f} min\n")

    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        save_progress(results, progress_file)

    # Save final progress
    save_progress(results, progress_file)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    results_df = pd.DataFrame(results)
    successful = results_df[results_df['success'] == True]
    failed = results_df[results_df['success'] == False]

    print(f"\n✅ Successful: {len(successful)}")
    print(f"❌ Failed: {len(failed)}")

    if len(successful) > 0:
        total_10q = successful['filings_10q'].sum()
        total_10k = successful['filings_10k'].sum()
        print(f"\n📊 Total 10-Q files: {total_10q}")
        print(f"📊 Total 10-K files: {total_10k}")
        print(f"📁 Files organized in: {OUTPUT_BASE}/[Office]/[Ticker]/")

        # Summary by office
        print(f"\n📋 Summary by Office:")
        for office in sorted(successful['office'].unique()):
            office_stocks = successful[successful['office'] == office]
            print(f"  {office}: {len(office_stocks)} tickers processed")

    if len(failed) > 0:
        print(f"\n❌ Failed tickers:")
        for _, row in failed.iterrows():
            print(f"   {row['ticker']} ({row.get('office', 'Unknown')}): {row.get('error', 'Unknown error')}")

    total_time = time.time() - start_time
    print(f"\n⏱️  Total time: {total_time/60:.1f} minutes ({total_time/3600:.2f} hours)")
    print("=" * 70)


if __name__ == "__main__":
    main()
