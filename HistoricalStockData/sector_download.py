"""
Organize stocks into sector folders using SEC SIC codes.
Copies existing CSV files from STOCKS folders into sector-specific folders.

Structure:
SECTORS/
  Healthcare/
    1yr/
    5yr/
    10yr/
    alltime/
  Energy/
    1yr/
    ...

NOTE: First run will take ~1 hour to download comprehensive SEC dataset.
Subsequent runs will be fast (~1-2 minutes) using cached data.
"""

import os
import shutil
from pathlib import Path
from edgar import set_identity
from edgar.reference.company_subsets import CompanySubset

# Set Edgar identity
set_identity("williamzoughaib@gmail.com")

# Define all 11 GICS sectors with their SIC code mappings
SECTORS = {
    'Healthcare': [(2830, 2839), (3840, 3849), (8000, 8099)],
    'Energy': [(1200, 1299), (1300, 1399), (2900, 2999)],
    'Materials': [(1000, 1099), (1400, 1499), (2600, 2699), (2800, 2829), (3300, 3399)],
    'Industrials': [(1500, 1799), (3400, 3599), (3700, 3799), (4000, 4799), (5000, 5099), (7200, 7399)],
    'Consumer_Discretionary': [(2300, 2399), (2500, 2599), (3600, 3699), (3900, 3999), (5200, 5999), (7000, 7199)],
    'Consumer_Staples': [(2000, 2099), (2100, 2199), (5100, 5199), (5400, 5499)],
    'Financials': [(6000, 6799)],
    'Information_Technology': [(3570, 3579), (3600, 3699), (7370, 7379)],
    'Communication_Services': [(4800, 4899)],
    'Utilities': [(4900, 4999)],
    'Real_Estate': [(6500, 6599)],
}

# Timeframe folders to copy
TIMEFRAMES = ['1yr', '5yr', '10yr', 'alltime']


def get_sector_companies(sector_name, sic_ranges):
    """Get all companies for a given sector using SIC code ranges."""
    print(f"\nFetching companies for {sector_name} sector...")

    # Start with first range
    first_range = sic_ranges[0]
    companies = CompanySubset(use_comprehensive=True).from_industry(sic_range=first_range)

    # Combine with remaining ranges
    for sic_range in sic_ranges[1:]:
        companies = companies.combine_with(
            CompanySubset(use_comprehensive=True).from_industry(sic_range=sic_range)
        )

    df = companies.get()
    print(f"Found {len(df)} companies in {sector_name}")
    return df


def copy_sector_files(sector_name, companies_df):
    """Copy existing stock CSV files from STOCKS folders to sector folders."""
    # Get list of tickers
    tickers = companies_df['ticker'].dropna().tolist()
    tickers_set = set(tickers)  # For faster lookup

    # Create sector folder structure
    sector_base = os.path.join('SECTORS', sector_name)
    for timeframe in TIMEFRAMES:
        os.makedirs(os.path.join(sector_base, timeframe), exist_ok=True)

    # Track statistics
    stats = {tf: {'found': 0, 'copied': 0} for tf in TIMEFRAMES}

    print(f"\nCopying files for {len(tickers)} companies in {sector_name}...")

    # Process each timeframe
    for timeframe in TIMEFRAMES:
        source_folder = os.path.join('STOCKS', timeframe)
        dest_folder = os.path.join(sector_base, timeframe)

        if not os.path.exists(source_folder):
            print(f"  {timeframe}: Source folder not found, skipping")
            continue

        # Get all CSV files in source folder
        csv_files = list(Path(source_folder).glob("*.csv"))

        for csv_file in csv_files:
            ticker = csv_file.stem  # Get filename without extension

            if ticker in tickers_set:
                stats[timeframe]['found'] += 1
                dest_file = os.path.join(dest_folder, csv_file.name)

                try:
                    shutil.copy2(csv_file, dest_file)
                    stats[timeframe]['copied'] += 1
                except Exception as e:
                    print(f"    Error copying {ticker}: {e}")

        print(f"  {timeframe}: {stats[timeframe]['copied']}/{stats[timeframe]['found']} files copied")

    return stats


def main():
    """Main execution function."""
    print("=" * 70)
    print("SEC GICS Sector Data Organization")
    print("=" * 70)
    print("\nNOTE: First run will download comprehensive SEC dataset (~30-60 min)")
    print("      Subsequent runs will be fast using cached data\n")
    print("Copying existing data from STOCKS folders to SECTORS folders...")

    # Create main SECTORS folder
    os.makedirs('SECTORS', exist_ok=True)

    results = {}

    for sector_name, sic_ranges in SECTORS.items():
        companies_df = get_sector_companies(sector_name, sic_ranges)

        if not companies_df.empty:
            stats = copy_sector_files(sector_name, companies_df)
            results[sector_name] = stats
        else:
            results[sector_name] = {tf: {'found': 0, 'copied': 0} for tf in TIMEFRAMES}

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY BY SECTOR")
    print("=" * 70)
    for sector_name, stats in results.items():
        total_copied = sum(s['copied'] for s in stats.values())
        print(f"\n{sector_name}:")
        for timeframe in TIMEFRAMES:
            print(f"  {timeframe:10} {stats[timeframe]['copied']} files copied")
        print(f"  Total: {total_copied} files")

    print("\n" + "=" * 70)
    grand_total = sum(sum(s['copied'] for s in stats.values()) for stats in results.values())
    print(f"Grand Total: {grand_total} files copied across all sectors")
    print("=" * 70)


if __name__ == "__main__":
    main()
