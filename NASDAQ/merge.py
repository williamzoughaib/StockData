"""
Merge SECstocks.csv with SIC.csv

This script matches stocks from SECstocks.csv with their corresponding
SEC office based on SIC codes from SIC.csv

Both files are located in the Data subfolder
"""

import pandas as pd
import os


def load_data(sec_stocks_path, sic_mapping_path):
    """
    Load both CSV files into pandas DataFrames

    Args:
        sec_stocks_path: Path to SECstocks.csv
        sic_mapping_path: Path to SIC.csv

    Returns:
        tuple: (sec_stocks_df, sic_mapping_df)
    """
    print(f"Loading {sec_stocks_path}...")
    sec_stocks = pd.read_csv(sec_stocks_path)
    print(f"  - Loaded {len(sec_stocks)} stocks")

    print(f"Loading {sic_mapping_path}...")
    sic_mapping = pd.read_csv(sic_mapping_path)
    print(f"  - Loaded {len(sic_mapping)} office-SIC mappings")

    return sec_stocks, sic_mapping


def analyze_data(sec_stocks, sic_mapping):
    """
    Analyze the data before merging

    Args:
        sec_stocks: DataFrame with stock data
        sic_mapping: DataFrame with office-SIC mappings
    """
    print("\n" + "="*60)
    print("DATA ANALYSIS")
    print("="*60)

    print("\nSECstocks.csv columns:", list(sec_stocks.columns))
    print("SIC.csv columns:", list(sic_mapping.columns))

    print(f"\nUnique SIC codes in SECstocks: {sec_stocks['SIC'].nunique()}")
    print(f"Unique SIC codes in SIC mapping: {sic_mapping['SIC'].nunique()}")
    print(f"Unique offices in mapping: {sic_mapping['Office'].nunique()}")

    # Check for SIC codes in stocks that aren't in the mapping
    stocks_sic = set(sec_stocks['SIC'].dropna().astype(int))
    mapping_sic = set(sic_mapping['SIC'].astype(int))

    unmapped_sic = stocks_sic - mapping_sic
    if unmapped_sic:
        print(f"\nWARNING: {len(unmapped_sic)} SIC codes in SECstocks have no office mapping:")
        print(f"  Sample unmapped SIC codes: {sorted(list(unmapped_sic))[:10]}")

    # Show office distribution
    print("\nOffice distribution in SIC mapping:")
    office_counts = sic_mapping['Office'].value_counts()
    for office, count in office_counts.items():
        print(f"  {office}: {count} SIC codes")


def merge_data(sec_stocks, sic_mapping):
    """
    Merge the two DataFrames based on SIC code

    Args:
        sec_stocks: DataFrame with stock data
        sic_mapping: DataFrame with office-SIC mappings

    Returns:
        DataFrame: Merged data
    """
    print("\n" + "="*60)
    print("MERGING DATA")
    print("="*60)

    # Ensure SIC codes are the same type for merging
    sec_stocks['SIC'] = sec_stocks['SIC'].astype('Int64')
    sic_mapping['SIC'] = sic_mapping['SIC'].astype('Int64')

    # Perform left merge to keep all stocks
    merged = sec_stocks.merge(
        sic_mapping,
        on='SIC',
        how='left'
    )

    print(f"\nTotal stocks in merged data: {len(merged)}")
    print(f"Stocks with office assignment: {merged['Office'].notna().sum()}")
    print(f"Stocks without office assignment: {merged['Office'].isna().sum()}")

    # Reorder columns for better readability
    column_order = ['NASDAQ Symbol', 'Security Name', 'SIC', 'Office', 'CIK']
    merged = merged[column_order]

    return merged


def generate_reports(merged):
    """
    Generate summary reports from merged data

    Args:
        merged: Merged DataFrame
    """
    print("\n" + "="*60)
    print("SUMMARY REPORTS")
    print("="*60)

    # Stocks by office
    print("\nStocks by SEC Office:")
    office_distribution = merged['Office'].value_counts().sort_values(ascending=False)
    for office, count in office_distribution.items():
        if pd.notna(office):
            print(f"  {office}: {count} stocks")

    unmapped_count = merged['Office'].isna().sum()
    if unmapped_count > 0:
        print(f"  [No Office Mapping]: {unmapped_count} stocks")

    # Show sample of unmapped stocks
    unmapped = merged[merged['Office'].isna()]
    if len(unmapped) > 0:
        print(f"\nSample of unmapped stocks (first 10):")
        print(unmapped[['NASDAQ Symbol', 'Security Name', 'SIC']].head(10).to_string(index=False))


def save_results(merged, output_path):
    """
    Save merged data to CSV

    Args:
        merged: Merged DataFrame
        output_path: Path to save the output file
    """
    merged.to_csv(output_path, index=False)
    print(f"\n{'='*60}")
    print(f"Results saved to: {output_path}")
    print(f"{'='*60}")


def main():
    # Define file paths - both files are in the Data subfolder
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, 'Data')

    sec_stocks_path = os.path.join(data_dir, 'SECstocks.csv')
    sic_mapping_path = os.path.join(data_dir, 'SIC.csv')
    output_path = os.path.join(data_dir, 'SECstocks_with_offices.csv')

    # Verify files exist
    if not os.path.exists(sec_stocks_path):
        print(f"ERROR: {sec_stocks_path} not found!")
        return
    if not os.path.exists(sic_mapping_path):
        print(f"ERROR: {sic_mapping_path} not found!")
        return

    # Load data
    sec_stocks, sic_mapping = load_data(sec_stocks_path, sic_mapping_path)

    # Analyze data
    analyze_data(sec_stocks, sic_mapping)

    # Merge data
    merged = merge_data(sec_stocks, sic_mapping)

    # Generate reports
    generate_reports(merged)

    # Save results
    save_results(merged, output_path)

    print("\nMerge complete!")


if __name__ == "__main__":
    main()
