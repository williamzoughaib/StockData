"""
Extract ALL historical financial data from Apple (AAPL) using SEC XBRL filings.
Organizes data by filing type (10-K annual, 10-Q quarterly).
Exports to CSV for analysis.
"""

from edgar import set_identity, Company
import pandas as pd
import os
from pathlib import Path

# Set identity
set_identity("williamzoughaib@gmail.com")

print("=" * 70)
print("AAPL All-Time Financial Data Extraction from SEC XBRL")
print("=" * 70)

# Get Apple company object
print("\nFetching Apple Inc. (AAPL)...")
aapl = Company("AAPL")
print(f"Company: {aapl.name}")
print(f"CIK: {aapl.cik}")

# Define the XBRL concepts we want to extract
# Core financial statement items
KEY_CONCEPTS = [
    'Revenues',
    'RevenueFromContractWithCustomerExcludingAssessedTax',
    'NetIncomeLoss',
    'Assets',
    'AssetsCurrent',
    'AssetsNoncurrent',
    'Liabilities',
    'LiabilitiesCurrent',
    'LiabilitiesNoncurrent',
    'StockholdersEquity',
    'NetCashProvidedByUsedInOperatingActivities',
    'NetCashProvidedByUsedInInvestingActivities',
    'NetCashProvidedByUsedInFinancingActivities',
]

# Share-related concepts
SHARE_CONCEPTS = [
    'CommonStockSharesOutstanding',
    'WeightedAverageNumberOfSharesOutstandingBasic',
    'WeightedAverageNumberOfSharesOutstandingDiluted',
    'WeightedAverageNumberOfDilutedSharesOutstanding',
    'CommonStockSharesIssued',
]

# EPS and per-share metrics
EPS_CONCEPTS = [
    'EarningsPerShareBasic',
    'EarningsPerShareDiluted',
    'EarningsPerShareBasicAndDiluted',
    'IncomeLossFromContinuingOperationsPerBasicShare',
    'IncomeLossFromContinuingOperationsPerDilutedShare',
    'IncomeLossFromDiscontinuedOperationsNetOfTaxPerBasicShare',
    'IncomeLossFromDiscontinuedOperationsNetOfTaxPerDilutedShare',
    'NetAssetValuePerShare',
    'BookValuePerShare',
    'CommonStockDividendsPerShareDeclared',
    'CommonStockDividendsPerShareCashPaid',
    'CommonStockParOrStatedValuePerShare',
]

# Combine all concepts
ALL_CONCEPTS = KEY_CONCEPTS + SHARE_CONCEPTS + EPS_CONCEPTS

def extract_filing_data(filing, concepts):
    """Extract XBRL data from a single filing."""
    try:
        # Get XBRL instance
        xbrl = filing.xbrl()
        facts_view = xbrl.facts

        filing_data = []

        for concept in concepts:
            try:
                # Query for this specific concept
                query_result = facts_view.query().by_concept(concept, exact=True)
                df = query_result.to_dataframe()

                if not df.empty:
                    df['metric'] = concept
                    df['filing_date'] = filing.filing_date
                    df['form'] = filing.form
                    df['accession_number'] = filing.accession_number
                    filing_data.append(df)
            except Exception as e:
                pass  # Silently skip missing concepts

        return filing_data if filing_data else None
    except Exception as e:
        print(f"  Error processing filing {filing.accession_number}: {e}")
        return None

# Create output directories
os.makedirs('10-K', exist_ok=True)
os.makedirs('10-Q', exist_ok=True)

# Process 10-K filings (Annual Reports)
print("\n" + "=" * 70)
print("Processing 10-K Filings (Annual Reports)...")
print("=" * 70)

filings_10k = aapl.get_filings(form="10-K")
print(f"Found {len(filings_10k)} 10-K filings")

all_10k_data = []

for i, filing in enumerate(filings_10k, 1):
    print(f"\n[{i}/{len(filings_10k)}] Processing 10-K filed {filing.filing_date}...")

    filing_data = extract_filing_data(filing, ALL_CONCEPTS)

    if filing_data:
        combined = pd.concat(filing_data, ignore_index=True)
        all_10k_data.append(combined)

        # Save individual filing data
        filename = f"10-K/aapl_10k_{filing.filing_date}_{filing.accession_number.replace('-', '')}.csv"
        combined.to_csv(filename, index=False)
        print(f"  ✓ Extracted {len(combined)} records")
        print(f"  ✓ Saved to: {filename}")
    else:
        print(f"  ✗ No data extracted from this filing")

# Combine all 10-K data
if all_10k_data:
    print("\n" + "=" * 70)
    print("Combining all 10-K data...")
    print("=" * 70)

    combined_10k = pd.concat(all_10k_data, ignore_index=True)
    combined_10k.to_csv('10-K/aapl_all_10k_financials.csv', index=False)
    print(f"✓ Total 10-K records: {len(combined_10k)}")
    print(f"✓ Saved to: 10-K/aapl_all_10k_financials.csv")

# Process 10-Q filings (Quarterly Reports)
print("\n" + "=" * 70)
print("Processing 10-Q Filings (Quarterly Reports)...")
print("=" * 70)

filings_10q = aapl.get_filings(form="10-Q")
print(f"Found {len(filings_10q)} 10-Q filings")

all_10q_data = []

for i, filing in enumerate(filings_10q, 1):
    print(f"\n[{i}/{len(filings_10q)}] Processing 10-Q filed {filing.filing_date}...")

    filing_data = extract_filing_data(filing, ALL_CONCEPTS)

    if filing_data:
        combined = pd.concat(filing_data, ignore_index=True)
        all_10q_data.append(combined)

        # Save individual filing data
        filename = f"10-Q/aapl_10q_{filing.filing_date}_{filing.accession_number.replace('-', '')}.csv"
        combined.to_csv(filename, index=False)
        print(f"  ✓ Extracted {len(combined)} records")
        print(f"  ✓ Saved to: {filename}")
    else:
        print(f"  ✗ No data extracted from this filing")

# Combine all 10-Q data
if all_10q_data:
    print("\n" + "=" * 70)
    print("Combining all 10-Q data...")
    print("=" * 70)

    combined_10q = pd.concat(all_10q_data, ignore_index=True)
    combined_10q.to_csv('10-Q/aapl_all_10q_financials.csv', index=False)
    print(f"✓ Total 10-Q records: {len(combined_10q)}")
    print(f"✓ Saved to: 10-Q/aapl_all_10q_financials.csv")

# Final summary
print("\n" + "=" * 70)
print("✓ Extraction Complete")
print("=" * 70)

print(f"\n10-K Filings: {len(filings_10k)} processed")
if all_10k_data:
    print(f"  Total records: {len(combined_10k)}")
    print(f"  Metrics extracted: {combined_10k['metric'].nunique()}")

print(f"\n10-Q Filings: {len(filings_10q)} processed")
if all_10q_data:
    print(f"  Total records: {len(combined_10q)}")
    print(f"  Metrics extracted: {combined_10q['metric'].nunique()}")

print("\nOutput structure:")
print("  10-K/")
print("    ├── aapl_all_10k_financials.csv (all annual data combined)")
print("    └── aapl_10k_YYYY-MM-DD_*.csv (individual filings)")
print("  10-Q/")
print("    ├── aapl_all_10q_financials.csv (all quarterly data combined)")
print("    └── aapl_10q_YYYY-MM-DD_*.csv (individual filings)")
