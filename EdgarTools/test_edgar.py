"""
Simple test to verify EdgarTools is working
"""

print("Testing EdgarTools installation...")

try:
    from edgar import set_identity
    from edgar.reference.tickers import get_company_tickers
    print("✓ EdgarTools imported successfully")
except ImportError as e:
    print(f"✗ Failed to import EdgarTools: {e}")
    print("\nInstall with: pip install edgartools")
    exit(1)

# Set identity
print("\nSetting identity...")
set_identity("williamzoughaib@gmail.com")
print("✓ Identity set")

# Try to fetch tickers
print("\nFetching company tickers from SEC...")
try:
    tickers_data = get_company_tickers()
    print(f"✓ Successfully fetched {len(tickers_data)} companies")

    print("\nAvailable columns:")
    print(tickers_data.columns.tolist())

    print("\nFirst 5 companies:")
    print(tickers_data.head())

    # Check for SIC codes
    if 'sic' in tickers_data.columns:
        print(f"\n✓ SIC codes available")
        print(f"Companies with SIC codes: {tickers_data['sic'].notna().sum()}")
    else:
        print("\n✗ No SIC codes in data")

except Exception as e:
    print(f"✗ Error fetching tickers: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n" + "=" * 60)
print("✓ EdgarTools is working correctly!")
print("=" * 60)
