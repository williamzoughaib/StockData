from edgar import *
from edgar.reference.tickers import get_company_tickers

set_identity("williamzoughaib@gmail.com")

print("Fetching all company tickers...")
# This should be much lighter than download_submissions()
tickers_data = get_company_tickers()

print(f"Total companies: {len(tickers_data)}")
print(f"Columns: {list(tickers_data.columns)}")
print(tickers_data.head())

# Check if we can filter by SIC
if 'sic' in tickers_data.columns:
    biotech = tickers_data[
        (tickers_data['sic'] >= 2833) &
        (tickers_data['sic'] <= 2836)
    ]
    print(f"\nFound {len(biotech)} biotech companies")
    print(biotech)

    biotech.to_csv("biotech_companies.csv", index=False)
    print("\nSaved to biotech_companies.csv")
else:
    print("\nNo SIC codes available in ticker data")
    print("You must use download_submissions() for full company data")
