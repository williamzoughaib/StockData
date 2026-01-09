from edgar import *
from edgar.reference.company_subsets import get_biotechnology_companies
from edgar.storage import download_submissions

set_identity("williamzoughaib@gmail.com")

# Download company data (one-time, ~500 MB)
print("Downloading company submissions data (one-time download)...")
download_submissions()

print("\nFetching biotechnology companies...")
biotech_stocks = get_biotechnology_companies()
print(biotech_stocks)