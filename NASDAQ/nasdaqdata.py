import pandas as pd
import edgar    
from pathlib import Path
from edgar import set_identity, Company

OUTPUT_DIR = Path("Data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 1. SETUP: Replace with your actual email to comply with SEC guidelines
set_identity("williamzoughaib@gmail.com")



if __name__ == "__main__":
    print("=" * 60)
    print("NASDAQ Stock Filtering & Export")
    print("=" * 60)

    # Download data
    url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    data = pd.read_csv(url, sep='|')
    data_clean = data[data['Test Issue'] == 'N']

    # Filter to stocks only
    stocks_candidates = data_clean[data_clean['ETF'] == 'N'].copy()
    mask = ~stocks_candidates['Security Name'].str.contains('- Unit|- Units|%|Fund|Notes|Subordinated|Total Return|Preferred|Dividend|consisting|Senior Notes|Warrant|Warrants|Right', case=False, na=False)
    stocks_filtered = stocks_candidates[mask]

    # Save filtered stocks to CSV, parquet
    stocks_filtered[['NASDAQ Symbol', 'Security Name']].to_csv(OUTPUT_DIR/"stocks.csv", index=False)
    stocks_filtered[['NASDAQ Symbol', 'Security Name']].to_parquet(OUTPUT_DIR/"stocks.parquet", index=False)

    # Filter to ETFs and save to CSV, parquet
    etf_candidates = data_clean[data_clean['ETF'] == 'Y'].copy()
    etf_candidates[['NASDAQ Symbol', 'Security Name']].to_csv(OUTPUT_DIR/"etfs.csv", index=False)
    etf_candidates[['NASDAQ Symbol', 'Security Name']].to_parquet(OUTPUT_DIR/"etfs.parquet", index=False)

def filter_by_edgar():
    # Load your NASDAQ list
    df = pd.read_csv(OUTPUT_DIR/"stocks.csv")
    
    # We will store the results here
    valid_companies = []
    
    print(f"Starting verification for {len(df)} tickers...")

    for index, row in df.iterrows():
        ticker = str(row['NASDAQ Symbol']).strip()
        
        try:
            company = Company(ticker)
            sic = company.sic
            cik = company.cik

            # 2. Check for existence of filings of type 10-K, 10-Q, 20-F, or 40-F
            # We look at the latest filings to see if these forms exist
            filings = company.get_filings()
            has_required_form = any(form in ['10-K', '10-Q','20-F','40-F'] for form in filings.to_pandas()['form'].unique())
            
            if has_required_form:
                print(f"[+] Keeping: {ticker} (SIC: {sic}, CIK: {cik})")
                row_copy = row.copy()
                row_copy['SIC'] = sic
                row_copy['CIK'] = cik
                valid_companies.append(row_copy)
            else:
                print(f"[-] Skipping: {ticker} - No filings found.")
                
        except Exception as e:
            # This handles cases where the ticker isn't found or has no filings
            print(f"[!] Error or Ticker not found: {ticker}")
            

    # Save the new list
    filtered_df = pd.DataFrame(valid_companies)
    filtered_df.to_csv(OUTPUT_DIR/"SECstocks.csv", index=False)
    filtered_df.to_parquet(OUTPUT_DIR/"SECstocks.parquet", index=False)
    print(f"\nDone! Saved {len(filtered_df)} companies to SECstocks")

# Run the function
filter_by_edgar()