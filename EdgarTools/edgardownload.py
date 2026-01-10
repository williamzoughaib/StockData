from edgar import use_local_storage, download_edgar_data, download_filings, set_identity
import os
set_identity("williamzoughaib@gmail.com")
# Setup offline-capable environment
storage_path = os.path.join(os.path.dirname(__file__), "Research")
os.makedirs(storage_path, exist_ok=True)
use_local_storage(storage_path)

# Download metadata (company info, filing indexes, facts API data)
download_edgar_data()  # ~24 GB - enables company lookups and EntityFacts

# Download only recent filings regularly
from datetime import datetime, timedelta

recent_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
download_filings(f"{recent_date}:")

