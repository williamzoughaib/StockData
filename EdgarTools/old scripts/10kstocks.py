import pandas as pd
from edgar import *
from edgar.reference.tickers import SEC_stocks  
from edgar.xbrl import *
set_identity("williamzougahib@gmail.com")

stocks = SEC_stocks()
tickers = stocks.Ticker.to_list()

ticker = tickers[]
print(ticker)
apple1 = Company(ticker)
filings = apple1.get_filings(form="10-Q", is_xbrl=True)
applexbrls1 = XBRLS.from_filings(filings)
print(applexbrls1)
apple_income_df = applexbrls1.statements.income_statement().to_dataframe()
apple_income_df = apple_income_df.replace("", pd.NA)
apple_income_df.to_parquet("AAPL_10Q_Income_Statement.parquet", index=False)
apple_balance_df = applexbrls1.statements.balance_sheet().to_dataframe()
apple_balance_df = apple_balance_df.replace("", pd.NA)
apple_balance_df.to_parquet("AAPL_10Q_Balance_Sheet_Statements.parquet", index=False)
combined_apple_df = pd.concat(
    [
        apple_income_df.assign(statement="income_statement"),
        apple_balance_df.assign(statement="balance_sheet"),
    ],
    ignore_index=True,
)
combined_apple_df.to_parquet("AAPL_10Q_Income_Balance_Combined.parquet", index=False)
combined_apple_df.to_csv("AAPL_10Q_Income_Balance_Combined.csv", index=False)
print(combined_apple_df)

keep_labels = {
    "Gross Profit",
    "Net Income",
    "Earnings Per Share (Basic)",
    "Earnings Per Share (Diluted)",
    "Shares Outstanding (Basic)",
    "Shares Outstanding (Diluted)",
    "Cash and Cash Equivalents",
    "Total Assets",
    "Short Term Debt",
    "Long Term Debt",
    "Total Liabilities",
    "Total Stockholders' Equity",
    "Total Liabilities and Stockholders' Equity",
}

# In edgar statements this column is usually named "label"
label_col = "label"

filtered_df = combined_apple_df.copy()
filtered_df["label"] = filtered_df["label"].astype(str).str.strip()
filtered_df = filtered_df[filtered_df["label"].isin(keep_labels)].copy()
filtered_df = filtered_df.drop(columns=["concept", "statement"], errors="ignore")

filtered_df.to_parquet("AAPL_10Q_Income_Balance_Filtered.parquet", index=False)
filtered_df.to_csv("AAPL_10Q_Income_Balance_Filtered.csv", index=False)
print(filtered_df)