import pandas as pd
from edgar import *
from edgar.reference.tickers import SEC_stocks
from edgar.xbrl import *
set_identity("williamzougahib@gmail.com")

stocks = SEC_stocks()
tickers = stocks.Ticker.to_list()

ticker = tickers[0]
print(ticker)
apple = Company(ticker)
filings = apple.get_filings(form="10-K").head(15)
applexbrls = XBRLS.from_filings(filings)
print(applexbrls)
apple_income_statement = applexbrls.statements.income_statement()
apple_income_statement_df = apple_income_statement.to_dataframe()
print(apple_income_statement_df)
apple_income_statement_df = apple_income_statement_df.replace("", pd.NA)
apple_income_statement_df.to_parquet("AAPL_10K_Income_Statement.parquet", index=False)
apple_balance_sheet = applexbrls.statements.balance_sheet()
apple_balance_sheet_df = apple_balance_sheet.to_dataframe()
print(apple_balance_sheet_df)
apple_balance_sheet_df = apple_balance_sheet_df.replace("", pd.NA)
apple_balance_sheet_df.to_parquet("AAPL_10K_Balance_Sheet_Statements.parquet", index=False)
combined_df = pd.concat(
    [
        apple_income_statement_df.assign(statement="income_statement"),
        apple_balance_sheet_df.assign(statement="balance_sheet"),
    ],
    ignore_index=True,
)
combined_df.to_parquet("AAPL_10K_Income_Balance_Combined.parquet", index=False)
combined_df.to_csv("AAPL_10K_Income_Balance_Combined.csv", index=False)
print(combined_df)

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

filtered_df = combined_df.copy()
filtered_df["label"] = filtered_df["label"].astype(str).str.strip()
filtered_df = filtered_df[filtered_df["label"].isin(keep_labels)].copy()
filtered_df = filtered_df.drop(columns=["concept", "statement"], errors="ignore")

filtered_df.to_parquet("AAPL_10K_Income_Balance_Filtered.parquet", index=False)
filtered_df.to_csv("AAPL_10K_Income_Balance_Filtered.csv", index=False)
print(filtered_df)