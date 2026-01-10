# Historical Stock Data

A comprehensive data collection system for downloading and organizing historical Stock and ETF data from NASDAQ with automatic sector classification.

## Overview

This project downloads historical price data for all traded stocks and etfs, maintains rolling time windows (1yr, 5yr, 10yr, and all-time), and organizes securities by GICS sector using SEC SIC codes. Modules include yfinance and the https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt. 

## Features

- Downloads all stocks and ETFs using yfinance
- Maintains multiple rolling time windows (1yr, 5yr, 10yr, all-time)
- Incremental updates - only fetches new data on subsequent runs
- Automatic sector classification using SEC EDGAR SIC codes
- Organized folder structure by asset type, sector, and timeframe

## Directory Structure

```
HistoricalStockData/
├── STOCKS/             # Individual stock data
│   ├── 1yr/            
│   ├── 5yr/            
│   ├── 10yr/           
│   └── alltime/         
├── ETFS/               # Individual ETF data
│   ├── 1yr/
│   ├── 5yr/
│   ├── 10yr/
│   └── alltime/
└── SECTORS/            # Data organized by GICS 
    ├── Healthcare/
    ├── Energy/
    ├── Materials/
    ├── Industrials/
    ├── Consumer_Discretionary/
    ├── Consumer_Staples/
    ├── Financials/
    ├── Information_Technology/
    ├── Communication_Services/
    ├── Utilities/
    └── Real_Estate/
```

## Installation

1. Install required packages:
```bash
pip install -r requirements.txt
```

2. For sector classification, install edgartools:
```bash
pip install edgartools
```

## Usage

### Download All Stock and ETF Data

Run the main script to download all NASDAQ data:

```bash
python historicalstockdata.py
```

This executes all modules in sequence:
1. Downloads tradeable stocks and ETFs
2. Processes and organizes tradeable data
3. Classifies stocks by sector
4. Organizes sector data



#### Data Format

Each CSV file contains OHLCV data:
- Date (index)
- Open
- High
- Low
- Close
- Volume

Both processing scripts add daily return and log volume to all .csv files for later graphical analysis. 

#### Time Windows

- **1yr**: 252 trading days (approximately 1 calendar year)
- **5yr**: 1,260 trading days (approximately 5 calendar years)
- **10yr**: 2,520 trading days (approximately 10 calendar years)
- **alltime**: Maximum available historical data

#### Requirements

- Python 3.14 (or compatible)
- pandas 2.3.3
- yfinance 1.0
- edgartools (for sector classification)

