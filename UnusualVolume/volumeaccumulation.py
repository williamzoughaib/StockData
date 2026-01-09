import time
import datetime
import numpy as np
from pathlib import Path
from tqdm import tqdm
from joblib import Parallel, delayed, parallel_backend
import multiprocessing
import pandas as pd

# --- Refined Variables for SLOW Accumulation ---
MONTH_CUTTOFF = 6        # Lookback for historical context
DAY_CUTTOFF = 20         # Use a 20-day (1 month) moving average baseline
STD_CUTTOFF = 1.5        # Low multiplier to catch subtle, steady volume
MIN_STOCK_VOLUME = 50000 # Lowered slightly to catch mid-cap accumulation
MIN_PRICE = 0.25         # Your original threshold
MAX_PRICE_SWING = 0.03   # Only count days where price moves < 3% (Stability)
MIN_ACCUM_DAYS = 4       # Number of days in a week volume must be elevated