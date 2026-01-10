@echo off
REM ============================================================================
REM StockData Environment Setup Script
REM Creates conda environment with Python 3.12 + RAPIDS + ML packages
REM ============================================================================

echo.
echo ============================================================================
echo Creating StockData Environment (Python 3.12 + GPU Acceleration)
echo ============================================================================
echo.

REM Step 1: Create environment
echo [1/5] Creating conda environment with Python 3.12...
call conda create -n stockdata python=3.12 -y
if errorlevel 1 (
    echo ERROR: Failed to create environment
    pause
    exit /b 1
)

REM Step 2: Activate environment
echo.
echo [2/5] Activating stockdata environment...
call conda activate stockdata
if errorlevel 1 (
    echo ERROR: Failed to activate environment
    pause
    exit /b 1
)

REM Step 3: Install core data science packages
echo.
echo [3/5] Installing core packages (pandas, numpy, matplotlib, etc.)...
call conda install -c conda-forge pandas numpy matplotlib seaborn plotly scikit-learn xgboost -y
if errorlevel 1 (
    echo ERROR: Failed to install core packages
    pause
    exit /b 1
)

REM Step 4: Install PyTorch with CUDA support
echo.
echo [4/5] Installing PyTorch with CUDA 12.1 support...
call conda install -c pytorch -c nvidia pytorch pytorch-cuda=12.1 -y
if errorlevel 1 (
    echo ERROR: Failed to install PyTorch
    pause
    exit /b 1
)

REM Step 5: Install CuPy for GPU-accelerated NumPy
echo.
echo [5/6] Installing CuPy (GPU-accelerated NumPy)...
call pip install cupy-cuda12x
if errorlevel 1 (
    echo WARNING: CuPy installation failed, continuing anyway...
)

REM Step 6: Install other pip packages
echo.
echo [6/6] Installing pip packages (yfinance, edgartools, polars)...
call pip install yfinance edgartools polars
if errorlevel 1 (
    echo ERROR: Failed to install pip packages
    pause
    exit /b 1
)

echo.
echo ============================================================================
echo Environment setup complete!
echo ============================================================================
echo.
echo To use this environment:
echo   1. Run: conda activate stockdata
echo   2. Verify PyTorch GPU: python -c "import torch; print(f'CUDA available: {torch.cuda.is_available()}')"
echo.
echo Installed packages:
echo   - Python 3.12
echo   - PyTorch with CUDA 12.1 (GPU deep learning)
echo   - CuPy (GPU-accelerated NumPy)
echo   - pandas, numpy, matplotlib, seaborn, plotly
echo   - scikit-learn, xgboost
echo   - yfinance, edgartools, polars
echo.
echo GPU Acceleration Strategy:
echo   - CuPy: GPU-accelerated array operations (like NumPy on GPU)
echo   - PyTorch: GPU tensors for ML and data processing
echo   - CPU Parallel: 20-core processing for I/O-heavy tasks
echo   - Your RTX 4080 SUPER is ready for all three!
echo.
pause
