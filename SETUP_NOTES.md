# StockData Environment Setup - Summary

## System Configuration
- **CPU**: Intel Core i9-14900KF (24 cores, 32 logical processors @ 3200 MHz)
- **RAM**: 32 GB
- **GPU**: NVIDIA GeForce RTX 4080 SUPER (16GB VRAM, CUDA 13.0)
- **Storage**: Samsung SSD 990 PRO 4TB (E: drive)

## Drive Setup
- **C: Drive** - Windows OS
- **D: Drive** - Fedora Linux OS
- **E: Drive** - NTFS (3.6 TB) - **Shared data between Windows and Fedora**
  - Path on Windows: `E:\FinTechTradingPlatform\StockData`
  - Path on Fedora: `/mnt/windows-e/FinTechTradingPlatform/StockData`

## Strategy: Dual-Boot Workflow

### Windows Environment
- **Purpose**: Development, visualization, exploratory analysis
- **Python**: 3.12 (downgraded from 3.14 for ML compatibility)
- **Acceleration**: CPU parallel (20 cores) + PyTorch GPU + CuPy
- **Setup Script**: `setup_environment.bat` (currently running)

### Fedora Environment
- **Purpose**: Heavy data processing, ML training, production work
- **Python**: 3.12
- **Acceleration**: RAPIDS cuDF (50-100x faster) + PyTorch + CPU parallel
- **Setup Script**: `setup_environment.sh` (to be created)

## Windows Setup (In Progress)

**Installed Packages:**
- Python 3.12
- PyTorch with CUDA 12.1
- CuPy (GPU-accelerated NumPy)
- pandas, numpy, matplotlib, seaborn, plotly
- scikit-learn, xgboost
- yfinance, edgartools, polars

**Environment**: `conda activate stockdata`

## Fedora Setup (To Do Tomorrow)

### 1. Mount E: Drive on Fedora
```bash
# Install NTFS support
sudo dnf install ntfs-3g

# Create mount point
sudo mkdir -p /mnt/windows-e

# Find device (check output for E: drive, likely nvme device)
lsblk

# Mount E: drive (replace /dev/XXX with actual device)
sudo mount -t ntfs-3g /dev/XXX /mnt/windows-e

# Verify access
ls /mnt/windows-e/FinTechTradingPlatform/StockData
```

### 2. Auto-mount E: on Boot
```bash
# Get UUID
sudo blkid | grep ntfs

# Edit fstab
sudo nano /etc/fstab

# Add line (replace UUID):
UUID=YOUR-UUID-HERE /mnt/windows-e ntfs-3g defaults,uid=1000,gid=1000 0 0
```

### 3. Run Fedora Setup Script
```bash
cd /mnt/windows-e/FinTechTradingPlatform/StockData
chmod +x setup_environment.sh
./setup_environment.sh
```

## GPU Acceleration Options

### Windows (No RAPIDS)
1. **CuPy**: GPU-accelerated NumPy (10-100x speedup for arrays)
2. **PyTorch**: GPU tensors for ML
3. **CPU Parallel**: 20 cores for I/O tasks

### Fedora (RAPIDS Available)
1. **RAPIDS cuDF**: GPU-accelerated DataFrames (50-100x speedup) - **PRIMARY**
2. **PyTorch**: GPU ML training
3. **CPU Parallel**: 20 cores for I/O tasks

## Performance Expectations

| Task | Windows (CPU + CuPy) | Fedora (RAPIDS) |
|------|---------------------|-----------------|
| CSV Processing | 15-20x faster | 50-100x faster |
| Data Transforms | 10-50x faster | 50-100x faster |
| ML Training | PyTorch GPU | PyTorch GPU (same) |
| I/O Operations | 15-20x faster | 15-20x faster (same) |

## Git Strategy

**Version Control:**
- Code: Push to GitHub regularly
- Large CSV files: Excluded from Git (use `.gitignore`)
- Work on same repo from both OSs (shared E: drive)

**.gitignore additions:**
```gitignore
# Data files
*.csv
*.parquet
*.h5
*.pkl

# Python
*.pyc
__pycache__/
.ipynb_checkpoints/

# Environment
.env
*.log

# OS
.DS_Store
Thumbs.db
*~
```

## Why Fedora for Data Science?

✅ RAPIDS cuDF native support (50-100x DataFrame speedup)
✅ Better memory management and performance
✅ Industry standard (most quant firms use Linux)
✅ Better GPU driver support
✅ All cutting-edge ML libraries work better
✅ Faster file I/O on ext4/optimized NTFS access

## VSCode + Claude Code

**Both Windows and Fedora:**
- VSCode works identically
- Claude Code CLI available on both
- Same extensions, same experience
- Same keyboard shortcuts

## Next Steps (Tomorrow)

1. ✅ Verify Windows setup completed successfully
2. Boot into Fedora
3. Mount E: drive (follow commands above)
4. Create Fedora setup script with RAPIDS
5. Run Fedora setup
6. Create optimized processing scripts (RAPIDS-accelerated)
7. Benchmark performance: Windows vs Fedora

## Key Files

- `setup_environment.bat` - Windows environment setup
- `setup_environment.sh` - Fedora environment setup (to be created)
- `SETUP_NOTES.md` - This file
- `.gitignore` - Exclude large files from Git
- `.env` - Credentials (not in Git)

## Email Protection

**Sensitive credentials:**
- Create `.env` file with email
- Add `.env` to `.gitignore`
- Update scripts to read from environment variables
- Never commit credentials to Git

---

**Hardware Summary:**
- 24-core CPU for parallel processing
- RTX 4080 SUPER for GPU acceleration
- 32GB RAM for large datasets
- 4TB Samsung 990 PRO SSD for fast I/O

**Software Summary:**
- Python 3.12 on both OSs
- RAPIDS cuDF on Fedora only (Linux-only library)
- PyTorch + CuPy on both
- Same code works on both (pandas API compatible)
