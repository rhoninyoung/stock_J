# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview
This is an A-share (Chinese stock market) KDJ indicator monitoring system. It tracks Chinese A-share stocks using 6-digit stock codes and calculates KDJ technical indicators for market analysis.

## Architecture
- **Single-file Python application** (`stock_monitor_a.py`) with modular class design
- **SQLite database** (`stock_data.db`) for caching stock data and KDJ calculations
- **Configuration-driven** via `config.ini` for stock lists, timeframes, and indicator parameters
- **Yahoo Finance integration** for A-share data (converts 6-digit codes to .SS/.SZ format)

## Key Components
- **StockDatabase**: SQLite operations for storing/retrieving KDJ data
- **AShareDataFetcher**: Handles A-share data retrieval from Yahoo Finance
- **KDJCalculator**: Computes K, D, J values from price data
- **AShareMonitor**: Main orchestrator class

## Development Commands

### Core Operations
```bash
# Update all A-share data
python3 stock_monitor_a.py --update

# Query specific stock
python3 stock_monitor_a.py --query 600519

# Show top N stocks with lowest J values
python3 stock_monitor_a.py --top 10

# Switch timeframe (daily/weekly/monthly)
python3 stock_monitor_a.py --top 5 --timeframe weekly
```

### Configuration
- **Stock Lists**: Edit `config.ini` under `[stocks]` section
- **KDJ Parameters**: Modify `[indicators]` section for n, m1, m2 values
- **Timeframe**: Set `timeframe = daily/weekly/monthly` in `[settings]`

### Dependencies
```bash
pip install -r requirements.txt
```

## Data Flow
1. 6-digit stock codes → Yahoo Finance format (.SS/.SZ)
2. Historical price data → KDJ calculation
3. Results cached in SQLite → Query operations
4. CLI output for analysis

## Stock Code Format
- Shanghai: 6xxxxxx → xxxx.SS
- Shenzhen: 0xxxxxx/3xxxxxx → xxxx.SZ