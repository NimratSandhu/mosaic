# Unified Signal Monitoring and Portfolio Context Platform

> A production-ready financial data platform that helps Portfolio Managers monitor trading signals, analyze market exposures, and make data-driven investment decisions. Built with Python, DuckDB, and Dash.

## 🎯 What is This?

This platform is a **complete end-to-end system** for quantitative finance that:

- **Ingests** daily price data and quarterly fundamentals from free public sources (Stooq, SEC EDGAR)
- **Processes** raw data through a curated data lake with quality validation
- **Calculates** interpretable financial features (volatility, momentum, mean reversion)
- **Generates** normalized signal scores (Z-scores) across a universe of stocks
- **Visualizes** signals, positions, and exposures in an interactive dashboard

Think of it as a **Bloomberg Terminal lite** - but open-source, free, and fully transparent.

## 🚀 Quick Start

### Prerequisites
- Python 3.11+ (3.11 recommended for prebuilt wheels)
- Docker (optional, for containerized deployment)

### Local Setup (5 minutes)

```bash
# 1. Clone the repository
git clone <your-repo-url>
cd mosaic

# 2. Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
make install

# 4. Configure environment
cp .env.example .env
# Edit .env and set:
# SEC_EDGAR_USER_AGENT="YourName your.email@example.com" (REQUIRED)
# SEC_EDGAR_USER_EMAIL="your.email@example.com" (REQUIRED)

# 5. Run the pipeline
make ingest-daily    # Fetch today's data
make curate          # Process and validate
make build-features  # Calculate signals
make run-dash        # Launch dashboard at http://localhost:8050
```

### Docker Deployment (One Command)

```bash
make deploy  # Builds and starts everything
# Dashboard available at http://localhost:8050
```

## 📊 What You Get

### 1. **Data Pipeline**
- **Raw Layer**: Untouched API responses (Stooq prices, SEC filings)
- **Curated Layer**: Cleaned, standardized tables with schema validation
- **Marts Layer**: Pre-computed features and signals ready for analysis

### 2. **Feature Engine**
Calculates interpretable financial metrics:
- **Price Features**:
  - 20-day realized volatility
  - 60-day momentum
  - 5-day mean reversion Z-score
- **Fundamental Features**:
  - Year-over-year revenue growth proxies

### 3. **Signal Scoring**
- Normalizes all features into Z-scores (standard deviations from mean)
- Generates composite signal scores for ranking stocks
- Creates long/short candidate lists based on signal strength

### 4. **Interactive Dashboard**
Two-page Dash application with dark theme:

**Page 1: Market Overview**
- Filter by date and sector
- Top long/short candidate tables
- Sector exposure charts

**Page 2: Single Name Deep Dive**
- Price charts with feature overlays
- Detailed signal breakdown
- Feature-by-feature explanation

## 🏗️ Architecture

```
┌─────────────────┐
│  Data Sources   │  Stooq (prices) | SEC EDGAR (fundamentals)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Raw Data Lake  │  Partitioned Parquet files
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Curation ETL   │  Schema standardization | Validation (Great Expectations)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Curated Tables  │  daily_prices | quarterly_fundamentals
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  DuckDB Marts   │  Analytical database for dashboard queries
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Feature Engine │  Volatility | Momentum | Mean Reversion
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Signal Scoring  │  Z-score normalization | Position generation
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Dashboard     │  Dash/Plotly visualization
└─────────────────┘
```

## 📁 Project Structure

```
mosaic/
├── src/                    # Python source code
│   ├── config/            # Configuration settings
│   ├── data_sources/      # API clients (Stooq, SEC)
│   ├── flows/             # Prefect orchestration flows
│   ├── curation/          # ETL and data quality
│   ├── db/                 # DuckDB client and loaders
│   ├── features/           # Feature calculation engine
│   ├── dashboard/         # Dashboard data access layer
│   └── logging_utils/      # Logging configuration
├── dash_app/              # Dash application
│   ├── pages/             # Dashboard pages
│   └── assets/            # CSS and JavaScript
├── data/                   # Data lake (gitignored)
│   ├── raw/               # Raw API responses
│   ├── curated/           # Cleaned tables
│   └── marts/             # Feature tables and DuckDB
├── config/                # Configuration files
│   └── universe/          # Stock universe (S&P 100)
├── scripts/               # Utility scripts
├── Dockerfile             # Container definition
├── docker-compose.yml     # Multi-service deployment
├── render.yaml            # Render.com deployment config
└── Makefile              # Common commands
```

## 🛠️ Key Technologies

- **Python 3.11**: Core language
- **Prefect**: Lightweight workflow orchestration
- **DuckDB**: Analytical database (SQL on Parquet)
- **Pandas**: Data processing and feature engineering
- **Dash/Plotly**: Interactive web dashboard
- **Great Expectations**: Data quality validation
- **Docker**: Containerized deployment

## 📝 Common Commands

### Local Development
```bash
make install          # Install dependencies
make ingest-daily     # Fetch today's data (prices + fundamentals)
make curate           # Process raw data → curated tables
make build-features   # Calculate features and signals
make run-dash         # Start dashboard (http://localhost:8050)
make query-db         # Query DuckDB database
make backfill         # Backfill historical data
```

### Docker
```bash
make build            # Build Docker image
make deploy           # Build and start dashboard
make docker-up        # Start services
make docker-down      # Stop services
make docker-ingest    # Run ingestion in container
```

## 🔧 Configuration

Required environment variables (see `.env.example`):

## 📚 Documentation

- [DEPLOYMENT.md](DEPLOYMENT.md): Detailed deployment guides (Render, AWS, etc.)
- Code is well-documented with type hints and docstrings
- See inline comments for feature calculation logic

## 🤝 Contributing

Feel free to fork, extend, or use as a template for your own projects!

## 🙏 Acknowledgments

- **Stooq**: Free historical price data
- **SEC EDGAR**: Public company filings
- **DuckDB**: Fast analytical database
- **Dash/Plotly**: Beautiful visualizations

---