# Stock Screener

A Python-based stock screening application that identifies discounted stocks based on valuation metrics. The application scrapes Nifty index data, stores stock information in a PostgreSQL database, and provides valuation analysis to find potentially undervalued stocks.

## Features

- **Stock Data Collection**: Automatically scrape and download stock symbols from various Nifty indices (50, 100, 200, 500, etc.)
- **Database Storage**: Store stock master data, valuation references, and snapshots in PostgreSQL
- **Valuation Analysis**: Calculate discounts based on 5-year average P/E ratios and industry benchmarks
- **Discounted Stock Screening**: Identify stocks trading at significant discounts to their historical valuations
- **Docker Integration**: Easy database setup using Docker Compose

## Project Structure

```
src/stock_screener/
├── stock_symbols/          # Stock symbol collection and storage
├── value_references/       # Valuation reference data management
├── valuation_snapshot/     # Current valuation data snapshots
├── dal_util/              # Database access layer utilities
├── screening_script.py    # Main screening script
└── consts.py              # Configuration constants
```

## Prerequisites

- Python 3.14 or higher
- Docker and Docker Compose
- Poetry (for dependency management)

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd stock-screener
   ```

2. **Install dependencies using Poetry**:
   ```bash
   poetry install
   ```

3. **Set up the database**:
   ```bash
   cd src/stock_screener/stock_symbols/sql_setup
   docker-compose up -d
   ```

4. **Configure environment**:
   Copy `dev.env` to `.env` and update database credentials if needed.

## Usage

### API Usage

The application now includes a FastAPI-based REST API for easy integration and automation.

1. **Activate the Poetry shell**:
   ```bash
   poetry shell
   ```

2. **Run the API server**:
   ```bash
   python -m src.stock_screener.stock_screener
   ```
   The API will be available at `http://localhost:8000`

3. **API Endpoints**:
   - `GET /health` - Health check
   - `POST /grab-csvs` - Download stock CSV files
   - `POST /populate-stocks` - Populate stocks table from CSV files
   - `POST /populate-valuation-references` - Calculate valuation references
   - `POST /populate-valuation-snapshots` - Generate valuation snapshots

4. **API Documentation**:
   Visit `http://localhost:8000/docs` for interactive API documentation.

### Command Line Usage

Alternatively, you can use the individual scripts:

1. **Run the stock symbol scraper**:
   ```bash
   python -m src.stock_screener.screening_script
   ```

2. **Save stocks to database**:
   ```bash
   python -m src.stock_screener.stock_symbols.save_stocks_to_sql
   ```

3. **Save valuation references**:
   ```bash
   python -m src.stock_screener.value_references.save_value_references_to_sql
   ```

4. **Save valuation snapshots**:
   ```bash
   python -m src.stock_screener.valuation_snapshot.save_valuation_snapshots
   ```

## Database Schema

The application uses three main tables:

- `stocks`: Master stock information
- `valuation_reference`: 5-year average P/E and discount thresholds
- `valuation_snapshots`: Current valuation data and discount calculations

A materialized view `mv_discounted_latest` provides the latest discounted stocks.

## Dependencies

- pandas: Data manipulation
- numpy: Numerical computations
- requests: HTTP requests
- beautifulsoup4: HTML parsing
- lxml: XML/HTML processing
- psycopg2: PostgreSQL adapter
- yfinance: Yahoo Finance data

## Development

The application includes both command-line scripts and a REST API for stock screening operations. The supporting modules and database schema are fully implemented.

### Running Tests

```bash
poetry run pytest
```

## License

See LICENSE file for details.

## Author

Tejas K Jaiswal - tejasjaiswal9711@gmail.com
