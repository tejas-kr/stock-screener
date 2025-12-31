from fastapi import FastAPI, HTTPException
from typing import List, Dict
import os
import glob
import csv
from psycopg2.extras import execute_values

from src.stock_screener.stock_symbols.nifty_csv_grabber import NiftyIndexSaver
from src.stock_screener.value_references.save_value_references_to_sql import ValueReferenceService, YahooFinanceSource, ValuationReferenceRepository, StockRepository
from src.stock_screener.valuation_snapshot.save_valuation_snapshots import DiscountScreenerService, ValuationSnapshotRepository
from src.stock_screener.consts import DB_CONFIG
from src.stock_screener.dal_util.db_conn import DatabaseConnection

app = FastAPI(title="Stock Screener API", description="API for managing stock screening data", version="1.0.0")

# Database connection
db_conn = None

def get_db_connection():
    global db_conn
    if db_conn is None:
        db_conn = DatabaseConnection(**DB_CONFIG).get_connection()
    return db_conn

@app.on_event("shutdown")
def shutdown_event():
    global db_conn
    if db_conn:
        db_conn.close()

@app.post("/grab-csvs", summary="Grab CSV files and store locally")
async def grab_csvs():
    """
    Scrape Nifty index pages and download constituent CSV files to local storage.
    """
    try:
        nifty_saver = NiftyIndexSaver()
        nifty_saver.scrape_and_download()
        return {"message": "CSV files downloaded successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to download CSV files: {str(e)}")

@app.post("/populate-stocks", summary="Populate stocks table")
async def populate_stocks():
    """
    Read CSV files from local storage and populate the stocks table in the database.
    """
    try:
        # Get all CSV files
        csv_files = glob.glob("./csvs/*.csv")
        if not csv_files:
            raise HTTPException(status_code=404, detail="No CSV files found. Run /grab-csvs first.")

        # Combine data from all CSVs
        combined_data = []
        for csv_path in csv_files:
            with open(csv_path, mode="r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    combined_data.append(dict(row))

        # Get unique data
        unique_data = list({
            tuple(sorted(d.items())): d
            for d in combined_data
        }.values())

        # Save to database
        conn = get_db_connection()
        query = """
        INSERT INTO stocks (symbol, company_name, industry, isin)
        VALUES %s
        ON CONFLICT (symbol) DO NOTHING;
        """
        values = [
            (row['Symbol'], row['Company Name'], row['Industry'], row['ISIN Code'])
            for row in unique_data
        ]
        with conn.cursor() as cursor:
            execute_values(cursor, query, values)
        conn.commit()

        return {"message": f"Populated stocks table with {len(values)} records"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to populate stocks table: {str(e)}")

@app.post("/populate-valuation-references", summary="Populate valuation reference table")
async def populate_valuation_references():
    """
    Calculate and populate valuation reference data using Yahoo Finance.
    """
    try:
        conn = get_db_connection()

        # Get all stocks
        stocks_repo = StockRepository(conn)
        all_stocks = stocks_repo.get_all_stocks_as_list()

        if not all_stocks:
            raise HTTPException(status_code=404, detail="No stocks found. Run /populate-stocks first.")

        # Initialize services
        source = YahooFinanceSource()
        repo = ValuationReferenceRepository(conn)
        service = ValueReferenceService(source, repo)

        # Run the service
        service.run(all_stocks)

        return {"message": f"Populated valuation references for {len(all_stocks)} stocks"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to populate valuation references: {str(e)}")

@app.post("/populate-valuation-snapshots", summary="Populate valuation snapshots table")
async def populate_valuation_snapshots():
    """
    Generate current valuation snapshots and identify discounted stocks.
    """
    try:
        conn = get_db_connection()

        # Initialize service
        repo = ValuationSnapshotRepository(conn)
        service = DiscountScreenerService(repo)

        # Run the service
        service.run()

        return {"message": "Populated valuation snapshots"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to populate valuation snapshots: {str(e)}")

@app.get("/health", summary="Health check")
async def health_check():
    """
    Check if the API is running and database is accessible.
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        return {"status": "healthy"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)