import os
import glob
import csv
import psycopg2

from typing import List, Dict, AnyStr
from psycopg2.extras import execute_values

from src.stock_screener.dal_util.db_conn import DatabaseConnection


def get_all_csv_files() -> List[AnyStr]:
    return glob.glob("./csvs/*.csv")


def get_all_combined_data_from_csvs(csv_files: List[AnyStr]) -> List[Dict[str, str]]:
    combined_data: List[Dict[str, str]] = []

    for csv_path in csv_files:
        with open(csv_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                combined_data.append(dict(row))
    return combined_data


def get_unique_combined_data(combined_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    return list({
        tuple(sorted(d.items())): d
        for d in combined_data
    }.values())


def get_connection():
    try:
        return psycopg2.connect(
            host="localhost",
            port="5432",
            user="appuser",
            password="apppassword",
            dbname="ashani_db"
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")

def save_stock_symbol_data(conn, unq_cmb_data: List[Dict[str, str]]) -> None:
    query = """
    INSERT INTO stocks (symbol, company_name, industry, isin)
    VALUES %s
    ON CONFLICT (symbol) DO NOTHING;
    """
    values = [
        (row['Symbol'], row['Company Name'], row['Industry'], row['ISIN Code'],)
        for row in unq_cmb_data
    ]
    with conn.cursor() as cursor:
        execute_values(cursor, query, values)
    conn.commit()


if __name__ == "__main__":
    csv_files = get_all_csv_files()
    combined_data = get_all_combined_data_from_csvs(csv_files)
    unique_combined_data = get_unique_combined_data(combined_data)
    print(unique_combined_data)

    db = DatabaseConnection(
        dbname=os.environ['DBNAME'],
        user=os.environ['USER'],
        password=os.environ['PASSWORD'],
        host=os.environ['HOST'],
        port=os.environ['PORT']
    )
    conn = db.get_connection()
    save_stock_symbol_data(conn, unique_combined_data)
    db.close_connection()
