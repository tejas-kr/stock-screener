from typing import List, Dict

import yfinance as yf
from datetime import date
from psycopg2.extras import execute_batch

from src.stock_screener.consts import DB_CONFIG
from src.stock_screener.dal_util.db_conn import DatabaseConnection


class ValuationSnapshotRepository:

    def __init__(self, conn):
        self.conn = conn

    def get_reference_data(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT
                    symbol,
                    avg_5y_pe,
                    discount_threshold_pct
                FROM valuation_reference
                WHERE avg_5y_pe IS NOT NULL
            """)
            return cur.fetchall()

    def insert_many(self, rows):
        sql = """
            INSERT INTO valuation_snapshots
            (
                symbol,
                snapshot_date,
                current_price,
                current_pe,
                discount_vs_5y_avg,
                is_discounted
            )
            VALUES (%s,%s,%s,%s,%s,%s)
        """
        with self.conn.cursor() as cur:
            execute_batch(cur, sql, rows)
        self.conn.commit()


class DiscountScreenerService:

    def __init__(self, repo):
        self.repo = repo

    def run(self):
        today = date.today()
        reference_rows_list: List[Dict[str, str | float | int]] = self.repo.get_reference_data()

        inserts = []

        for reference_rows_dict in reference_rows_list:
            # symbol, avg_5y_pe, threshold
            symbol = reference_rows_dict.get("symbol")
            avg_5y_pe = float(reference_rows_dict.get("avg_5y_pe"))
            threshold = reference_rows_dict.get("discount_threshold_pct")

            print(f"Screening {symbol}...")

            t = yf.Ticker(f"{symbol}.NS")
            info = t.info

            price = info.get("currentPrice") or info.get("regularMarketPrice")
            pe = info.get("trailingPE")

            if not price or not pe or pe <= 0:
                continue

            discount_pct = (avg_5y_pe - pe) / avg_5y_pe * 100
            is_discounted = discount_pct >= threshold

            inserts.append((
                symbol,
                today,
                float(price),
                float(pe),
                round(discount_pct, 2),
                is_discounted
            ))

        self.repo.insert_many(inserts)


if __name__ == "__main__":
    conn = DatabaseConnection(**DB_CONFIG).get_connection()
    repo = ValuationSnapshotRepository(conn)

    service = DiscountScreenerService(repo)
    service.run()
