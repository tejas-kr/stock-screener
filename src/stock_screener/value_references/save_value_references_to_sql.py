# =====================================================
# Populate valuation_reference using Yahoo Finance
# =====================================================
import time
import pandas as pd
import yfinance as yf

from typing import List, Tuple
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, date

from psycopg2.extras import execute_values
from yfinance.exceptions import YFRateLimitError

from src.stock_screener.consts import DB_CONFIG
from src.stock_screener.dal_util.db_conn import DatabaseConnection

# =====================================================
# CONFIG
# =====================================================

DISCOUNT_THRESHOLD = 30.0

END_DATE = datetime.today()
START_DATE = END_DATE - timedelta(days=365 * 5)

# =====================================================
# DATA SOURCE (Strategy)
# =====================================================

class MarketDataSource(ABC):

    @abstractmethod
    def get_price_history(self, symbol: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_current_pe(self, symbol: str) -> float | None:
        pass


class YahooFinanceSource(MarketDataSource):

    def _yahoo_symbol(self, symbol: str) -> str:
        return f"{symbol}.NS"

    def get_price_history(self, symbol: str) -> pd.DataFrame | None:
        for attempt in range(3):
            try:
                df = yf.download(
                    self._yahoo_symbol(symbol),
                    start=START_DATE.strftime("%Y-%m-%d"),
                    end=END_DATE.strftime("%Y-%m-%d"),
                    progress=False,
                    auto_adjust=True
                )

                if df is None or df.empty:
                    return pd.DataFrame()

                df = df.reset_index()
                df["year"] = df["Date"].dt.year
                df.rename(columns={"Close": "close_price"}, inplace=True)

                return df[["Date", "year", "close_price"]]

            except YFRateLimitError:
                print("Rate limited. Sleeping 60s...")
                time.sleep(60)
            except Exception:
                return pd.DataFrame()
        return None

    def get_current_pe(self, symbol: str) -> float | None:
        try:
            t = yf.Ticker(self._yahoo_symbol(symbol))
            pe = t.info.get("trailingPE")
            pe = float(pe)
            return pe if pe and pe > 0 else None
        except Exception:
            return None


# =====================================================
# REPOSITORY (Persistence)
# =====================================================

class StockRepository:
    def __init__(self, conn):
        self.conn = conn
        self.table_name = "stocks"

    def get_all_stocks_as_list(self) -> List[str]:
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT symbol
                FROM {self.table_name}
            """)
            stocks = cur.fetchall()
        return [row['symbol'] for row in stocks]


class ValuationReferenceRepository:

    def __init__(self, conn):
        self.conn = conn
        self.table_name = "valuation_reference"

    def upsert_many(
        self,
        sym_avg_pe_tuple: List[Tuple]
    ):
        sql = f"""
            INSERT INTO {self.table_name}
            (
                symbol,
                avg_5y_pe,
                discount_threshold_pct,
                last_updated
            )
            VALUES %s
            ON CONFLICT (symbol) DO UPDATE
            SET
                avg_5y_pe = EXCLUDED.avg_5y_pe,
                last_updated = EXCLUDED.last_updated;
        """
        rows = [
            (symbol, avg_pe, DISCOUNT_THRESHOLD, date.today())
            for symbol, avg_pe in sym_avg_pe_tuple
            if avg_pe is not None
        ]
        with self.conn.cursor() as cur:
            execute_values(cur, sql, rows)
        self.conn.commit()

# =====================================================
# DOMAIN SERVICE (Pure Logic)
# =====================================================

class ValuationCalculator:

    @staticmethod
    def average_pe(price_df: pd.DataFrame, current_pe: float) -> float | None:
        if price_df.empty or not current_pe or current_pe <= 0:
            return None

        # Approximate EPS using current price / current PE
        latest_price = price_df["close_price"].iloc[-1].item()
        current_pe = float(current_pe)

        current_eps = latest_price / current_pe

        if current_eps <= 0:
            return None

        yearly_avg_price = (
            price_df.groupby("year")["close_price"]
            .mean()
            .tail(5)
        )

        pe_values = yearly_avg_price / current_eps

        if pe_values.empty:
            return None

        return round(pe_values.mean().item(), 2)

# =====================================================
# APPLICATION SERVICE
# =====================================================

class ValueReferenceService:

    def __init__(
        self,
        source: MarketDataSource,
        repository: ValuationReferenceRepository
    ):
        self.source = source
        self.repo = repository

    def _get_symbol_and_avg_pe(self, symbol: str) -> Tuple[str, float | None]:
        print(f"Processing {symbol}...")

        prices = self.source.get_price_history(symbol)
        if prices.empty:
            print("  Skipped (no price history)")
            return symbol, None

        current_pe = self.source.get_current_pe(symbol)
        if not current_pe:
            print("  Skipped (no PE)")
            return symbol, None

        avg_pe = ValuationCalculator.average_pe(prices, current_pe)
        if avg_pe is None:
            print("  Skipped (insufficient data)")
            return symbol, None

        return symbol, avg_pe


    def run(self, symbols: list[str]):

        sym_avg_pe_list = [self._get_symbol_and_avg_pe(symbol) for symbol in symbols]
        print(sym_avg_pe_list)

        self.repo.upsert_many(
            sym_avg_pe_list
        )
        print("Saved to DB")

# =====================================================
# ENTRY POINT (Dependency Injection)
# =====================================================

if __name__ == "__main__":
    db_conn = DatabaseConnection(**DB_CONFIG).get_connection()

    stocks_repo = StockRepository(db_conn)
    all_stored_stocks = stocks_repo.get_all_stocks_as_list()

    source = YahooFinanceSource()
    repo = ValuationReferenceRepository(db_conn)

    service = ValueReferenceService(source, repo)
    service.run(all_stored_stocks)
