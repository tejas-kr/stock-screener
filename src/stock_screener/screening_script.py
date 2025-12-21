"""
download_indian_stock_data.py

Produces CSV with columns:
1. Sub-Sector
2. Market Cap
3. Close Price
4. PE Ratio
5. 1M Return
6. 1D Return
7. Return on Equity

Default universe: current NIFTY-50 constituents (scraped from Wikipedia).
"""

import pandas as pd
import numpy as np
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def get_nifty50_tickers() -> list[str]:
    """
    Scrape NIFTY-50 constituents from Wikipedia and return a list of tickers.

    Returns:
        list[str]: List of stock tickers (without .NS suffix)

    Raises:
        RuntimeError: If the table cannot be found or parsed
        requests.RequestException: If the HTTP request fails
    """
    url = "https://en.wikipedia.org/wiki/NIFTY_50"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        logging.info("Fetching NIFTY-50 constituents from Wikipedia...")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all('table', {'class': 'wikitable'})

        if not tables:
            raise RuntimeError("No wikitable found on the Wikipedia page")

        for table in tables:
            try:
                df = pd.read_html(str(table))[0]

                # Look for a column that likely contains tickers
                for col in df.columns:
                    if any(x in col.lower() for x in ['symbol', 'ticker', 'code', 'company']):
                        tickers = df[col].astype(str).str.strip().tolist()
                        # Clean up tickers (remove .NS, .BO suffixes if present)
                        tickers = [
                            t.split('.')[0].strip()
                            for t in tickers
                            if t and t.lower() not in ['symbol', 'ticker', 'code', 'company', 'nan']
                        ]
                        if tickers:  # If we found valid tickers
                            logging.info(f"Successfully extracted {len(tickers)} tickers")
                            return tickers

            except Exception as e:
                logging.debug(f"Skipping table due to: {str(e)}")
                continue

        # If we get here, no valid table was found
        raise RuntimeError("Could not find a valid table with tickers on the Wikipedia page")

    except requests.RequestException as e:
        logging.error(f"Failed to fetch data from Wikipedia: {str(e)}")
        raise

def ensure_ns_suffix(ticker):
    """Return ticker with .NS if not present (for NSE on Yahoo Finance)"""
    ticker = ticker.strip()
    if ticker.endswith('.NS') or ticker.endswith('.BO'):
        return ticker
    return ticker + '.NS'

def compute_roe(ticker_obj):
    """
    Compute trailing ROE from last available annual info:
    ROE = (Net Income) / (Total Stockholder Equity)
    Uses yfinance .financials (income statement) and .balance_sheet.
    Returns float or np.nan
    """
    try:
        fin = ticker_obj.financials  # annual income statement (columns are years)
        bs = ticker_obj.balance_sheet  # annual balance sheet
        if fin is None or bs is None or fin.empty or bs.empty:
            return np.nan
        # pick latest column (first column)
        net_income_row = [r for r in fin.index if 'Net Income' in r or 'NetIncome' in r or 'Net income' in r]
        if net_income_row:
            net_income = fin.loc[net_income_row[0]].iloc[0]
        else:
            # try common alternative labels
            possible = ['Net Income', 'NetIncome', 'Profit for the period', 'Net income']
            found = None
            for k in possible:
                if k in fin.index:
                    found = k
                    break
            if found:
                net_income = fin.loc[found].iloc[0]
            else:
                # try last row
                net_income = fin.iloc[-1,0]

        # Total stockholders equity common labels
        eq_row = None
        for label in ['Total Stockholder Equity', 'Total stockholders\' equity', 'Total Equity', 'Total shareholders\' equity', 'Total equity']:
            if label in bs.index:
                eq_row = label
                break
        if eq_row is None:
            # fallback: try last row
            total_equity = bs.iloc[-1,0]
        else:
            total_equity = bs.loc[eq_row].iloc[0]

        # guard against zero/div by nan
        if pd.isna(net_income) or pd.isna(total_equity) or total_equity == 0:
            return np.nan
        roe = float(net_income) / float(total_equity)
        return roe
    except Exception as e:
        logging.debug(f"ROE compute error for {ticker_obj.ticker}: {e}")
        return np.nan

def fetch_stock_data(tickers, out_csv="indian_stock_data.csv", pause_seconds=0.5):
    """
    tickers: list of tickers WITHOUT .NS suffix (function will append .NS)
    """
    rows = []
    today = datetime.now().date()
    one_month_ago = today - timedelta(days=30)

    for t in tickers:
        yf_ticker = ensure_ns_suffix(t)
        logging.info(f"Fetching {yf_ticker}")
        try:
            tk = yf.Ticker(yf_ticker)
            info = tk.info  # may be rate-limited or missing fields
        except Exception as e:
            logging.warning(f"Failed to init yfinance Ticker for {yf_ticker}: {e}")
            info = {}

        # Sub-Sector: try "industry" first then "sector"
        sub_sector = info.get('industry') or info.get('sector') or np.nan

        market_cap = info.get('marketCap', np.nan)

        # Close Price: use recent history
        close_price = np.nan
        try:
            hist = tk.history(period='7d', interval='1d')
            if hist is not None and not hist.empty:
                close_price = float(hist['Close'].iloc[-1])
        except Exception as e:
            logging.debug(f"Could not fetch recent history for {yf_ticker}: {e}")

        # PE Ratio
        pe_ratio = info.get('trailingPE') or info.get('forwardPE') or np.nan

        # 1D Return: (close_today - prev_close)/prev_close
        one_d_return = np.nan
        try:
            if hist is not None and len(hist) >= 2:
                prev_close = float(hist['Close'].iloc[-2])
                if prev_close != 0:
                    one_d_return = (close_price - prev_close) / prev_close
        except Exception:
            one_d_return = np.nan

        # 1M Return: use 30 calendar days historical close (or 21 trading days)
        one_m_return = np.nan
        try:
            hist_1m = tk.history(period='35d', interval='1d')  # cover ~1 month
            if hist_1m is not None and not hist_1m.empty:
                # find earliest date >= one_month_ago
                hist_1m = hist_1m.dropna(subset=['Close'])
                if not hist_1m.empty:
                    # pick first valid close older than or equal to ~30 days before last
                    first_close = hist_1m['Close'].iloc[0]
                    if first_close != 0:
                        one_m_return = (close_price - first_close) / first_close
        except Exception as e:
            logging.debug(f"1M return error for {yf_ticker}: {e}")

        # ROE
        roe = compute_roe(tk)

        rows.append({
            'Ticker': t,
            'Sub-Sector': sub_sector,
            'Market Cap': market_cap,
            'Close Price': close_price,
            'PE Ratio': pe_ratio,
            '1M Return': one_m_return,
            '1D Return': one_d_return,
            'Return on Equity': roe
        })

        time.sleep(pause_seconds)

    df = pd.DataFrame(rows)
    # convert ratios to percentages where helpful (keep in decimal for ROE/returns)
    df.to_csv(out_csv, index=False)
    logging.info(f"Wrote results to {out_csv} ({len(df)} rows)")
    return df

if __name__ == "__main__":
    # default: NIFTY-50
    try:
        tickers = get_nifty50_tickers()
        logging.info(f"Found {len(tickers)} tickers from NIFTY-50 (sample: {tickers[:5]})")
    except Exception as e:
        logging.warning(f"Could not fetch NIFTY-50 tickers automatically: {e}")
        # fallback example list
        tickers = ['RELIANCE', 'TCS', 'HDFC', 'HDFC', 'INFY', 'HINDUNILVR']  # replace with your tickers

    # out_csv = f"indian_stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    # df = fetch_stock_data(tickers, out_csv=out_csv)
    # # simple display
    # print(df.head(20))
