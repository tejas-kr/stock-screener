from src.stock_screener.stock_symbols.nifty_csv_grabber import NiftyIndexSaver


def grab_nifty_index():
    nifty_index_saver = NiftyIndexSaver()
    nifty_index_saver.scrape_and_download()
