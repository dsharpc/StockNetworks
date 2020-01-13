from vantage_api import fetch_price, RateLimitExceeded
from lse_scraper import get_symbols
from utils import get_pg_engine, read_table
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Alpha Vantage API stocks')
    parser.add_argument('-gs','--get_symbols', action='store_true', help='Scrape the London Stock Exchange website for stock names and symbols')
    parser.add_argument('-f','--fetch_price', action='store_true', help='Fetch price of existing stocks')

    args = parser.parse_args()

    if args.get_symbols:
        get_symbols()

    if args.fetch_price:
        stocks = read_table('symbols')
        assert len(stocks) > 0, 'There are no records in the symbols table. Please match the symbols using the -gs flag first'
        for sym in stocks['vantage_symbol']:
            try:
                fetch_price(sym)
            except AssertionError as e:
                print(e)
                pass
            except RateLimitExceeded as e:
                print(e)
                break