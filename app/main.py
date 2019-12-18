from vantage_api import download_stock_list, get_symbols, fetch_price, symbol_match_status, manual_symbols
from utils import get_pg_engine, read_table
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Alpha Vantage API stocks')
    parser.add_argument('-d','--download', action='store_true', help='Download list of companies to database')
    parser.add_argument('-gs','--get_symbols', type=int, default=0, help='Match the stocks\' symbols and how many to match')
    parser.add_argument('-f','--fetch_price', action='store_true', help='Fetch price of existing stocks')
    parser.add_argument('-m','--match_status', action='store_true', help='Status of unmatched companies, how many are there?')
    parser.add_argument('-u','--update_symbols', action='store_true', help='Manually update symbols')

    args = parser.parse_args()

    if args.download:
        download_stock_list()

    if args.get_symbols > 0:
        get_symbols(args.get_symbols)

    if args.fetch_price:
        stocks = read_table('symbols')
        assert len(stocks) > 0, 'There are no records in the symbols table. Please match the symbols using the -gs flag first'
        for sym in stocks['symbol']:
            try:
                fetch_price(sym)
            except AssertionError as e:
                print(e)
                pass

    if args.match_status:
        symbol_match_status()

    if args.update_symbols:
        manual_symbols()