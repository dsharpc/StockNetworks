from vantage_api import match_stocks, fetch_price, read_table
from utils import get_pg_engine
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Alpha Vantage API stocks')
    parser.add_argument('stocks', type=int, help='Number of stocks to process. Choose -1 if all')
    parser.add_argument('-m','--match', action='store_true', help='Only match stocks')
    parser.add_argument('-f','--fetch_price', action='store_true', help='Only fetch price of existing stocks')

    args = parser.parse_args()
    engine = get_pg_engine()

    stocks = read_table('symbols')

    if args.match is True and args.fetch_price is False:
        match_stocks(args.stocks)
    elif args.match is False and args.fetch_price is True:
        assert stocks is None, 'There are no companies in the symbols table and thus we cannot fetch the price'
        for sym in stocks['symbol']:
            fetch_price(sym)
    else:
        match_stocks(args.stocks)
        for sym in stocks['symbol']:
            fetch_price(sym)