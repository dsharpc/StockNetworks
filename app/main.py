from price import fetch_price, get_existing, RateLimitExceededException
from symbol import get_symbols
from correlations import build_correlations
from utils import get_pg_engine, read_table
import argparse


def symbols(namespace):
    get_symbols()

def prices(namespace):
    e = get_existing()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stock exploration using Graph Networks')

    subparsers = parser.add_subparsers(help='Action type to perform')

    parser_gs = subparsers.add_parser('gs', help = "'gs' for getting Symbols from the API")
    parser_gs.set_defaults(func=symbols)

    parser_f = subparsers.add_parser('p', help = "'p' for fetching prices from the API")
    parser_f.add_argument('-s','--stock', default='*', help='Stock symbol to fetch, if not given, it will pull price for all stocks')
    parser_f.set_defaults(func=prices)


    help_text="""
                + 'cor' for calculating the pairwise correlations between the stocks, this command has 3 arguments:\n
                    \t+ '-sd' : start date for price data
                    \t+ '-ed' : end data for price data
                    \t+ '-ns' : number of stocks to correlate (these are ordered by transaction volume over the previous period)
              """
    parser_corr = subparsers.add_parser('cor', help = help_text)

    args = parser.parse_args()
    