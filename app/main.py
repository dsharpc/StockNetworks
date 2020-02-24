from price import fetch_price
from symbol import get_symbols, get_nonexisting
from correlations import build_correlations
from neo4j import NeoGraph
from utils import get_pg_engine, read_table
import argparse
from tqdm import tqdm
from datetime import datetime

def symbols(namespace):
    get_symbols(namespace.exchange)

def prices(namespace):
    ne = get_nonexisting()
    if namespace.stock != '*':
        fetch_price(namespace.stock)
    else:
        for s in tqdm(ne):
            fetch_price(s)

def correlate(namespace):
    build_correlations(namespace.start_date, namespace.end_date, namespace.num_stocks)


def to_neo(namespace):
    ng = NeoGraph()
    if not namespace.dont_truncate:
        ng.truncate()
    symbols = read_table('symbols')
    var_coef = read_table('coef_variation')
    cor = read_table(namespace.corr_id)
    cor = cor.query('cor == cor')
    symbols = symbols[(symbols['symbol'].isin(cor['symbol1'])) | (symbols['symbol'].isin(cor['symbol2']))]
    symbols = symbols.merge(var_coef,on='symbol', how='left')
    ng.add_companies(symbols)
    ng.create_links(cor)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stock exploration using Graph Networks')

    subparsers = parser.add_subparsers(help='Action type to perform')

    parser_gs = subparsers.add_parser('gs', help = "'gs' for getting Symbols from the API")
    parser_gs.add_argument('-e','--exchange', help="Stock Exchange identifier, by default it's LON for London Stock Exchange")
    parser_gs.set_defaults(func=symbols)

    parser_f = subparsers.add_parser('p', help = "'p' for fetching prices from the API")
    parser_f.add_argument('-s','--stock', default='*', help='Stock symbol to fetch, if not given, it will pull price for all stocks')
    parser_f.set_defaults(func=prices)


    help_text="""
                + 'cor' for calculating the pairwise correlations between the stocks, this command has 3 arguments:\n
                    \t+ '-sd' : start date for price data (YYYY-MM-DD)
                    \t+ '-ed' : end data for price data
                    \t+ '-ns' : number of stocks to correlate (these are ordered by transaction volume over the previous period)
              """
    parser_corr = subparsers.add_parser('cor', help = help_text)
    parser_corr.add_argument('-sd','--start-date', default = '2019-11-01', help='Format is YYYY-MM-DD')
    parser_corr.add_argument('-ed','--end-date', default = datetime.now().strftime('%Y-%m-%d'),help='Format is YYYY-MM-DD')
    parser_corr.add_argument('-ns','--num-stocks', default = 1000, type=int)
    parser_corr.set_defaults(func=correlate)

    parser_neo = subparsers.add_parser('neo', help = 'Add stock data to Neo4j database')
    parser_neo.add_argument('-dt','--dont-truncate', action='store_true')
    parser_neo.add_argument('-c','--corr-id', default = 'correlations_2019_11_01_2020_02_11_1000', help='Name of the correlation table to use')
    parser_neo.set_defaults(func=to_neo)

    args = parser.parse_args()
    args.func(args)
    