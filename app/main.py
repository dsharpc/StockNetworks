from vantage_api import fetch_price, drop_existing, RateLimitExceededException
from lse_scraper import get_symbols
from stock_analyser import build_correlations
from utils import get_pg_engine, read_table
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Alpha Vantage API stocks')


    # parser.add_argument('-a',
    #                     choices = ['gs','f','cor'],
    #                     dest='action',
    #                     required=True,
    #                     action='store',
    #                     help=help_text)

    subparsers = parser.add_subparsers(help='Action type to perform')

    parser_gs = subparsers.add_parser('gs', help = "'gs' for getting Symbols by scraping the London Stock Exchange website")
    parser_f = subparsers.add_parser('f', help = "'f' for fetching prices from the Alpha Vantage API")


    help_text="""
                + 'cor' for calculating the pairwise correlations between the stocks, this command has 3 arguments:\n
                    \t+ '-sd' : start date for price data
                    \t+ '-ed' : end data for price data
                    \t+ '-ns' : number of stocks to correlate (these are ordered by transaction volume over the previous period)
              """
    parser_corr = subparsers.add_parser('cor', help = help_text)
    # parser_corr.add_argument('-sd', '--start_date', default='2019-01-01')
    # parser_corr.add_argument('-ed', '--end_date',default='2020-01-01')
    # parser_corr.add_argument('-ns', '--number_stocks',type=int, default=1000)

    args = parser.parse_args()
    args_corr= parser_corr.parse_args()
    print(args)
    print(args_corr)

    # if args.action == 'gs':
    #     get_symbols()

    # if args.action == 'f':
    #     stocks = read_table('symbols')
    #     assert len(stocks) > 0, 'There are no records in the symbols table. Please match the symbols using the gs flag first'
    #     stocks = drop_existing(stocks)
    #     for sym in stocks['vantage_symbol']:
    #         try:
    #             fetch_price(sym)
    #         except AssertionError as e:
    #             print(e)
    #             pass
    #         except RateLimitExceededException as e:
    #             print(e)
    #             break
    
    # if args.action == 'cor':
    #     build_correlations(parser_corr.start_date, parser_corr.end_date, parser_corr.number_stocks)


    