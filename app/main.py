from vantage_api import match_stocks, fetch_price
import argparse
import os
from sqlalchemy import create_engine 
import pandas as pd

POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_DB = os.environ['POSTGRES_DB']

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Alpha Vantage API stocks')
    parser.add_argument('stocks', type=int, help='Number of stocks to process. Choose -1 if all')

    args = parser.parse_args()
    match_stocks(args.stocks)
    
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@pgdb:5432/{POSTGRES_DB}')
    
    stocks = pd.read_sql('SELECT companyname, symbol FROM symbols', engine)

    for sym in stocks['symbol']:
        fetch_price(sym)