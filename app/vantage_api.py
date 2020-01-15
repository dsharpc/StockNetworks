import requests
import pandas as pd
import os
from sqlalchemy.exc import ProgrammingError
import time
import psycopg2
from utils import clean_columns, get_pg_engine, read_table
import io
import logging
from tqdm import tqdm
from datetime import datetime
import sys


engine = get_pg_engine()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    handlers=[
                        logging.FileHandler(f"logs/vantage.log"),
                        logging.StreamHandler(sys.stdout)
                    ] )

logger=logging.getLogger() 

VANTAGE_API_KEY = os.environ['VANTAGE_API_KEY']

class RateLimitExceededException(Exception):
    def __init__(self,msg=None):
        if msg is None:
            msg = "API Rate Limit Exceeded"


def drop_existing(df):
    df = df.copy()
    df_exist = pd.read_sql('SELECT distinct symbol from (select symbol from price union select symbol from errors) o', engine)
    print(f"Removed {sum(df['vantage_symbol'].isin(df_exist['symbol']))} records which were already in the database")
    df = df[~df['vantage_symbol'].isin(df_exist['symbol'])]
    return df

def fetch_price(symbol):
    """
    Function which takes a company's symbol and fetches the price history for it. It writes the price history back to the database
    """

    # Check whether table for stock price already exists in the database, otherwise create it.
    engine.execute("CREATE TABLE IF NOT EXISTS price (timestamp date, open double precision,high double precision, low double precision, close double precision, volume double precision, symbol text)")
    engine.execute("CREATE TABLE IF NOT EXISTS errors (symbol text, error text, datetime date)")

    assert symbol != 'None', 'Can\'t get a price'
    logging.info(f"Fetching stock with symbol: {symbol}")
    time.sleep(13)

    df = pd.read_csv(f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={symbol}&apikey={VANTAGE_API_KEY}&datatype=csv')
    df['stock'] = symbol
    
    if 'Error' in df.iloc[0][0]:
        logging.info(f'Could not fetch price for {symbol}, value will be stored in the errors table for manual verification')
        engine.execute(f"INSERT INTO errors (symbol, error, datetime) VALUES ('{symbol}', 'Not matched to VantageAPI', '{datetime.now().strftime('%d-%m-%Y')}')")
    elif 'Information' in df.iloc[0][0]:
        logging.info(f"Could not fetch price for {symbol} as call limit was exceeded")
        engine.execute(f"INSERT INTO errors (symbol, error) VALUES ('{symbol}', 'Rate limit exceeded')")
        raise RateLimitExceededException
    else:
        # Write the price data to the database
        logging.info(f"Symbol {symbol} found, inserting price data to database")
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cur.copy_from(output, 'price', null="") # null values become ''
        conn.commit()