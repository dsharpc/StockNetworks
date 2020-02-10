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
                        logging.FileHandler(f"logs/price.log"),
                        logging.StreamHandler(sys.stdout)
                    ] )

logger=logging.getLogger() 


def get_existing():
    engine.execute("CREATE TABLE IF NOT EXISTS price (timestamp date, open double precision,high double precision, low double precision, close double precision, volume double precision, symbol text)")
    engine.execute("CREATE TABLE IF NOT EXISTS errors (symbol text, error text, datetime date)")
    df_exist = pd.read_sql('SELECT distinct symbol from (select symbol from price union select symbol from errors) o', engine)
    res = df_exist['symbol'].tolist()
    return res

def fetch_price(symbol):
    """
    Function which takes a company's symbol and fetches the price history for it. It writes the price history back to the database
    """
    engine.execute("CREATE TABLE IF NOT EXISTS \
        price (timestamp date, close double precision, volume double precision, \
            change double precision, change_percent double precision, change_over_time double precision, symbol text)")
    engine.execute("CREATE TABLE IF NOT EXISTS errors (symbol text, error text, datetime date)")

    # Check whether table for stock price already exists in the database, otherwise create it.
    assert symbol != 'None', 'Can\'t get a price'
    logging.info(f"Fetching stock with symbol: {symbol}")

    response = requests.get(f"{os.getenv('IEX_ROOT')}/stable/stock/{symbol}/chart/3m?token={os.getenv('IEX_TOKEN')}&chartCloseOnly=true")
    
    if not response.ok:
        logging.info(f'Could not fetch price for {symbol}, value will be stored in the errors table for manual verification')
        engine.execute(f"INSERT INTO errors (symbol, error, datetime) VALUES ('{symbol}', '{response.status_code}', '{datetime.now().strftime('%Y-%m-%d')}')")

    else:
        df = pd.DataFrame.from_dict(response.json())
        df['symbol'] = symbol

        # Write the price data to the database
        logging.info(f"Symbol {symbol} found, used {response.headers.get('iexcloud-messages-used')}\n inserting price data to database")
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cur.copy_from(output, 'price', null="") # null values become ''
        conn.commit()