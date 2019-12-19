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

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    handlers=[
                        logging.FileHandler(f"vantage.log"),
                        logging.StreamHandler(sys.stdout)
                    ] )

logger=logging.getLogger() 
logger.setLevel(logging.DEBUG) 

VANTAGE_API_KEY = os.environ['VANTAGE_API_KEY']
STOCK_LIST = os.environ['STOCK_LIST']


def download_stock_list():
    """
    This function will download the list of stocks from the s3 storage and writes it to the companies table in the database
    """
    logging.info("Downloading stock list")
    logging.info("Reading full list from cloud")
    df = pd.read_csv(STOCK_LIST)

    engine = get_pg_engine()

    # Write new stocks
    logging.info("Writing new stocks to db")
    df = clean_columns(df)
    df['companyname'] = df['companyname'].str.replace('\'','')
    df.to_sql('companies', engine, if_exists='replace')
    logging.info("Stock list inserted in database")

def get_symbols(num = 0):
    """
    This function will match the top n stocks ordered by their market cap, where these n stocks don't yet exist in the database.
    Its only argument is the number of stocks to match. This is default -1, which means all the stocks.
    It will also check if these stocks already exist inside the database to avoid making unnecesary API calls.
    """    
    logging.info("Starting symbol matching")
    # Make sure that the number of stocks being obtained is correct
    assert num >= 0, 'Cannot match a negative number of stocks'
    assert isinstance(num, int), 'Number of stocks has to be an integer'

    if num > 500:
        logging.warning("This execution might fail due to API limiting as Alpha Vantage API allows a maximum of 500 calls per day")        


    # Create table to store the symbols in if it doesn't exist
    engine = get_pg_engine()
    engine.execute("CREATE TABLE IF NOT EXISTS symbols (companyname text, symbol text)")
    
    df = read_table('companies')
    df_exist = read_table('symbols')

    # Of the total list, filter those that are already in the database
    df = df[~df['companyname'].isin(df_exist['companyname'])]

    df = df.sort_values(by='companymarketcapm', ascending=False)
    
    if num > 0:
        df = df.head(num)

    logging.info(f"A total of {df.shape[0]} new stocks will be added to the database") 
    
    for _, row in tqdm(df.iterrows(), total = df.shape[0]):
        
        logging.info(f'Fetching company {row["companyname"]}')

        exists = engine.execute(f"SELECT * FROM symbols WHERE companyname = \'{row['companyname']}\'")

        # Check if we already have a symbol for the company 
        if exists.rowcount == 0:
            # Need to sleep for 13 seconds because api is limited to 5 API calls per minute
            time.sleep(13)
            # For some reason, the Alpha Vantage API symbol search endpoint works better when you don't give the full company name
            # I've arbitrarily given it the first 15 characters
            company_keywords = row['companyname'][:15]
            response = requests.get(f'https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={company_keywords}&apikey={VANTAGE_API_KEY}')
            symbol = None
            # It checks for the first symbol of a company that is in the UK
            for res in response.json()['bestMatches']:
                if res["4. region"] == 'United Kingdom':
                    symbol = res['1. symbol']
                    break
            # Write the symbol to the database. If not found, it will insert a 'None' value and we will have to manually fix that. 
            statement = f"INSERT INTO symbols (companyname, symbol) VALUES (\'{row['companyname']}\',\'{symbol}\')"
            engine.execute(statement)
            logging.info(f"Inserted new symbol successfully for {row['companyname']}")
            if symbol is None:
                logging.info(f"Could not match stock {row['companyname']}")
           
        else:
            logging.info(f"Stock ({row['companyname']}) already exists in db")

def symbol_match_status():
    """
    Function to verify how many companies are left to be matched with a symbol
    """
    logging.info("Calculating symbol match status")
    engine = get_pg_engine()
    exists = engine.execute(f"SELECT * FROM symbols WHERE symbol = 'None'")
    logging.info(f'There are {exists.rowcount} companies unmatched')

def fetch_price(symbol):
    """
    Function which takes a company's symbol and fetches the price history for it. It writes the price history back to the database
    """
    engine = get_pg_engine()
    # Check whether table for stock price already exists in the database, otherwise create it.
    engine.execute("CREATE TABLE IF NOT EXISTS price (timestamp date, open double precision,high double precision, low double precision, close double precision, volume double precision, symbol text)")
    df_exist = pd.read_sql('SELECT * from price', engine)

    assert symbol not in df_exist['symbol'].tolist(), f'Price data already exists for this symbol {symbol}'
    assert symbol != 'None', 'Can\'t get a price'
    logging.info(f"Fetching stock with symbol: {symbol}")
    time.sleep(13)

    df = pd.read_csv(f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={symbol}&apikey={VANTAGE_API_KEY}&datatype=csv')
    df['stock'] = symbol
   
    # Write the price data to the database
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, 'price', null="") # null values become ''
    conn.commit()

def manual_symbols():
    """
    Function which allows the user to manually input the missing symbols for the companies
    """

    engine = get_pg_engine()

    df = pd.read_sql("SELECT * from symbols where symbol = 'None'", engine)

    for _, row in df.iterrows():
        symbol = input(f"What is the symbol for {row['companyname']}\n")

        engine.execute(f"UPDATE symbols SET symbol = '{symbol}' WHERE companyname = '{row['companyname']}'")
