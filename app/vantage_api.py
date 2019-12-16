import requests
import pandas as pd
import os
from sqlalchemy import create_engine 
from sqlalchemy.exc import ProgrammingError
import time
import psycopg2
from utils import clean_columns
import io

VANTAGE_API_KEY = os.environ['VANTAGE_API_KEY']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_DB = os.environ['POSTGRES_DB']
STOCK_LIST = os.environ['STOCK_LIST']

def match_stocks(num = 0):
    """
    This function will match the top n stocks ordered by their market cap. 
    Its only argument is the number of stocks to match. This is default -1, which means all the stocks.
    It will also check if these stocks already exist inside the database to avoid making unnecesary API calls.
    """    
    assert num >= 0, 'Cannot match a negative number of stocks'
    assert isinstance(num, int), 'Number of stocks has to be an integer'

    print("Reading full list from cloud")
    df = pd.read_csv(STOCK_LIST)
    
    # Fetch number of companies to get
    if num == -1:
        df = df.sort_values(by='Company Market Cap (£m)', ascending=False)
    else:
        df = df.sort_values(by='Company Market Cap (£m)', ascending=False).head(num)

    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@pgdb:5432/{POSTGRES_DB}')
    
    engine.execute("CREATE TABLE IF NOT EXISTS symbols (companyname text, symbol text)")
    
    df = clean_columns(df)
    
    try:
        df_exist = pd.read_sql('SELECT companyname, market FROM companies', engine)
    except ProgrammingError:
        df_exist = {'companyname':[]}

    df = df[~df['companyname'].isin(df_exist['companyname'])]

    print("Writing new stocks to db")
    df.to_sql('companies', engine, if_exists='append')
    print(df.shape)
    
    for _, row in df.iterrows():
        
        print(f'Fetching company {row["companyname"]}')

        exists = engine.execute(f"SELECT * FROM symbols WHERE companyname = \'{row['companyname']}\'")

        if exists.rowcount == 0:
            time.sleep(13)
            company_keywords = row['companyname'][:10]
            response = requests.get(f'https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={company_keywords}&apikey={VANTAGE_API_KEY}')
            symbol = None
            for res in response.json()['bestMatches']:
                if res["4. region"] == 'United Kingdom':
                    symbol = res['1. symbol']
                    break
            statement = f"INSERT INTO symbols (companyname, symbol) VALUES (\'{row['companyname']}\',\'{symbol}\')"
            engine.execute(statement)
            print("Inserted new symbol successfully")
            if symbol is None:
                print(f"Could not match stock {row['companyname']}")
           
        else:
            print('Stock already exists in db')

def fetch_price(symbol):
    """
    Function which takes a company's symbol and fetches the price history for it. It writes the price history back to the database
    """
    print(f"Fetching stock with symbol: {symbol}")
    time.sleep(13)
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@pgdb:5432/{POSTGRES_DB}')
    
    engine.execute("CREATE TABLE IF NOT EXISTS price (timestamp date, open double precision,high double precision, low double precision, close double precision, volume double precision, symbol text)")

    df = pd.read_csv(f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={symbol}&apikey={VANTAGE_API_KEY}&datatype=csv')
    df['stock'] = symbol
   
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, 'price', null="") # null values become ''
    conn.commit()