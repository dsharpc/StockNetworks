import requests
import pandas as pd
import os
from sqlalchemy.exc import ProgrammingError
import time
import psycopg2
from utils import clean_columns, get_pg_engine, read_table
import io

VANTAGE_API_KEY = os.environ['VANTAGE_API_KEY']
STOCK_LIST = os.environ['STOCK_LIST']


# NEED TO SPLIT THIS FUNCTION INTO DOWNLOAD STOCKS AND THEN GET SYMBOLS, OTHERWISE, IT WON'T WORK ON A SECON RERUN
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

    # Create table to store the symbols in
    engine = get_pg_engine()
    engine.execute("CREATE TABLE IF NOT EXISTS symbols (companyname text, symbol text)")
    
    df = clean_columns(df)
    
    # Fetch list of companies that are already in the database, if the table doesn't exist, then return an empty array
    df_exist = read_table('companies')

    if df_exist is None:
        df_exist = {'companyname':[]}
    # Of the total list, filter those that are already in the database
    df = df[~df['companyname'].isin(df_exist['companyname'])]

    # Write new stocks
    print("Writing new stocks to db")
    df.to_sql('companies', engine, if_exists='append')

    print(f"A total of {df.shape[0]} new stocks will be added to the database") 
    
    for _, row in df.iterrows():
        
        print(f'Fetching company {row["companyname"]}')

        exists = engine.execute(f"SELECT * FROM symbols WHERE companyname = \'{row['companyname']}\'")

        # Check if we already have a symbol for the company 
        if exists.rowcount == 0:
            time.sleep(13)\
            # For some reason, the Alpha Vantage API symbol search endpoint works better when you don't give the full company name
            # I've arbitrarily given it the first 10 characters
            company_keywords = row['companyname'][:10]
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
    engine = get_pg_engine()
    
    # Check whether table for stock price already exists in the database, otherwise create it.
    engine.execute("CREATE TABLE IF NOT EXISTS price (timestamp date, open double precision,high double precision, low double precision, close double precision, volume double precision, symbol text)")

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