from utils import get_pg_engine
import requests
import pandas as pd
import os
import logging
import sys

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    handlers=[
                        logging.FileHandler(f"logs/lse_scraper.log"),
                        logging.StreamHandler(sys.stdout)
                    ] )

logger=logging.getLogger() 



def get_symbols(exchange = 'LON',full_refresh = False):
    """
    Function which scrapes the Stock's names and symbols from the London Stock Exchange website
    """
    engine = get_pg_engine()
    if full_refresh:
        engine.execute("DROP TABLE IF EXISTS symbols")

    url = f"{os.getenv('IEX_ROOT')}/stable/ref-data/exchange/{exchange}/symbols?token={os.getenv('IEX_TOKEN')}"
    response = requests.get(url)
    if response.ok:
        df = pd.DataFrame.from_dict(response.json())
        logger.info(f"Found {len(df)} stock symbols, writing to DB")
        df.set_index('symbol').to_sql('symbols', engine, if_exists='append')
    else:
        logger.info(f"API call not successful, error: {response.text}")


def get_nonexisting(shuffle = True, types=['cs']):
    engine = get_pg_engine()
    engine.execute("CREATE TABLE IF NOT EXISTS price (timestamp date, open double precision,high double precision, low double precision, close double precision, volume double precision, symbol text)")
    engine.execute("CREATE TABLE IF NOT EXISTS errors (symbol text, error text, datetime date)")
    query = f"""
    with existing as (
    SELECT distinct symbol from (select symbol from price union select symbol from errors) o
    )
    select symbol from symbols where symbol not in (select * from existing) and type in ({','.join([f"'{t}'" for t in types])})
    """
    df = pd.read_sql(query, engine)
    if shuffle:
        res = df.sample(frac=1)['symbol'].tolist()
    else:
        res = df.sample(frac=1)['symbol'].tolist()
    return res