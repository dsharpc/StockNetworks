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