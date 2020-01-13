from bs4 import BeautifulSoup
from utils import get_pg_engine
import requests
import pandas as pd
import os
import time
from datetime import datetime
from random import randint
import logging
import sys

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    handlers=[
                        logging.FileHandler(f"lse_scraper.log"),
                        logging.StreamHandler(sys.stdout)
                    ] )

logger=logging.getLogger() 


def get_max_page():
    engine = get_pg_engine()
    try:
        lp = pd.read_sql('Select max(page) from symbols', engine).values[0][0]
    except:
        lp = 1
    return lp

def get_symbols(use_scraperapi = True, full_refresh = False):
    """
    Function which scrapes the Stock's names and symbols from the London Stock Exchange website
    """
    engine = get_pg_engine()
    if full_refresh:
        engine.execute("DROP TABLE IF EXISTS symbols")

    page_num = get_max_page()

    url = f'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/main-market/main-market.html?marketCode=MAINMARKET&page={page_num}'
    while url is not None:
        if use_scraperapi:
            payload = {'api_key': os.environ['SCRAPERAPI_KEY'], 'url':url}
            response = requests.get('http://api.scraperapi.com', payload)
        else:
            response = requests.get(url)
            
        assert response.status_code == 200, f'API call was not successful, response code was {response.status_code} with content {response.content}'

        soup = BeautifulSoup(response.content, 'html.parser')
        states=[]

        page_num = soup.find('div', class_='paging').find('p', class_='floatsx').get_text()
        logging.info(f"Fetching {page_num} from url: {url}")

        for row in soup.find('table', class_='table_dati').find('tbody').find_all('tr'):
            values = row.find_all('td')
            state = {'symbol': values[0].get_text(),\
                    'vantage_symbol' : values[0].get_text()+'.LON',\
                    'company':values[1].find('a').get_text(),\
                    'price':values[3].get_text(),\
                    'currency':values[2].get_text(),\
                    'scrape_timestamp':datetime.today().isoformat(),
                    'page':page_num.strip().split(' ')[1],
                    'url':url}
            states.append(state)
        try:
            url = 'https://www.londonstockexchange.com' + soup.find('div', class_='paging').find('p', class_='aligndx').find('a', title='Next')['href']
        except TypeError:
            url = None 
            logging.info("Scraping finished successfully"")

        df = pd.DataFrame(states)
        df.set_index('symbol').to_sql('symbols', engine, if_exists='append')
