from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
import time
from datetime import datetime
from random import randint


def get_symbols(use_scraperapi = True):
    """
    Function which scrapes the Stock's names and symbols from the London Stock Exchange website
    """
    if ~os.path.exists("symbols.csv"):
        df = pd.DataFrame({'symbol':[],'company':[], 'price':[], 'currency':[], 'scrape_timestamp':[], 'page':[], 'url':[]})
        url = 'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/main-market/main-market.html?marketCode=MAINMARKET'
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
            print(f"Fetching {page_num}")

            for row in soup.find('table', class_='table_dati').find('tbody').find_all('tr'):
                values = row.find_all('td')
                state = {'symbol': values[0].get_text(),\
                        'company':values[1].find('a').get_text(),\
                        'price':values[3].get_text(),\
                        'currency':values[2].get_text(),\
                        'scrape_timestamp':datetime.today().isoformat(),
                        'page':page_num.strip().split(' ')[1],
                        'url':url}
                states.append(state)
            try:
                url = 'https://www.londonstockexchange.com' + soup.find('div', class_='paging').find('p', class_='aligndx').find('a', title='Next')['href']
            except KeyError:
                url = None 

            df = df.append(pd.DataFrame(states))
            df.to_csv('symbols.csv').set_index('symbol')
    else:
        df.read_csv('symbols.csv')
    
    return df