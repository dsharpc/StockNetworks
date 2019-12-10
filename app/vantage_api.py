import requests
import pandas as pd
import os
from py2neo import Graph, Node, NodeMatcher
import time

VANTAGE_API_KEY = os.environ['VANTAGE_API_KEY']
NEO4J_USER = os.environ['NEO4J_USER']
NEO4J_PASS = os.environ['NEO4J_PASS']
STOCK_LIST = os.environ['STOCK_LIST']

def match_stocks(num = 0):
    """
    This function will match the top n stocks ordered by their market cap. 
    Its only argument is the number of stocks to match. This is default -1, which means all the stocks.
    It will also check if these stocks already exist inside the database to avoid making unnecesary API calls.
    """    
    assert num >= 0, 'Cannot match a negative number of stocks'
    assert isinstance(num, int), 'Number of stocks has to be an integer'

    df = pd.read_csv(STOCK_LIST)
    
    if num == -1:
        df = df.sort_values(by='Company Market Cap (£m)', ascending=False)
    else:
        df = df.sort_values(by='Company Market Cap (£m)', ascending=False).head(num)


    g = Graph(host='db',user = NEO4J_USER, password = NEO4J_PASS)
    matcher = NodeMatcher(g)
    tx = g.begin()
    for _, row in df.iterrows():
        
        print(f'Fetching company {row["Company Name"]}')

        if matcher.match("Stock", company=row['Company Name']).first() is None:
            time.sleep(13)
            company_keywords = row['Company Name'][:10]
            response = requests.get(f'https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={company_keywords}&apikey={VANTAGE_API_KEY}')
            symbol = None
            for res in response.json()['bestMatches']:
                if res["4. region"] == 'United Kingdom':
                    symbol = res['1. symbol']              

            if symbol is None:
                print(f"Could not match stock {row['Company Name']}")
            n = Node("Stock", symbol = symbol, company = row['Company Name'], industry = row['ICB Industry'], \
            sub_sector = row['ICB Super-Sector'], country = row['Country of Incorporation'], market_cap = row['Company Market Cap (£m)'])
            tx.create(n)
        else:
            print('Stock already exists in db')

    tx.commit()