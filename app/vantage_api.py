import requests
import pandas as pd
import os

VANTAGE_API_KEY = os.environ['VANTAGE_API_KEY']

def match_stocks(num = -1):
    df = pd.read_csv('https://daniel-sharp.s3-us-west-2.amazonaws.com/stock-networks/LSE_companies.csv')