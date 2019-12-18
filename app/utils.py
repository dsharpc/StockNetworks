import pandas as pd
import string
from sqlalchemy import create_engine 
import os
from sqlalchemy.exc import ProgrammingError

POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_DB = os.environ['POSTGRES_DB']

def clean_columns(df):
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('[{}]'.format(string.punctuation), '').str.replace('£','')
    return df


def get_pg_engine():
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@pgdb:5432/{POSTGRES_DB}')
    return engine

def read_table(table_name, limit = 0):
    engine = get_pg_engine()
    assert limit >= 0, 'Limit must be positive'
    assert isinstance(limit, int), 'Limit has to be an integer'
    if limit == 0:
        query = f"SELECT * FROM {table_name}"
    else:
        query = f"SELECT * FROM {table_name} LIMIT {limit}"

    try:
        df = pd.read_sql(query, engine)
    except ProgrammingError:
        df = None
    
    return df
