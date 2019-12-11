import pandas as pd
import string

def clean_columns(df):
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('[{}]'.format(string.punctuation), '')
    return df