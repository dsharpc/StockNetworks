from datetime import datetime
from utils import get_pg_engine
import pandas as pd




def build_correlations(start_date = '2019-01-01', end_date = datetime.now().strftime('%Y-%m-%d'), num_stocks = 1000):
    

    assert num_stocks > 10, 'To build an interesting analysis, make sure the number of stocks to use is at least 10'

    start_date_fmt = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_fmt = datetime.strptime(end_date, '%Y-%m-%d')
    diff = end_date_fmt - start_date_fmt
    assert int(diff.days/7) > 12, 'For more meaningful correlations increase the window between the start_date and end_date'

    engine = get_pg_engine()

    engine.execute("CREATE TABLE IF NOT EXISTS correlations (window_id text, start_date date, end_date date, symbol_a text, symbol_b text, correlation double precision)")

    stocks = pd.read_sql(f'select symbol, sum(volume) as volume \
                       from price \
                       where timestamp between {start_date} and {end_date} \
                       group by symbol \
                       order by sum(volume) desc \
                       limit {num_stocks}', engine)
       
    