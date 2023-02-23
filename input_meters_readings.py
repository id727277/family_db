from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import datetime 

engine = create_engine("sqlite:///family.db")

# replace values here for each new month
meters_data = pd.DataFrame({
    'dtm': pd.to_datetime(datetime.now(),format='%Y-%m-%d %H:%M:%S'),
    'meter': ['86076', '89264', '200503854', '83616', 'T1', 'T2', 'T3'],
    'data': [53.939, 187.251, 34.298, 9.313, 2307, 1052, 2978]
})

if __name__ == '__main__':

    meters_data.to_sql('meters_data', con=engine, index=False, if_exists='append')
    query = 'select count(*) as c from meters_data'
    response = pd.read_sql(query, engine)['c'][0]
    print(response)