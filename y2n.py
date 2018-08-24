import sqlite3 as lite
import numpy as np
import pandas as pd
import fix_yahoo_finance as yf
from utilities import filepath
from time import sleep

def fetch():
    def stored_eid():
        conn = lite.connect(filepath('Securities'))
        cur = conn.cursor()
        return [_ for _ in [__[0] for __ in cur.execute("SELECT DISTINCT eid FROM records ORDER BY eid ASC").fetchall()] if _ not in [805]]
    res, aid, process = {}, stored_eid(), True
    while process:
        ar = yf.download(['{:04d}.HK'.format(_) for _ in aid], '2016-01-01', group_by='ticker')
        if len(ar): process = not process
        else:
            print('Retry in 10 seconds')
            sleep(10)
    for _ in aid:
        rd = ar['{:04d}.HK'.format(_)]
        d = rd.T
        res[_] = np.asarray(pd.concat([rd[__] for __ in [___ for ___ in d.index if ___ not in ['Adj Close']]], axis=1, join='inner', ignore_index=False))
    return pd.Series(res)
