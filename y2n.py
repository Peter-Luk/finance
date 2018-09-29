import sqlite3 as lite
import numpy as np
import pandas as pd
import fix_yahoo_finance as yf
from utilities import filepath
from time import sleep

def fetch(code=None, start=None, table='Securities', exclude=[805], years=4, adhoc=True):
    res, conn = {}, lite.connect(filepath(table))
    def stored_eid(table):
        cur = conn.cursor()
        return [_ for _ in [__[0] for __ in cur.execute("SELECT DISTINCT eid FROM records ORDER BY eid ASC").fetchall()] if _ not in exclude]

    if not start:
        start = pd.datetime(pd.datetime.now().year - years, 1, 1)
    if code:
        if isinstance(code, [int, float]): aid = [int(code)]
    else: aid = stored_eid(table=table)
    if adhoc:
        while adhoc:
            ar = yf.download(['{:04d}.HK'.format(_) for _ in aid], start, group_by='ticker')
            if len(ar): adhoc = not adhoc
            else:
                print('Retry in 30 seconds')
                sleep(30)
        for _ in aid:
            hdr = {}
            rd = ar['{:04d}.HK'.format(_)]
            d = rd.T
            hdr['Date'] = [__.to_pydatetime() for __ in rd.index]
            hdr['Data'] = np.asarray(pd.concat([rd[__] for __ in [___ for ___ in d.index if ___ not in ['Adj Close']]], axis=1, join='inner', ignore_index=False))
            res[_] = hdr
    else:
        for _ in aid:
            hdr, qstr = {}, "SELECT date, open, high, low, close, volume FROM records WHERE eid={:d}".format(_)
            if start: qstr += " AND date > '{:%Y-%m-%d}'".format(start)
            q = conn.execute(qstr)
            cols = [c[0].capitalize() for c in q.description]
            rd = pd.DataFrame.from_records(data=q.fetchall(), index='Date', columns=cols)
            hdr['Date'] = [pd.datetime.strptime(__, '%Y-%m-%d') for __ in rd.index]
            hdr['Data'] = np.asarray(rd)
            res[_] = hdr
    return pd.Series(res)
