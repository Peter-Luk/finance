import sqlite3 as lite
import numpy as np
import pandas as pd
import fix_yahoo_finance as yf
from utilities import filepath, datetime
from time import sleep

class Futures(object):
    def __init__(self, db='Futures'):
        self.__conn = lite.connect(filepath(db))
        self.__conn.row_factory = lite.Row

    def __del__(self):
        self.__conn = None
        del(self.__conn)

    def combine(self, code, type='daily'):
        res = {}
        for __ in [_['date'] for _ in self.__conn.execute("SELECT DISTINCT date FROM records WHERE code='{}'".format(code)).fetchall()]:
            _ = self.__conn.execute("SELECT session, open, high, low, close, volume FROM records WHERE code='{}' AND date='{}'".format(code, __)).fetchall()
            if _:
                tmp = {'open': _[0]['open'], 'high': _[0]['high'], 'low': _[0]['low'], 'close': _[0]['close'], 'volume': _[0]['volume']}
                if len(_) > 1:
                    for ___ in _:
                        if ___['session'] == 'a':
                            if ___['high'] > tmp['high']: tmp['high'] = ___['high']
                            if ___['low'] < tmp['low']: tmp['low'] = ___['low']
                            tmp['close'] = ___['close']
                            tmp['volume'] += ___['volume']
            res[datetime.strptime(__, '%Y-%m-%d')] = [tmp['open'], tmp['high'], tmp['low'], tmp['close'], tmp['volume']]
        hdr = {}
        hdr['Date'] = [_.date() for _ in res.keys()]
        hdr['Data'] = np.array(list(res.values()))
        return hdr

class Equities(object):
    def __init__(self, db='Securities'):
        self.__conn = lite.connect(filepath(db))

    def __del__(self):
        self.__conn = None
        del(self.__conn)

    def fetch(self, code=None, start=None, table='records', exclude=[805], years=4, adhoc=False):
        res = {}
        def stored_eid(table):
            cur = self.__conn.cursor()
            return [_ for _ in [__[0] for __ in cur.execute("SELECT DISTINCT eid FROM {} ORDER BY eid ASC".format(table)).fetchall()] if _ not in exclude]

        if not start:
            start = pd.datetime(pd.datetime.now().year - years, 1, 1)
        aid = stored_eid(table=table)
        if code:
            if isinstance(code, (tuple, list)): aid = list(code)
            if isinstance(code, (int, float)):
                if int(code) == 5: aid = [11, int(code)]
                else: aid = [5, int(code)]
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
                hdr, qstr = {}, "SELECT date, open, high, low, close, volume FROM {} WHERE eid={:d}".format(table, _)
                if start: qstr += " AND date > '{:%Y-%m-%d}'".format(start)
                q = self.__conn.execute(qstr)
                cols = [c[0].capitalize() for c in q.description]
                rd = pd.DataFrame.from_records(data=q.fetchall(), index='Date', columns=cols)
                hdr['Date'] = [pd.datetime.strptime(__, '%Y-%m-%d') for __ in rd.index]
                hdr['Data'] = np.asarray(rd)
                res[_] = hdr
        return pd.Series(res)
