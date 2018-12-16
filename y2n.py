import sqlite3 as lite
import numpy as np
import pandas as pd
import fix_yahoo_finance as yf
from nta import Viewer
from utilities import filepath, datetime
from time import sleep

class Futures(Viewer):
    k_period = {'atr':14, 'er':7, 'fast':2, 'slow':14}
    x_period = 7
    def __init__(self, code, db='Futures'):
        self.__conn = lite.connect(filepath(db))
        self.__conn.row_factory = lite.Row
        rc = entities(db).tolist()
        if code.upper() not in rc: code = rc[-1]
        self.data = self.combine(code)
        self._v = Viewer(self.data)
        self.date = self.data['Date'][-1]
        self.close = self.data['Data'][-1, -2]

    def __del__(self):
        self.__conn = self.data = self._v = self.date = self.close = None
        del(self.__conn, self.data, self._v, self.date, self.close)

    def kama(self, data=None, period=k_period):
        if not data: data = self.data
        return self._v.kama(data, period=period)

    def kc(self, data=None, period=k_period):
        if not data: data = self.data
        return self._v.kc(data, period=period)

    def adx(self, data=None, period=x_period):
        if not data: data = self.data
        return self._v.adx(data, period=period)

    def mas(self, data=None, period=k_period):
        if not data: data = self.data
        return self._v.mas(data, period=period)

    def combine(self, code, freq='bi-daily'):
        if freq.lower() == 'bi-daily':
            res = {}
            for __ in [_['date'] for _ in self.__conn.execute("SELECT DISTINCT date FROM records WHERE code='{}' ORDER BY date ASC".format(code)).fetchall()]:
                _ = self.__conn.execute("SELECT session, open, high, low, close, volume FROM records WHERE code='{}' AND date='{}' ORDER BY session ASC".format(code, __)).fetchall()
                if _:
                    tmp = {'open': _[0]['open'], 'high': _[0]['high'], 'low': _[0]['low'], 'close': _[0]['close'], 'volume': _[0]['volume']}
                    if len(_) > 1:
                        for ___ in _:
                            if ___['session'] == 'a':
                                if ___['high'] > tmp['high']: tmp['high'] = ___['high']
                                if ___['low'] < tmp['low']: tmp['low'] = ___['low']
                                tmp['close'] = ___['close']
                                tmp['volume'] += ___['volume']
                res[datetime.strptime(__, '%Y-%m-%d').date()] = [tmp['open'], tmp['high'], tmp['low'], tmp['close'], tmp['volume']]
            hdr = {}
            hdr['Date'] = list(res.keys())
            hdr['Date'].sort()
            tp = []
            tp.extend([res[_] for _ in hdr['Date']])
            hdr['Data'] = np.array(tp)
            return hdr

class Equities(Viewer):
    def __init__(self, code, adhoc=False, db='Securities'):
        self.__conn = lite.connect(filepath(db))
        rc = entities(db).tolist()
        if code not in rc: adhoc = True
        self.data = self.fetch(code, adhoc=adhoc).to_dict()[code]
        self._v = Viewer(self.data)
        self.date = self.data['Date'][-1]
        self.close = self.data['Data'][-1, -2]

    def __del__(self):
        self.__conn = self.data = self._v = self.date = self.close = None
        del(self.__conn, self.data, self._v, self.date, self.close)

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

def rmi(el, adhoc=False):
    def __process(e):
        er, eo = e.ratr(), e.ovr()
        return er[(er > eo['min'].min()) & (er < eo['max'].max())].min()
    if isinstance(el, str) and el.upper() in entities('Futures').tolist():
        return __process(Futures(el.upper()))
    if isinstance(el, int):
        return __process(Equities(el, adhoc))
    res = []
    if isinstance(el, (list, tuple)):
        for _ in el:
            if isinstance (_, str):
                if _.upper() in entities('Futures').tolist():
                    res.append(__process(Futures(_.upper())))
                else: res.append(np.nan)
            elif isinstance(_, int):
                res.append(__process(Equities(_, adhoc)))
            else: res.append(np.nan)
        return res

def entities(db='Futures'):
    conn = lite.connect(filepath(db))
    conn.row_factory = lite.Row
    fin = 'code'
    if db == 'Securities': fin = 'eid'
    return pd.Series([_[fin] for _ in conn.execute(f"SELECT DISTINCT {fin} FROM records ORDER BY {fin} ASC").fetchall()])
