import sqlite3 as lite
import sqlalchemy as db
import numpy as np
import pandas as pd
import fix_yahoo_finance as yf
from nta import Viewer, hsirnd
from utilities import filepath, datetime, mtf
from time import sleep
import pref

class Futures(Viewer):
    periods = pref.periods['Futures']
    def __init__(self, code):
        db_conf = pref.db['Futures']
        engine = db.create_engine(f"sqlite:///{filepath(db_conf['name'])}")
        self.rc = db.Table(db_conf['table'], db.MetaData(), autoload=True, autoload_with=engine).columns
        self.__ac = engine.connect()
        self.__conn = lite.connect(filepath(db_conf['name']))
        self.__conn.row_factory = lite.Row
        rc = entities(db_conf['name']).tolist()
        if code.upper() not in rc: code = rc[-1]
        self.data = self.combine(code, db_conf['freq'])
        self._v = Viewer(self.data)
        self.date = self.data['Date'][-1]
        self.close = self.data['Data'][-1, -2]

    def __del__(self):
        self.__conn = self.data = self._v = self.date = self.close = None
        del(self.__conn, self.data, self._v, self.date, self.close)

    def __call__(self, enhanced=True):
        if enhanced: return pd.DataFrame({'proposed':[self.best_quote(), self.best_quote('sell')]}, index=['buy','sell'])
        return self.close

    def ma(self, raw=None, period=periods['simple'], favour='s', req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self._v.ma(raw, period, favour, req_field, programmatic)

    def kama(self, data=None, period=periods):
        if not data: data = self.data
        return self._v.kama(data, period)

    def bbw(self, raw=None, period=periods['simple'], req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self._v.bbw(raw, period, req_field, programmatic)

    def bb(self, raw=None, period=periods['simple'], req_field='c', programmatic=False):
        if not raw: raw = self.data
        return self._v.bb(raw, period, req_field, programmatic)

    def rsi(self, raw=None, period=periods['simple']):
        if not raw: raw = self.data
        return self._v.rsi(raw, period)

    def kc(self, data=None, period=periods):
        if not data: data = self.data
        return self._v.kc(data, period)

    def atr(self, raw=None, period=periods['atr'], programmatic=False):
        if not raw: raw = self.data
        return self._v.atr(raw, period, programmatic)

    def trp(self, data=None, period=periods['atr']):
        if not data: data = self.data
        return self._v.trp(data, period)

    def adx(self, data=None, period=periods['adx']):
        if not data: data = self.data
        return self._v.adx(data, period)

    def mas(self, data=None, period=periods):
        if not data: data = self.data
        return self._v.mas(data, period)

    def idrs(self, data=None, period=periods):
        if not data: data = self.data
        return self._v.idrs(data, period)

    def apz(self, raw=None, period=periods, df=None, programmatic=False):
        if not raw: raw = self.data
        return self._v.apz(raw, period, df, programmatic)

    def ovr(self, raw=None, period=periods,date=datetime.today().date()):
        if not raw: raw = self.data
        return self._v.ovr(raw, period, date)

    def ratr(self, raw=None, period=periods['atr']):
        if not raw: raw = self.data
        return self._v.ratr(raw, period)

    def alternate_combine(self, code, freq):
        if freq.lower() == 'bi-daily':
            res = []
            for _ in [__[0] for __ in self.__ac.execute(db.select([self.rc.date.distinct()]).where(self.rc.code==code).order_by(db.asc(self.rc.date))).fetchall()]:
                tmp = {}
                __ = self.__ac.execute(db.select([self.rc.session, self.rc.open, self.rc.high, self.rc.low, self.rc.close, self.rc.volume]).where(db.and_(self.rc.code==code, self.rc.date==_)).order_by(db.desc(self.rc.session))).fetchall()
                p_ = pd.DataFrame(__)
                p_.columns = __[0].keys()
                p_.set_index('session', inplace=True)
                if len(p_) == 1:
                    try:
                        tmp['open'] = p_['open']['M']
                        tmp['high'] = p_['high']['M']
                        tmp['low'] = p_['low']['M']
                        tmp['close'] = p_['close']['M']
                        tmp['volume'] = p_['volume']['M']
                    except:
                        tmp['open'] = p_['open']['A']
                        tmp['high'] = p_['high']['A']
                        tmp['low'] = p_['low']['A']
                        tmp['close'] = p_['close']['A']
                        tmp['volume'] = p_['volume']['A']
                elif len(p_) == 2:
                    tmp['open'] = p_['open']['M']
                    tmp['high'] = p_['high'].max()
                    tmp['low'] = p_['low'].min()
                    tmp['close'] = p_['close']['A']
                    tmp['volume'] = p_['volume'].sum()
                tmp['date'] = _
                res.append(tmp)
            _ = pd.DataFrame(res)
            _.set_index(pref.db['Futures']['index'], inplace=True)
            return pd.DataFrame([_['open'], _['high'], _['low'], _['close'], _['volume']]).T

    def combine(self, code, freq):
        if freq.lower() == 'bi-daily':
            res = {}
            for __ in [_['date'] for _ in self.__conn.execute(f"SELECT DISTINCT date FROM records WHERE code='{code}' ORDER BY date ASC").fetchall()]:
                _ = self.__conn.execute(f"SELECT session, open, high, low, close, volume FROM records WHERE code='{code}' AND date='{__}' ORDER BY session DESC").fetchall()
                if _:
                    tmp = {'open': _[0]['open'], 'high': _[0]['high'], 'low': _[0]['low'], 'close': _[0]['close'], 'volume': _[0]['volume']}
                    if len(_) > 1:
                        if _[-1]['session'] == 'A':
                            if tmp['high'] < _[-1]['high']: tmp['high'] = _[-1]['high']
                            if tmp['low'] > _[-1]['low']: tmp['low'] = _[-1]['low']
                            tmp['close'] = _[-1]['close']
                            tmp['volume'] += _[-1]['volume']
                res[datetime.strptime(__, '%Y-%m-%d').date()] = [tmp['open'], tmp['high'], tmp['low'], tmp['close'], tmp['volume']]
            hdr = {}
            hdr['Date'] = list(res.keys())
            hdr['Date'].sort()
            tp = []
            tp.extend([res[_] for _ in hdr['Date']])
            hdr['Data'] = np.array(tp)
            return hdr

class Equities(Viewer):
    periods = pref.periods['Equities']
    def __init__(self, code, adhoc=False):
        db_conf = pref.db['Equities']
        self.__conn = lite.connect(filepath(db_conf['name']))
        rc = entities(db_conf['name']).tolist()
        if code not in rc: adhoc = True
        self.data = self.fetch(code, adhoc=adhoc).to_dict()[code]
        self._v = Viewer(self.data)
        self.date = self.data['Date'][-1]
        self.close = self.data['Data'][-1, -2]

    def __del__(self):
        self.__conn = self.data = self._v = self.date = self.close = None
        del(self.__conn, self.data, self._v, self.date, self.close)

    def __call__(self, enhanced=True):
        if enhanced: return pd.DataFrame({'proposed':[self.best_quote(), self.best_quote('sell')]}, index=['buy','sell'])
        return self.close

    def fetch(self, code=None, start=None, table='records', exclude=[805], years=4, adhoc=False):
        res = {}
        def stored_eid(table):
            cur = self.__conn.cursor()
            return [_ for _ in [__[0] for __ in cur.execute(f"SELECT DISTINCT eid FROM {table} ORDER BY eid ASC").fetchall()] if _ not in exclude]

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
                ar = yf.download([f'{_:04d}.HK' for _ in aid], start, group_by='ticker')
                if len(ar): adhoc = not adhoc
                else:
                    print('Retry in 30 seconds')
                    sleep(30)
            for _ in aid:
                hdr = {}
                rd = ar[f'{_:04d}.HK']
                d = rd.T
                hdr['Date'] = [__.to_pydatetime() for __ in rd.index]
                hdr['Data'] = np.asarray(pd.concat([rd[__] for __ in [___ for ___ in d.index if ___ not in ['Adj Close']]], axis=1, join='inner', ignore_index=False))
                res[_] = hdr
        else:
            for _ in aid:
                hdr, qstr = {}, f"SELECT date, open, high, low, close, volume FROM {table} WHERE eid={_:d}"
                if start: qstr += f" AND date > '{start:%Y-%m-%d}'"
                q = self.__conn.execute(qstr)
                cols = [c[0].capitalize() for c in q.description]
                rd = pd.DataFrame.from_records(data=q.fetchall(), index='Date', columns=cols)
                hdr['Date'] = [pd.datetime.strptime(__, '%Y-%m-%d') for __ in rd.index]
                hdr['Data'] = np.asarray(rd)
                res[_] = hdr
        return pd.Series(res)

    def ma(self, raw=None, period=periods['simple'], favour='s', req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self._v.ma(raw, period, favour, req_field, programmatic)

    def kama(self, data=None, period=periods):
        if not data: data = self.data
        return self._v.kama(data, period)

    def bbw(self, raw=None, period=periods['simple'], req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self._v.bbw(raw, period, req_field, programmatic)

    def bb(self, raw=None, period=periods['simple'], req_field='c', programmatic=False):
        if not raw: raw = self.data
        return self._v.bb(raw, period, req_field, programmatic)

    def rsi(self, raw=None, period=periods['simple']):
        if not raw: raw = self.data
        return self._v.rsi(raw, period)

    def kc(self, data=None, period=periods):
        if not data: data = self.data
        return self._v.kc(data, period=period)

    def atr(self, raw=None, period=periods['atr'], programmatic=False):
        if not raw: raw = self.data
        return self._v.atr(raw, period, programmatic)

    def trp(self, data=None, period=periods['atr']):
        if not data: data = self.data
        return self._v.trp(data, period)

    def adx(self, data=None, period=periods['adx']):
        if not data: data = self.data
        return self._v.adx(data, period)

    def mas(self, data=None, period=periods):
        if not data: data = self.data
        return self._v.mas(data, period)

    def idrs(self, data=None, period=periods):
        if not data: data = self.data
        return self._v.idrs(data, period=period)

    def apz(self, raw=None, period=periods, df=None, programmatic=False):
        if not raw: raw = self.data
        return self._v.apz(raw, period, df, programmatic)

    def ovr(self, raw=None, period=periods,date=datetime.today().date()):
        if not raw: raw = self.data
        return self._v.ovr(raw, period, date)

    def ratr(self, raw=None, period=periods['atr']):
        if not raw: raw = self.data
        return self._v.ratr(raw, period)

def bqo(el, action='buy', adhoc=False, bound=True):
    def __process(e, action, bound):
        er, eo = e.ratr(), e.ovr()
        _ = er[(er > eo['min'].min()) & (er < eo['max'].max())]
        if action == 'buy':
            if bound:
                if e.close < _.min(): return pd.Series([__ for __ in er if __ < _.min()])
                return np.nan
            return er.min()
        if action == 'sell':
            if bound:
                if e.close > _.max(): return pd.Series([__ for __ in er if __ > _.max()])
                return np.nan
            return er.max()

    if isinstance(el, str) and el.upper() in entities('Futures').tolist():
        hdr = __process(Futures(el.upper()), action, bound)
        if action == 'buy':
            if isinstance(hdr, (float, int)): return hdr
            try: return hdr.head()
            except: return hdr
        if action == 'sell':
            if isinstance(hdr, (float, int)): return hdr
            try: return hdr.tail()
            except: return hdr

    if isinstance(el, int):
        hdr = __process(Equities(el, adhoc), action, bound)
        if action == 'buy':
            if isinstance(hdr, (float, int)): return hdr
            try: return hdr.head()
            except: return hdr
        if action == 'sell':
            if isinstance(hdr, (float, int)): return hdr
            try: return hdr.tail()
            except: return hdr

    res = {}
    if isinstance(el, (list, tuple)):
        if bound:
            for _ in el:
                hdr = np.nan
                if isinstance (_, str):
                    if _.upper() in entities('Futures').tolist():
                        hdr = __process(Futures(_.upper()), action, bound)
                elif isinstance(_, int):
                    hdr = __process(Equities(_, adhoc), action, bound)
                try: res[_] = hdr.head()
                except: pass
                if action == 'sell':
                    try: res[_] = hdr.tail()
                    except: pass
            hdr = pd.DataFrame(res)
        else:
            il, __ = [], []
            for _ in el:
                if isinstance (_, str):
                    if _.upper() in entities('Futures').tolist():
                        __.append(__process(Futures(_.upper()), action, bound))
                        il.append(_)
                elif isinstance(_, int):
                    try:
                        __.append(__process(Equities(_, adhoc), action, bound))
                        il.append(_)
                    except: pass
            hdr = pd.DataFrame({action:__}, index=il)

        if hdr.empty: return None
        return hdr

def entities(db=None):
    if not db: db = pref.db['Future']['name']
    conn = lite.connect(filepath(db))
    conn.row_factory = lite.Row
    fin = 'code'
    if db == pref.db['Equities']['name']: fin = 'eid'
    return pd.Series([_[fin] for _ in conn.execute(f"SELECT DISTINCT {fin} FROM records ORDER BY {fin} ASC").fetchall()])


def aef(eid, start=datetime(2014,12,31)):
    import sqlalchemy as db
    db_conf = pref.db['Equities']
    engine = db.create_engine(f"sqlite:///{filepath(db_conf['name'])}")
    meta = db.MetaData()
    conn = engine.connect()
    rc = db.Table(db_conf['table'], meta, autoload=True, autoload_with=engine).columns
    query = db.select([rc.date, rc.open, rc.high, rc.low, rc.close, rc.volume]).where(db.and_(rc.eid==eid, rc.date>start))
    res = conn.execute(query).fetchall()
    df = pd.DataFrame(res)
    df.columns = res[0].keys()
    df.set_index(db_conf['index'], inplace=True)
    return df
