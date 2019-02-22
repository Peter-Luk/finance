import pref
pd, np, db, yf, datetime, sleep = pref.y2n
from utilities import filepath, mtf
from nta import Viewer
from alchemy import AF, AE

class Futures(AF, Viewer):
    periods = pref.periods['Futures']
    def __init__(self, code):
        self._conf = pref.db['Futures']
        rc = entities(self._conf['name'])
        if code.upper() not in rc: code = rc[-1]
        self.code = code
        self._af = AF(self.code)
        self.table = self._af.table
        self.rc = self._af.columns
        self.__conn = self._af.connect
        self.data = self.combine(self._conf['freq'])
        self.view = Viewer(self.data)
        self.date = self.data['Date'][-1]
        self.close = self.data['Data'][-1, -2]

    def __del__(self):
        self._conf = self.table = self.rc = self._af = self.__conn = self.code = self.data = self.view = self.date = self.close = None
        del(self._conf, self.table, self.rc, self._af, self.__conn, self.code, self.data, self.view, self.date, self.close)

    def __call__(self, enhanced=True):
        if enhanced: return pd.DataFrame({'proposed':[self.best_quote(), self.best_quote('sell')]}, index=['buy','sell'])
        return self.close

    def insert(self, values, conditions):
        return self._af.append(values, conditions)

    def delete(self, conditions):
        return self._af.remove(conditions)

    def update(self, values, conditions):
        return self._af.amend(values, conditions)

    def combine(self, freq='bi-daily', dataframe=False):
        return self._af.combine(freq, dataframe)

    def ma(self, raw=None, period=periods['simple'], favour='s', req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self.view.ma(raw, period, favour, req_field, programmatic)

    def soc(self, raw=None, period=periods['soc'], dataframe=True):
        if not raw:
            if isinstance(self.data, dict):
                raw = self.data
                try: raw = self._ae()
                except: pass
        return self.view.soc(raw, period, dataframe)

    def macd(self, raw=None, period=periods['macd'], dataframe=True):
        if not raw: raw = self.data
        return self.view.macd(raw, period, dataframe)

    def kama(self, data=None, period=periods['kama']):
        if not data: data = self.data
        return self.view.kama(data, period)

    def bbw(self, raw=None, period=periods['simple'], req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self.view.bbw(raw, period, req_field, programmatic)

    def bb(self, raw=None, period=periods['simple'], req_field='c', programmatic=False):
        if not raw: raw = self.data
        return self.view.bb(raw, period, req_field, programmatic)

    def rsi(self, raw=None, period=periods['simple']):
        if not raw: raw = self.data
        return self.view.rsi(raw, period)

    def kc(self, data=None, period=periods):
        if not data: data = self.data
        return self.view.kc(data, period)

    def atr(self, raw=None, period=periods['atr'], programmatic=False):
        if not raw: raw = self.data
        return self.view.atr(raw, period, programmatic)

    def trp(self, data=None, period=periods['atr']):
        if not data: data = self.data
        return self.view.trp(data, period)

    def adx(self, data=None, period=periods['adx']):
        if not data: data = self.data
        return self.view.adx(data, period)

    def mas(self, period=periods):
        # if not data: data = self.data
        return self.view.mas(period)

    def idrs(self, period=periods):
        # if not data: data = self.data
        return self.view.idrs(period)

    def apz(self, raw=None, period=periods, df=None, programmatic=False):
        if not raw: raw = self.data
        return self.view.apz(raw, period, df, programmatic)

    def ovr(self, raw=None, period=periods,date=datetime.today().date()):
        if not raw: raw = self.data
        return self.view.ovr(raw, period, date)

    def ratr(self, raw=None, period=periods['atr']):
        if not raw: raw = self.data
        return self.view.ratr(raw, period)

class Equities(AE, Viewer):
    periods = pref.periods['Equities']
    def __init__(self, code, adhoc=False):
        self._conf = pref.db['Equities']
        if code not in entities(self._conf['name']): adhoc = True
        self.code = code
        self._ae = AE(self.code)
        self.table = self._ae.table
        self.rc = self._ae.columns
        self.__conn = self._ae.connect
        self.data = self.fetch(code, adhoc=adhoc)[code]
        self.view = Viewer(self.data)
        self.date = self.data['Date'][-1]
        self.close = self.data['Data'][-1, -2]

    def __del__(self):
        self._conf = self.rc = self._ae =self.__conn = self.data = self.view = self.date = self.close = None
        del(self._conf, self.rc, self._ae, self.__conn, self.data, self.view, self.date, self.close)

    def __call__(self, enhanced=True):
        if enhanced: return pd.DataFrame({'proposed':[self.best_quote(), self.best_quote('sell')]}, index=['buy','sell'])
        return self.close

    def insert(self, values, conditions):
        return self._ae.append(values, conditions)

    def delete(self, conditions):
        return self._ae.remove(conditions)
    def update(self, values, conditions):
        return self._ae.amend(values, conditions)

    def acquire(self, conditions, dataframe=True):
        return self._ae.acquire(conditions, dataframe)

    def fetch(self, code=None, start=None, table=pref.db['Equities']['table'], exclude=pref.db['Equities']['exclude'], years=4, adhoc=False, series=False):
        res = {}
        if not start:
            start = pd.datetime(pd.datetime.now().year - years, 1, 1)
        aid = [_ for _ in entities(self._conf['name']) if _ not in exclude]
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
                query = db.select([self.rc.date, self.rc.open, self.rc.high, self.rc.low, self.rc.close, self.rc.volume]).where(self.rc.eid==_)
                if start:
                    query = db.select([self.rc.date, self.rc.open, self.rc.high, self.rc.low, self.rc.close, self.rc.volume]).where(db.and_(self.rc.eid==_, self.rc.date > start))
                hdr = self.__conn.execute(query).fetchall()
                p_ = pd.DataFrame(hdr)
                p_.columns = hdr[0].keys()
                p_.set_index(self._conf['index'], inplace=True)
                pp_ = pd.DataFrame([p_['open'], p_['high'], p_['low'], p_['close'], p_['volume']]).T
                res[_] = {'Date': list(pp_.index), 'Data': pp_.values}
        if series: return pd.Series(res)
        return res

    def ma(self, raw=None, period=periods['simple'], favour='s', req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self.view.ma(raw, period, favour, req_field, programmatic)

    def macd(self, raw=None, period=periods['macd'], dataframe=True):
        if not raw: raw = self.data
        return self.view.macd(raw, period, dataframe)

    def soc(self, raw=None, period=periods['soc'], dataframe=True):
        if not raw:
            if isinstance(self.data, dict):
                raw = self.data
                try: raw = self._ae()
                except: pass
        return self.view.soc(raw, period, dataframe)

    def stc(self, raw=None, period=periods['stc'], dataframe=True):
        if not raw: raw = self.data
        return self.view.stc(raw, period, dataframe)

    def kama(self, data=None, period=periods['kama']):
        if not data: data = self.data
        return self.view.kama(data, period)

    def bbw(self, raw=None, period=periods['simple'], req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self.view.bbw(raw, period, req_field, programmatic)

    def bb(self, raw=None, period=periods['simple'], req_field='c', programmatic=False):
        if not raw: raw = self.data
        return self.view.bb(raw, period, req_field, programmatic)

    def rsi(self, raw=None, period=periods['simple']):
        if not raw: raw = self.data
        return self.view.rsi(raw, period)

    def kc(self, data=None, period=periods):
        if not data: data = self.data
        return self.view.kc(data, period=period)

    def atr(self, raw=None, period=periods['atr'], programmatic=False):
        if not raw: raw = self.data
        return self.view.atr(raw, period, programmatic)

    def trp(self, data=None, period=periods['atr']):
        if not data: data = self.data
        return self.view.trp(data, period)

    def adx(self, data=None, period=periods['adx']):
        if not data: data = self.data
        return self.view.adx(data, period)

    def mas(self, period=periods):
        return self.view.mas(period)

    def idrs(self, period=periods):
        return self.view.idrs(period)

    def apz(self, raw=None, period=periods, df=None, programmatic=False):
        if not raw: raw = self.data
        return self.view.apz(raw, period, df, programmatic)

    def ovr(self, raw=None, period=periods,date=datetime.today().date()):
        if not raw: raw = self.data
        return self.view.ovr(raw, period, date)

    def ratr(self, raw=None, period=periods['atr']):
        if not raw: raw = self.data
        return self.view.ratr(raw, period)

def bqo(el, action='buy', adhoc=False, bound=True):
    def __process(e, action, bound):
        er, eo = e.ratr(), e.ovr()
        _ = er[(er > eo['min'].min()) & (er < eo['max'].max())]
        if action == 'buy':
            if bound:
                if _.size > 0:
                    if e.close < _.min(): return pd.Series([__ for __ in er if __ < e.close])
                    return pd.Series([__ for __ in _ if __ < e.close])
                return np.nan
            return er.min()
        if action == 'sell':
            if bound:
                if _.size > 0:
                    if e.close > _.max(): return pd.Series([__ for __ in er if __ > e.close])
                    return pd.Series([__ for __ in _ if __ > e.close])
                return np.nan
            return er.max()

    pF = pref.db['Futures']
    if isinstance(el, str) and el.upper() in entities(pF['name']):
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
                    if _.upper() in entities(pF['name']):
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
                    if _.upper() in entities(pF['name']):
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

def entities(dbname=None, series=False):
    pF, pE = pref.db['Futures'], pref.db['Equities']
    if not dbname: dbname = pE['name']
    engine = db.create_engine(f'sqlite:///{filepath(dbname)}')
    conn = engine.connect()
    if dbname == pF['name']:
        rc = db.Table(pF['table'], db.MetaData(), autoload=True, autoload_with=engine).columns
        query = db.select([rc.code.distinct()]).order_by(db.asc(rc.code))
    if dbname == pE['name']:
        rc = db.Table(pE['table'], db.MetaData(), autoload=True, autoload_with=engine).columns
        query = db.select([rc.eid.distinct()]).order_by(db.asc(rc.eid))
    __ = [_[0] for _ in conn.execute(query).fetchall()]
    if series: return pd.Series(__)
    return __