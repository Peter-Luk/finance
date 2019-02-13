import pref
pd, np, db, yf, datetime, sleep = pref.y2n
from utilities import filepath, mtf
from nta import Viewer, hsirnd
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
        self.data = self.combine(self._conf['freq'], True)
        self.view = Viewer(self.data)
        self._date = self.data.index[-1]
        self._close = self.data['Close'][-1]

    def __del__(self):
        self._conf = self.table = self.rc = self._af = self.__conn = self.code = self.data = self.view = self._date = self._close = None
        del(self._conf, self.table, self.rc, self._af, self.__conn, self.code, self.data, self.view, self._date, self._close)

    def __call__(self, date=None):
        if date == None: date = self._date
        _ = self.best_quote(date=date)
        _.name = self.code
        return _

    def __str__(self):
        delta = round(self.data['Close'].diff()[-1] / self.data['Close'][-2] * 100, 3)
        return f"{self.code} close @ {self._close} ({delta}%) on {self._date:'%Y-%m-%d'}"

    def insert(self, values, conditions):
        return self._af.append(values, conditions)

    def delete(self, conditions):
        return self._af.remove(conditions)

    def update(self, values, conditions):
        return self._af.amend(values, conditions)

    def combine(self, freq='bi-daily', dataframe=False):
        return self._af.combine(freq, dataframe)

    def mas(self, raw=None, period=periods, dataframe=True):
        if raw == None: raw = self.data
        return self.view.mas(raw, period, dataframe)

    def idrs(self, raw=None, period=periods, dataframe=True):
        if raw == None: raw = self.data
        return self.view.idrs(raw, period, dataframe)

    def macd(self, raw=None, period=periods['macd'], dataframe=True):
        if raw == None: raw = self.data
        return self.view.macd(raw, period, dataframe)

    def stc(self, raw=None, period=periods['stc'], dataframe=True):
        if raw == None: raw = self.data
        return self.view.stc(raw, period, dataframe)

    def best_quote(self, raw=None, period=periods, date=None, unbound=False, exclusive=True, dataframe=True):
        if raw == None: raw = self.data
        if date == None: date = raw.index[-1]
        return self.view.best_quote(raw, period, date, unbound, exclusive,  dataframe)

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
        _raw = self.fetch(code, adhoc=adhoc)[code]
        self.data = pd.DataFrame(_raw['Data'], index=_raw['Date'], columns = ['Open', 'High', 'Low', 'Close', 'Volume'])
        self.view = Viewer(self.data)
        self._date = self.data.index[-1]
        self._close = hsirnd(self.data['Close'][-1])

    def __del__(self):
        self._conf = self.rc = self._ae =self.__conn = self.data = self.view = self._date = self._close = None
        del(self._conf, self.rc, self._ae, self.__conn, self.data, self.view, self._date, self._close)

    def __call__(self, date=None):
        if date == None: date = self._date
        return self.best_quote(date=date)

    def __str__(self):
        delta = round(self.data['Close'].diff()[-1] / self.data['Close'][-2] * 100, 3)
        return f"{self.code} close @ {self._close} ({delta}%) on {self._date:'%Y-%m-%d'}"

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

    def mas(self, raw=None, period=periods, dataframe=True):
        if raw == None: raw = self.data
        return self.view.mas(raw, period, dataframe)

    def idrs(self, raw=None, period=periods, dataframe=True):
        if raw == None: raw = self.data
        return self.view.idrs(raw, period, dataframe)

    def macd(self, raw=None, period=periods['macd'], dataframe=True):
        if raw == None: raw = self.data
        return self.view.macd(raw, period, dataframe)

    def stc(self, raw=None, period=periods['stc'], dataframe=True):
        if raw == None: raw = self.data
        return self.view.stc(raw, period, dataframe)

    def best_quote(self, raw=None, period=periods, date=None, unbound=False, exclusive=True, dataframe=True):
        if raw == None: raw = self.data
        if date == None: date = raw.index[-1]
        return self.view.best_quote(raw, period, date, unbound, exclusive,  dataframe)

def bqo(el, action='buy', bound=True, adhoc=False, dataframe=True):
    dl, il = [], []
    if isinstance(el, (int, str)): el = [el]
    for _ in el:
        if isinstance(_, int):
            pE = pref.db['Equities']
            if _ in [__ for __ in entities(pE['name']) if __ not in pE['exclude']]: o_ = Equities(_, adhoc)
            # o_ = Equities(_, adhoc)
        if isinstance(_, str):
            pF = pref.db['Futures']
            if _.upper() in entities(pF['name']): o_ = Futures(_.upper())
        val = o_.best_quote(unbound=True).T[action][o_.date]
        if bound: val = o_().T[action][o_.date]
        dl.append(val)
        il.append(o_.code)
    _ = pd.DataFrame({action:dl}, index=il)
    if dataframe: return _.T
    return _.to_dict()

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
