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
        _ = self.maverick(date=date, unbound=False)
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
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.mas(raw, period, dataframe)

    def idrs(self, raw=None, period=periods, dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.idrs(raw, period, dataframe)

    def macd(self, raw=None, period=periods['macd'], dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.macd(raw, period, dataframe)

    def adx(self, raw=None, period=periods['adx'], dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.adx(raw, period, dataframe)

    def soc(self, raw=None, period=periods['soc'], dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.soc(raw, period, dataframe)

    def stc(self, raw=None, period=periods['stc'], dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.stc(raw, period, dataframe)

    def ratr(self, raw=None, period=periods['rsi'], dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.ratr(raw, period, dataframe)

    def maverick(self, raw=None, period=periods, date=None, unbound=True, exclusive=True, dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        if date == None: date = raw.index[-1]
        return self.view.maverick(raw, period, date, unbound, exclusive, dataframe)

class Equities(AE, Viewer):
    periods = pref.periods['Equities']
    def __init__(self, code, adhoc=False):
        self._conf = pref.db['Equities']
        if code not in entities(self._conf['name']): adhoc = True
        self.code = code
        self.data = self.fetch(self.code, adhoc=adhoc)
        if adhoc == False:
            self._ae = AE(self.code)
            self.table = self._ae.table
            self.rc = self._ae.columns
            self.__conn = self._ae.connect
        self.view = Viewer(self.data)
        self._date = self.data.index[-1]
        self._close = hsirnd(self.data['Close'][-1])

    def __del__(self):
        self._conf = self.rc = self._ae =self.__conn = self.data = self.view = self._date = self._close = None
        del(self._conf, self.rc, self._ae, self.__conn, self.data, self.view, self._date, self._close)

    def __call__(self, date=None):
        if date == None: date = self._date
        return self.maverick(date=date, unbound=False)

    def __str__(self):
        delta = round(self.data['Close'].diff()[-1] / self.data['Close'][-2] * 100, 3)
        return f"#{self.code} close @ {self._close} ({delta}%) on {self._date:'%Y-%m-%d'}"

    def insert(self, values, conditions):
        return self._ae.append(values, conditions)

    def delete(self, conditions):
        return self._ae.remove(conditions)

    def update(self, values, conditions):
        return self._ae.amend(values, conditions)

    def acquire(self, conditions, dataframe=True):
        return self._ae.acquire(conditions, dataframe)

    def fetch(self, code=None, start=None, table=pref.db['Equities']['table'], exclude=pref.db['Equities']['exclude'], years=4, adhoc=False, dataframe=True):
        if not start:
            start = pd.datetime(pd.datetime.now().year - years, 1, 1)
        if code:
            if isinstance(code, (int, float)): code = int(code)
            if code not in [_ for _ in entities(self._conf['name']) if _ not in exclude]: adhoc = True
        if adhoc:
            while adhoc:
                __ = yf.download(f'{code:04d}.HK', start, group_by='ticker')
                if len(__): adhoc = not adhoc
                else:
                    print('Retry in 30 seconds')
                    sleep(30)
            __.drop('Adj Close', 1, inplace=True)
        else:
            from alchemy import AS
            qtext = f"SELECT date, open, high, low, close, volume FROM records WHERE eid={code:d} AND date>{start:'%Y-%m-%d'}"
            conn = AS(self._conf['name']).connect
            __ = pd.read_sql(qtext, conn, index_col='date', parse_dates=['date'])
            __.columns = [_.capitalize() for _ in __.columns]
            __.index.name = __.index.name.capitalize()
        if dataframe: return __
        return {'Date':list(__.index), 'Data':__.values}

    def mas(self, raw=None, period=periods, dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.mas(raw, period, dataframe)

    def idrs(self, raw=None, period=periods, dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.idrs(raw, period, dataframe)

    def adx(self, raw=None, period=periods['adx'], dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.adx(raw, period, dataframe)

    def macd(self, raw=None, period=periods['macd'], dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.macd(raw, period, dataframe)

    def soc(self, raw=None, period=periods['soc'], dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.soc(raw, period, dataframe)

    def stc(self, raw=None, period=periods['stc'], dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.stc(raw, period, dataframe)

    def ratr(self, raw=None, period=periods['rsi'], dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        return self.view.ratr(raw, period, dataframe)

    def maverick(self, raw=None, period=periods, date=None, unbound=True, exclusive=True, dataframe=True):
        try:
            if raw == None: raw = self.data
        except: pass
        if date == None: date = raw.index[-1]
        return self.view.maverick(raw, period, date, unbound, exclusive, dataframe)

def bqo(el, action='buy', bound=True, adhoc=False, dataframe=True):
    dl, il = [], []
    if isinstance(el, (int, str)): el = [el]
    for _ in el:
        if isinstance(_, int):
            pE = pref.db['Equities']
            if _ in [__ for __ in entities(pE['name']) if __ not in pE['exclude']]: o_ = Equities(_, adhoc)
        if isinstance(_, str):
            pF = pref.db['Futures']
            if _.upper() in entities(pF['name']): o_ = Futures(_.upper())
        val = o_.maverick(unbound=True).T[action][o_._date]
        if bound: val = o_().T[action][o_._date]
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
