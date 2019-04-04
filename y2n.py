import pref
pd, np, db, yf, gr, datetime, sleep = pref.y2n
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
        self.data = self.combine(self._conf['freq'])
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

    def combine(self, freq='bi-daily'):
        return self._af.combine(freq)

    def mas(self, period=periods):
        return self.view.mas(self.data, period)

    def idrs(self, period=periods):
        return self.view.idrs(self.data, period)

    def sma(self, period=periods['simple'], req_field='c'):
        return self.view.ma(self.data, period, 's', req_field).map(hsirnd)

    def ema(self, period=periods['simple'], req_field='c'):
        return self.view.ma(self.data, period, 'e', req_field).map(hsirnd)

    def wma(self, period=periods['simple'], req_field='c'):
        return self.view.ma(self.data, period, 'w', req_field).map(hsirnd)

    def kama(self, period=periods['kama'], req_field='c'):
        return self.view.kama(self.data, period, req_field).map(hsirnd)

    def atr(self, period=periods['atr']):
        return self.view.atr(self.data, period)

    def rsi(self, period=periods['rsi']):
        return self.view.rsi(self.data, period)

    def obv(self):
        return self.view.obv(self.data)

    def ovr(self, period=periods):
        return self.view.ovr(self.data, period)

    def vwap(self):
        return self.view.vwap(self.data)

    def bb(self, period=periods['simple']):
        return self.view.bb(self.data, period)

    def apz(self, period=periods['apz']):
        return self.view.apz(self.data, period)

    def kc(self, period=periods['kc']):
        return self.view.kc(self.data, period)

    def macd(self, period=periods['macd']):
        return self.view.macd(self.data, period)

    def adx(self, period=periods['adx']):
        return self.view.adx(self.data, period)

    def soc(self, period=periods['soc']):
        return self.view.soc(self.data, period)

    def stc(self, period=periods['stc']):
        return self.view.stc(self.data, period)

    def ratr(self, period=periods['atr'], date=None):
        return self.view.ratr(self.data, period, date)

    def maverick(self, period=periods, date=None, unbound=True, exclusive=True):
        if date == None: date = self.data.index[-1]
        return self.view.maverick(self.data, period, date, unbound, exclusive)

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
        return f"#{self.code} close @ {self._close:.02f} ({delta}%) on {self._date:'%Y-%m-%d'}"

    def insert(self, values, conditions):
        return self._ae.append(values, conditions)

    def delete(self, conditions):
        return self._ae.remove(conditions)

    def update(self, values, conditions):
        return self._ae.amend(values, conditions)

    def acquire(self, conditions, ):
        return self._ae.acquire(conditions)

    def fetch(self, code=None, start=None, table=pref.db['Equities']['table'], exclude=pref.db['Equities']['exclude'], years=4, adhoc=False):
        if not start:
            start = pd.datetime(pd.datetime.now().year - years, 12, 31)
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
        return __

    def mas(self, period=periods):
        return self.view.mas(self.data, period)

    def idrs(self, period=periods):
        return self.view.idrs(self.data, period)

    def sma(self, period=periods['simple'], req_field='c'):
        return self.view.ma(self.data, period, 's', req_field).map(hsirnd)

    def ema(self, period=periods['simple'], req_field='c'):
        return self.view.ma(self.data, period, 'e', req_field).map(hsirnd)

    def wma(self, period=periods['simple'], req_field='c'):
        return self.view.ma(self.data, period, 'w', req_field).map(hsirnd)

    def kama(self, period=periods['kama'], req_field='c'):
        return self.view.kama(self.data, period, req_field).map(hsirnd)

    def atr(self, period=periods['atr']):
        return self.view.atr(self.data, period)

    def rsi(self, period=periods['rsi']):
        return self.view.rsi(self.data, period)

    def obv(self):
        return self.view.obv(self.data)

    def ovr(self, period=periods):
        return self.view.ovr(self.data, period)

    def vwap(self):
        return self.view.vwap(self.data)

    def bb(self, period=periods['simple']):
        return self.view.bb(self.data, period)

    def apz(self, period=periods['apz']):
        return self.view.apz(self.data, period)

    def kc(self, period=periods['kc']):
        return self.view.kc(self.data, period)

    def macd(self, period=periods['macd']):
        return self.view.macd(self.data, period)

    def adx(self, period=periods['adx']):
        return self.view.adx(self.data, period)

    def soc(self, period=periods['soc']):
        return self.view.soc(self.data, period)

    def stc(self, period=periods['stc']):
        return self.view.stc(self.data, period)

    def ratr(self, period=periods['atr'], date=None):
        return self.view.ratr(self.data, period, date)

    def maverick(self, period=periods, date=None, unbound=True, exclusive=True):
        if date == None: date = self.data.index[-1]
        return self.view.maverick(self.data, period, date, unbound, exclusive)

def bqo(el, action='buy', bound=True, adhoc=False):
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
    return _.T

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
    res = [__ for __ in [_[0] for _ in conn.execute(query).fetchall()] if __ not in pE['exclude']]
    if series: return pd.Series(res)
    return res

def compose(code=None):
    if code == None: code = entities(pref.db['Equities']['name'])
    tlist = []
    if isinstance(code, (int, float)): code = [int(code)]
    if isinstance(code, list):
        try: code = [int(_) for _ in code]
        except: pass
    for _ in code:
        e = Equities(_)
        rd = e.data
        pdhr = pd.concat([e.rsi(), rd.High.sub(rd.Low), rd.Close.diff(), e.atr(), e.adx()[f"ADX{pref.periods['Equities']['adx']}"].diff()], axis=1)
        pdhr.columns = ['RSI', 'dHL', 'dpC', 'ATR', 'dADX']
        tlist.append(pdhr)
    return pd.concat(tlist, keys=code, names=['Code','Data'], axis=1)

def listed(df, date, buy=True):
    txr = df.reorder_levels(['Data','Code'], 1)
    rtr = txr.loc[date, 'RSI']
    hdr = []
    if buy:
        rl = rtr[(rtr < (1 - 1 / gr) * 100) & (txr.loc[date, 'dpC'] > txr.loc[date, 'ATR'])].index.tolist()
        if rl:
            hdr.extend([Equities(_).maverick(date=date, unbound=False).loc["buy", date] for _ in rl])
            return pd.Series(hdr, index=rl, name='buy')
    rl = rtr[(rtr > 1 / gr * 100) & (txr.loc[date, 'dpC'] > txr.loc[date, 'ATR'])].index.tolist()
    if rl:
        hdr.extend([Equities(_).maverick(date=date, unbound=False).loc["sell", date] for _ in rl])
        return pd.Series(hdr, index=rl, name='sell')
