import copy
import datetime
# from typing import Optional
# from pydantic import BaseModel
from fastapi import FastAPI
from sqlalchemy.orm import sessionmaker, declarative_base, deferred, defer
from sqlalchemy import create_engine, Column, Integer, Date, String, text
from utilities import filepath, getcode, gslice, waf
from fintools import FOA, get_periods, pd, np, gap, prefer_stock
from finaux import roundup

Session = sessionmaker()
Base = declarative_base()
app = FastAPI()
idx = []


class Record(Base):
    __tablename__ = 'records'
    id = Column(Integer, autoincrement=True, primary_key=True)
    code = Column(String)
    _create_at = datetime.datetime.now()
    date = Column(Date, default=_create_at.date(),
            server_default=text(str(_create_at.date())))
    if _create_at.time() < datetime.time(12, 25, 0):
        session = deferred(Column(String, default='M', server_default=text('M')))
    else:
        session = deferred(Column(String, default='A', server_default=text('A')))
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Integer)

    def __repr__(self):
        _ = f"<Record(code='{self.code}', "
        _ += f"date='{self.date}', session='{self.session}', "
        _ += f"open={self.open}, high={self.high}, low={self.low}, "
        _ += f"close={self.close}, volume={self.volume})>"
        return _

    def range(self, v1, v2):
        (v1, v2) = (v2, v1) if v1 < v2 else (v1, v2)
        self.high = v1 if self.high is None or self.high < v1 else self.high
        self.low = v2 if self.low is None or self.low > v2 else self.low
        # h = v1 if v1 > v2 else v2
        # l = v2 if v2 < v1 else v1
        # self.high = h if self.high is None or self.high < h else self.high
        # self.low = l if self.low is None or self.low > l else self.low
        self.close = None

    def finish(self, close, volume):
        self.close = close
        self.volume = volume


class Index(Record):
    def __init__(self, db='Futures'):
        super(Index, self).__init__()
        self.engine = create_engine(f"sqlite:///{filepath(db)}")
        Session.configure(bind=self.engine)
        self.session = Session()
        self.query = self.session.query(Record)


class Securities(Record):
    def __init__(self, db='Securities'):
        self.__engine = create_engine(f"sqlite:///{filepath(db)}")
        Session.configure(bind=self.__engine)
        self.session = Session()
        query = self.session.query(Record.code.label('code')).subquery()
        self.query = self.session.query(query.c.code.label('eid')).options(defer('session'))


class Futures(Index, FOA):
    def __init__(self, code):
        super(Futures, self).__init__()
        self.__db = 'Futures'
        self.session = Index(self.__db).session
        self.periods = get_periods(self.__db)
        self.code = code.upper()
        self.query = self.session.query(Record).filter(Record.code==self.code)
        self.__data = self.compose()
        self.analyser = FOA(self.__data, int)
        self.change = 0
        if self.__data.shape[0] > 1:
            self.change = self.__data.close.diff(1)[-1] / self.__data.close[-2]
        self.date = self.__data.index[-1].to_pydatetime()
        __ = self.__data.iloc[-1]
        for _ in self.__data.columns:
            exec(f"self.{_}=__.{_}")

    def __call__(self):
        return self.compose()

    def copy(self):
        return copy.copy(self)

    def compose(self):
        def unique_date():
            res = []
            for _ in self.query.all():
                if _.date not in res:
                    res.append(_.date)
            return res
        res = []
        for _ in unique_date():
            try:
                md = self.query.filter(Record.date==_).filter(Record.session=='M').one()
                open = md.open
                high = md.high
                low = md.low
                close = md.close
                volume = md.volume
                try:
                    ad = self.query.filter(Record.date==_).filter(Record.session=='A').one()
                    if ad.high > high:
                        high = ad.high
                    if ad.low < low:
                        low = ad.low
                    close = ad.close
                    volume += ad.volume
                except Exception:
                    pass
            except:
                try:
                    ad = self.query.filter(Record.date==_).filter(Record.session=='A').one()
                    open = ad.open
                    high = ad.high
                    low = ad.low
                    close = ad.close
                    volume = ad.volume
                except Exception:
                    pass
            res.append({'date':_, 'open':open,'high':high, 'low':low, 'close':close, 'volume':volume})
        _ = pd.DataFrame(res)
        _ = _.convert_dtypes()
        _[[col for col in _.columns if _[col].dtypes == object]] = _[[col for col in _.columns if _[col].dtypes == object]].astype('string')
        _.date = pd.to_datetime(_.date, format='%Y-%m-%d')
        _ = _.set_index(pd.DatetimeIndex(_.date))
        _.drop('date', axis=1, inplace=True)
        return _

    def __str__(self):
        return f"{self.date:%d-%m-%Y}: close @ {self.close:d} ({self.change:0.3%}), rsi: {self.rsi().iloc[-1]:0.3f} and KAMA is {int(self.kama().iloc[-1]):d}"

    def sma(self, period=None):
        if period is None:
            period = self.periods['simple']
        _ = self.analyser.sma(period)
        return _.round()

    def wma(self, period=None):
        if period is None:
            period = self.periods['simple']
        _ = self.analyser.wma(period)
        return _.round()

    def ema(self, period=None):
        if period is None:
            period = self.periods['simple']
        _ = self.analyser.ema(period)
        return _.round()

    def macd(self, period=None):
        if period is None:
            period = self.periods['macd']
        return self.analyser.macd(period)

    def rsi(self, period=None):
        if period is None:
            period = self.periods['rsi']
        return self.analyser.rsi(period)

    def atr(self, period=None):
        if period is None:
            period = self.periods['atr']
        return self.analyser.atr(period)

    def kama(self, period=None):
        if period is None:
            period = self.periods['kama']
        _ = self.analyser.kama(period)
        return _.round()

    def soc(self, period=None):
        if period is None:
            period = self.periods['soc']
        return self.analyser.soc(period)

    def stc(self, period=None):
        if period is None:
            period = self.periods['stc']
        return self.analyser.stc(period)

    def adx(self, period=None):
        if period is None:
            period = self.periods['adx']
        return self.analyser.adx(period)

    def kc(self, period=None):
        if period is None:
            period = self.periods['kc']
        _ = self.analyser.kc(period)
        return _.applymap(round, na_action='ignore')

    def apz(self, period=None):
        if period is None:
            period = self.periods['apz']
        _ = self.analyser.apz(period)
        return _.applymap(round, na_action='ignore')

    def dc(self, period=None):
        if period is None:
            period = self.periods['dc']
        return self.analyser.dc(period)

    def obv(self):
        return self.analyser.obv()

    def vwap(self):
        return self.analyser.vwap()

    def bb(self, period=None):
        if period is None:
            period = self.periods['simple']
        _ = self.analyser.bb(period)
        return _.applymap(round, na_action='ignore')

    def mas(self, period=None):
        if period is None:
            period = {'simple':self.periods['simple'], 'kama':self.periods['kama']}
        sma = self.sma(period['simple'])
        wma = self.wma(period['simple'])
        ema = self.ema(period['simple'])
        kama = self.kama(period['kama'])
        __ = pd.concat([sma, wma, ema, kama], axis=1)
        __['high'] = __.max(axis=1)
        __['low'] = __.min(axis=1)
        __.drop(['sma', 'wma', 'ema', 'kama'], axis=1, inplace=True)
        return __.applymap(round, na_action='ignore')

    def optinum(self, date=None, base=None, delta=None):
        if date is None:
            date = self.date
        if base is None:
            base = self.__data
        if delta is None:
            delta = self.atr(self.periods['atr'])

        def _patr(raw, date):
            lc, lr = raw.close.loc[date], delta.loc[date]
            _ = np.arange(lc - lr, lc + lr, lr).tolist()
            _.extend(gslice([lc + lr, lc]))
            _.extend(gslice([lc, lc - lr]))
            return _

        def _pgap(pivot, raw):
            gap = pivot - raw.close.iloc[-1]
            _ = gslice([pivot + gap, pivot])
            _.extend(gslice([pivot, pivot - gap]))
            return _
        hdr = []
        [hdr.extend(_pgap(_, base)) for _ in _patr(base, date)]
        hdr.sort()

        buy = [round(_) for _ in hdr if _ < base.close.loc[date]]
        sell = [round(_) for _ in hdr if _ > base.close.loc[date]]
        return {'Buy':pd.Series(buy).unique(), 'Sell':pd.Series(sell).unique()}


class Equity(Securities, FOA):
    def __init__(self, code, static=True, exchange='HKEx'):
        s = copy.copy(Securities)
        self.session = s('Securities').session
        # self.periods = get_periods('pref.yaml')['Equities']
        self.periods = get_periods()

        self.code = code
        self.exchange = exchange
        self.yahoo_code = getcode(self.code, self.exchange)
        self.__fields = ['date', 'open', 'high', 'low', 'close', 'volume']
        if exchange == 'HKEx':
            self.query = self.session.query(*[eval(f"Record.{_}") for _ in self.__fields]).filter(text(f"eid={self.code}"))
        self.__data = self.compose(static)
        self.analyser = FOA(self.__data, float)
        self.change = self.__data.close.diff(1)[-1] / self.__data.close[-2]
        self.date = self.__data.index[-1].to_pydatetime()
        __ = self.__data.iloc[-1]
        for _ in self.__data.columns:
            exec(f"self.{_}=__.{_}")
            if self.exchange == 'HKEx':
                exec(f"self.{_}=roundup(__.{_})")

    def __call__(self, static=True):
        return self.__data

    def copy(self):
        return copy.copy(self)

    def compose(self, static):
        if self.exchange != 'HKEx':
            static = False
        if static:
            __ = pd.read_sql(self.query.statement, self.session.bind, parse_dates=['date'])
            __ = __.set_index(pd.DatetimeIndex(__.date))
            __.drop('date', axis=1, inplace=True)
        else:
            import yfinance as yf
            today = datetime.datetime.today()
            start = today.replace(2000, 1, 1)
            __ = yf.download(self.yahoo_code, start, today, group_by='ticker')
            __.drop('Adj Close', axis=1, inplace=True)
            __.columns = [_.lower() for _ in __.columns]
            __.index.name = __.index.name.lower()
        return __

    def __str__(self):
        return f"{self.date:%d-%m-%Y}: close @ {self.close:,.2f} ({self.change:0.3%}), rsi: {self.rsi().iloc[-1]:0.3f}, sar: {self.sar().iloc[-1]:,.2f} and KAMA: {self.kama().iloc[-1]:,.2f}"

    def sma(self, period=None):
        if period is None:
            period = self.periods['simple']
        _ = self.analyser.sma(period).astype('float64')
        if self.exchange == 'HKEx':
            _ = _.apply(roundup)
        return _

    def sar(self, period=None):
        if period is None:
            period = self.periods['sar']
        _ = self.analyser.sar(period['acceleration'], period['maximum']).astype('float64')
        if self.exchange == 'HKEx':
            _ = _.apply(roundup)
        return _

    def wma(self, period=None):
        if period is None:
            period = self.periods['simple']
        _ = self.analyser.wma(period).astype('float64')
        if self.exchange == 'HKEx':
            _ = _.apply(roundup)
        return _

    def ema(self, period=None):
        if period is None:
            period = self.periods['simple']
        _ = self.analyser.ema(period).astype('float64')
        if self.exchange == 'HKEx':
            _ = _.apply(roundup)
        return _

    def macd(self, period=None):
        if period is None:
            period = self.periods['macd']
        return self.analyser.macd(period)

    def rsi(self, period=None):
        if period is None:
            period = self.periods['rsi']
        return self.analyser.rsi(period)

    def atr(self, period=None):
        if period is None:
            period = self.periods['atr']
        return self.analyser.atr(period)

    def kama(self, period=None):
        if period is None:
            period = self.periods['kama']
        _ = self.analyser.kama(period).astype('float64')
        if self.exchange == 'HKEx':
            _ = _.apply(roundup)
        return _

    def soc(self, period=None):
        if period is None:
            period = self.periods['soc']
        return self.analyser.soc(period)

    def stc(self, period=None):
        if period is None:
            period = self.periods['stc']
        return self.analyser.stc(period)

    def adx(self, period=None):
        if period is None:
            period = self.periods['adx']
        return self.analyser.adx(period)

    def kc(self, period=None):
        if period is None:
            period = self.periods['kc']
        _ = self.analyser.kc(period)
        return _roundup(_, self.exchange)

    def apz(self, period=None):
        if period is None:
            period = self.periods['apz']
        _ = self.analyser.apz(period)
        return _roundup(_, self.exchange)

    def dc(self, period=None):
        if period is None:
            period = self.periods['dc']
        _ = self.analyser.dc(period)
        return _roundup(_, self.exchange)

    def obv(self):
        return self.analyser.obv()

    def vwap(self):
        return self.analyser.vwap()

    def bb(self, period=None):
        if period is None:
            period = self.periods['simple']
        _ = self.analyser.bb(period)
        return _roundup(_, self.exchange)
        # return _.applymap(roundup, na_action='ignore')

    def mas(self, period=None):
        if period is None:
            period = {'simple':self.periods['simple'], 'kama':self.periods['kama']}
        sma = self.sma(period['simple'])
        wma = self.wma(period['simple'])
        ema = self.ema(period['simple'])
        kama = self.kama(period['kama'])
        __ = pd.concat([sma, wma, ema, kama], axis=1)
        __['Upper'] = __.max(axis=1)
        __['Lower'] = __.min(axis=1)
        __.drop(['sma', 'wma', 'ema', 'kama'], axis=1, inplace=True)
        if self.exchange == 'HKEx':
            __.Upper = __.Upper.apply(roundup)
            __.Lower = __.Lower.apply(roundup)
        return _roundup(__, self.exchange)

    def optinum(self, date=None, base=None, delta=None):
        if date is None:
            date = self.date
        if base is None:
            base = self.__data
        if delta is None:
            delta = self.atr(self.periods['atr'])

        def _patr(raw, date):
            lc, lr = raw.close.loc[date], delta.loc[date]
            _ = np.arange(lc - lr, lc + lr, lr).tolist()
            _.extend(gslice([lc + lr, lc]))
            _.extend(gslice([lc, lc - lr]))
            return _

        def _pgap(pivot, raw):
            gap = pivot - raw.close.iloc[-1]
            _ = gslice([pivot + gap, pivot])
            _.extend(gslice([pivot, pivot - gap]))
            return _
        hdr = []
        [hdr.extend(_pgap(_, base)) for _ in _patr(base, date)]
        hdr.sort()

        buy = [round(_, 2) for _ in hdr if _ < base.close.loc[date]]
        sell = [round(_, 2) for _ in hdr if _ > base.close.loc[date]]
        if self.exchange == 'TSE':
            buy = [round(_) for _ in hdr if _ < base.close.loc[date]]
            sell = [round(_) for _ in hdr if _ > base.close.loc[date]]
        if self.exchange == 'HKEx':
            buy = [roundup(_) for _ in hdr if _ < base.close.loc[date]]
            sell = [roundup(_) for _ in hdr if _ > base.close.loc[date]]

        return {'Buy':pd.Series(buy).unique(), 'Sell':pd.Series(sell).unique()}

    def range(self, period=None):
        if period is None:
            period = {'rsi':self.periods['rsi'], 'kama':self.periods['kama']}
        tk = self.kama(period['kama']).iloc[-1]
        tr = self.atr(period['rsi']).iloc[-1]
        upper = gap([tk + tr, tk])
        lower = gap([tk - tr, tk])
        _l = upper
        _l.extend(lower)
        _a = np.array(_l)
        return pd.Series(np.unique(_a))


def submit(values, db='Futures'):
    from tqdm import tqdm
    _ = Index(db).session
    with _.begin():
        for i in tqdm(values, desc='submitting'):
            _.add(i)
    print('Done')
    _.close()


def collect(futures=waf()):
    values = []
    for _ in futures:
        s, v = 'M', 0
        if datetime.datetime.now().time() > datetime.time(12, 4):
            s = 'A'
            try:
                __ = Futures(_)().iloc[-1]
                if __.name.date() == datetime.datetime.today().date():
                    v -= __.volume
            except Exception:
                pass
        values.append(Record(code=_, date=datetime.datetime.today().date(), session=s, volume=v))
    return zip(futures, values)


def baseplot(rdf, latest=None):
    if isinstance(rdf, (Index, Equity)):
        df = rdf.copy()
        _ = df.kc()
        _['kama'.upper()] = df.kama()
        _['close'.capitalize()] = df().close
        if latest is None:
            latest = get_periods('Futures').get('simple')
            if isinstance(df, Equity):
                latest = get_periods('Equities').get('simple')
        code = df.code
        if isinstance(df, Equity):
            code = df.yahoo_code
        _.tail(latest).plot(title=f"{code} last: {latest}")


def mplot(df, last=200):
    import mplfinance as mpf
#     mc = mpf.make_marketcolors(up='tab:blue',down='tab:red', edge='lime', wick={'up':'blue','down':'red'}, volume='lawngreen')
#     s  = mpf.make_mpf_style(base_mpl_style="seaborn", marketcolors=mc)

    adps = [mpf.make_addplot(df.kc()[-last:], color='blue'), \
            mpf.make_addplot(df.kama()[-last:], color='silver'), \
            mpf.make_addplot(df.rsi()[-last:], panel=1, color='green'), \
            mpf.make_addplot(df.sar()[-last:], type='scatter', marker='o', markersize=1, color='black')]
    tt = df.code
    try:
        tt = df.yahoo_code
    except Exception:
        pass
    mpf.plot(df()[-last:], type='candle', style='charles', addplot=adps, title=tt, volume=True, ylabel='Price')


def _roundup(_, exchange):
    if pd.__version__ < '1.2':
        if exchange == 'HKEx':
            _.Upper = _.Upper.apply(roundup)
            _.Lower = _.Lower.apply(roundup)
        return _
    return _.applymap(roundup, na_action='ignore')


@app.get("/hkex/{code}")
async def quote_hk(code: int):
    _ = Equity(code, False)
    __ = f'{code:04d}.HK'
    return {__: f'{_}'}


@app.get("/tse/{code}")
async def quote_tse(code: str):
    __  = prefer_stock('TSE')
    return {f'{__[code.lower()]}.T': f'{Equity(__[code.lower()], exchange="TSE")}' if code.lower() in __.keys() else {}}


@app.get("/nyse/{code}")
async def quote_nyse(code: str):
    _ = Equity(code, exchange="NYSE")
    return {code.upper(): f'{_}'}
