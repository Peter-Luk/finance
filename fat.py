from sqlalchemy.orm import sessionmaker, declarative_base, deferred, defer
from sqlalchemy import create_engine, Column, Integer, Date, String, text
from utilities import filepath, waf, getcode
from fintools import FOA, pd, np
from finaux import roundup
from pref import periods
import datetime

Session = sessionmaker()
Base = declarative_base()

class Record(Base):
    __tablename__ = 'records'
    id = Column(Integer, autoincrement=True, primary_key=True)
    code = Column(String)
    _create_at = datetime.datetime.now()
    date = Column(Date, default=_create_at.date(),
            server_default=text(str(_create_at.date())))
    if _create_at.time() < datetime.time(12,25,0):
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


class Futures(Record):
    def __init__(self, db='Futures'):
        self.engine = create_engine(f"sqlite:///{filepath(db)}")
        Session.configure(bind=self.engine)
        self.session = Session()
        self.query = self.session.query(Record)


class Securities(Record):
    def __init__(self, db='Securities'):
        self.engine = create_engine(f"sqlite:///{filepath(db)}")
        Session.configure(bind=self.engine)
        self.session = Session()
        query = self.session.query(Record.code.label('code')).subquery()
        self.query = self.session.query(query.c.code.label('eid')).options(defer('session'))


class Index(Futures, FOA):
    def __init__(self, code):
        self.session = Futures('Futures').session
        self.code = code.upper()
        self.query = self.session.query(Record).filter(Record.code==self.code)
        self.__data = self.compose()
        self.analyser = FOA(self.__data, int)
        self.pct_change = self.__data.close.diff(1)[-1] / self.__data.close[-2] * 100
        self.date = self.__data.index[-1].to_pydatetime()
        __ = self.__data.iloc[-1]
        for _ in self.__data.columns:
            exec(f"self.{_}=__.{_}")

    def __call__(self):
        return self.compose()

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

    def sma(self, period=periods['Futures']['simple']):
        return self.analyser.sma(period)

    def wma(self, period=periods['Futures']['simple']):
        return self.analyser.wma(period)

    def ema(self, period=periods['Futures']['simple']):
        return self.analyser.ema(period)

    def macd(self, period=periods['Futures']['macd']):
        return self.analyser.macd(period)

    def rsi(self, period=periods['Futures']['rsi']):
        return self.analyser.rsi(period)

    def atr(self, period=periods['Futures']['atr']):
        return self.analyser.atr(period)

    def kama(self, period=periods['Futures']['kama']):
        return self.analyser.kama(period)

    def soc(self, period=periods['Futures']['soc']):
        return self.analyser.soc(period)

    def stc(self, period=periods['Futures']['stc']):
        return self.analyser.stc(period)

    def adx(self, period=periods['Futures']['adx']):
        return self.analyser.adx(period)

    def kc(self, period=periods['Futures']['kc']):
        _ = self.analyser.kc(period)
        _.dropna(inplace=True)
        _ = _.astype('int64')
        return _

    def apz(self, period=periods['Futures']['apz']):
        _ = self.analyser.apz(period)
        return _

    def dc(self, period=periods['Futures']['dc']):
        return self.analyser.dc(period)

    def obv(self):
        return self.analyser.obv()

    def vwap(self):
        return self.analyser.vwap()

    def bb(self, period=periods['Futures']['simple']):
        _ = self.analyser.bb(period)
        return _

    def mas(self, period={'simple':periods['Futures']['simple'], 'kama':periods['Futures']['kama']}):
        sma = self.sma(period['simple'])
        wma = self.wma(period['simple'])
        ema = self.ema(period['simple'])
        kama = self.kama(period['kama'])
        __ = pd.concat([sma, wma, ema, kama], axis=1)
        __['high'] = __.max(axis=1)
        __['low'] = __.min(axis=1)
        __.drop(['sma', 'wma', 'ema', 'kama'], axis=1, inplace=True)
        return __


class Equity(Securities, FOA):
    def __init__(self, code, static=True, exchange='HKEx'):
        self.session = Securities('Securities').session
        self.code = code
        self.exchange = exchange
        self.__fields = ['date', 'open', 'high', 'low', 'close', 'volume']
        if exchange == 'HKEx':
            self.query = self.session.query(*[eval(f"Record.{_}") for _ in self.__fields]).filter(text(f"eid={self.code}"))
        self.__data = self.compose(static)
        self.analyser = FOA(self.__data, float)
        self.pct_change = self.__data.close.diff(1)[-1] / self.__data.close[-2] * 100
        self.date = self.__data.index[-1].to_pydatetime()
        __ = self.__data.iloc[-1]
        for _ in self.__data.columns:
            exec(f"self.{_}=__.{_}")
            if self.exchange == 'HKEx':
                exec(f"self.{_}=roundup(__.{_})")

    def __call__(self, static=True):
        return self.__data

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
            __ = yf.download(getcode(self.code, self.exchange), start, today, group_by='ticker')
            __.drop('Adj Close', axis=1, inplace=True)
            __.columns = [_.lower() for _ in __.columns]
            __.index.name = __.index.name.lower()
        return __

    def sma(self, period=periods['Equities']['simple']):
        _ = self.analyser.sma(period).astype('float64')
        if self.exchange == 'HKEx':
            _ = _.apply(roundup)
        return _

    def wma(self, period=periods['Equities']['simple']):
        _ = self.analyser.wma(period).astype('float64')
        if self.exchange == 'HKEx':
            _ = _.apply(roundup)
        return _

    def ema(self, period=periods['Equities']['simple']):
        _ = self.analyser.ema(period).astype('float64')
        if self.exchange == 'HKEx':
            _ = _.apply(roundup)
        return _

    def macd(self, period=periods['Equities']['macd']):
        return self.analyser.macd(period)

    def rsi(self, period=periods['Equities']['rsi']):
        return self.analyser.rsi(period)

    def atr(self, period=periods['Equities']['atr']):
        return self.analyser.atr(period)

    def kama(self, period=periods['Equities']['kama']):
        _ = self.analyser.kama(period).astype('float64')
        if self.exchange == 'HKEx':
            _ = _.apply(roundup)
        return _

    def soc(self, period=periods['Equities']['soc']):
        return self.analyser.soc(period)

    def stc(self, period=periods['Equities']['stc']):
        return self.analyser.stc(period)

    def adx(self, period=periods['Equities']['adx']):
        return self.analyser.adx(period)

    def kc(self, period=periods['Equities']['kc']):
        _ = self.analyser.kc(period)
        if self.exchange == 'HKEx':
            _.Upper = _.Upper.apply(roundup)
            _.Lower = _.Lower.apply(roundup)
        return _

    def apz(self, period=periods['Equities']['apz']):
        _ = self.analyser.apz(period)
        if self.exchange == 'HKEx':
            _.Upper = _.Upper.apply(roundup)
            _.Lower = _.Lower.apply(roundup)
        return _

    def dc(self, period=periods['Equities']['dc']):
        _ = self.analyser.dc(period)
        return _

    def obv(self):
        return self.analyser.obv()

    def vwap(self):
        return self.analyser.vwap()

    def bb(self, period=periods['Equities']['simple']):
        _ = self.analyser.bb(period)
        if self.exchange == 'HKEx':
            _.Upper = _.Upper.apply(roundup)
            _.Lower = _.Lower.apply(roundup)
        return _

    def mas(self, period={'simple':periods['Equities']['simple'], 'kama':periods['Equities']['kama']}):
        sma = self.sma(period['simple'])
        wma = self.wma(period['simple'])
        ema = self.ema(period['simple'])
        kama = self.kama(period['kama'])
        __ = pd.concat([sma, wma, ema, kama], axis=1)
        __['high'] = __.max(axis=1)
        __['low'] = __.min(axis=1)
        if self.exchange == 'HKEx':
            __.high = __.high.apply(roundup)
            __.low = __.low.apply(roundup)
        __.drop(['sma', 'wma', 'ema', 'kama'], axis=1, inplace=True)
        return __


def commit(values):
    _ = Index(waf()[-1]).session
    _.add_all(values)
    _.commit()
