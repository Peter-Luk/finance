from sqlalchemy.orm import sessionmaker, declarative_base, load_only
from sqlalchemy import create_engine, Column, Integer, Date, String, text
from utilities import filepath, waf
from pref import periods
import datetime
import pandas as pd
import numpy as np

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
        session = Column(String, default='M', server_default=text('M'))
    else:
        session = Column(String, default='A', server_default=text('A'))
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


class Index(Futures):
    def __init__(self, code):
        self.session = Futures('Futures').session
        self.code = code.upper()
        self.query = self.session.query(Record).filter(Record.code==self.code)
        self.__data = self.compose()

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
        _ = _.drop('date', axis=1)
        return _

    def sma(self, period=periods['Futures']['simple'], data=None):
        if isinstance(data, type(None)):
            data = self.__data.close
        _ = data.rolling(period).mean()
        _.name = 'sma'
        return _

    def wma(self, period=periods['Futures']['simple'], data=None):
        if isinstance(data, type(None)):
            data = self.__data
        _ = (data.close * data.volume).rolling(period).sum() / data.volume.rolling(period).sum()
        _.name = 'wma'
        return _

    def ema(self, period=periods['Futures']['simple'], data=None):
        if isinstance(data, type(None)):
            data = self.__data.close
        sma = self.sma(period, data)
        _ = pd.concat([data, sma], axis=1)
        i, tmp = 0, []
        while i < len(_):
            try:
                if pd.isna(tmp[i-1]):
                    v = _.iloc[i, 1]
                else:
                    v = (tmp[-1] * (period - 1) + _.iloc[i, 0]) / period
            except Exception:
                v = _.iloc[i, 1]
            tmp.append(v)
            i += 1
        _['ema'] = tmp
        return _.ema

    def rsi(self, period=periods['Futures']['rsi'], data=None):
        if isinstance(data, type(None)):
            data = self.__data
        fd = data.close.diff()

        def avgstep(source, direction, period):
            i, res = 0, []
            while i < len(source):
                if i < period:
                    _ = np.nan
                elif i == period:
                    hdr = source[:i]
                    if direction == '+':
                        _ = hdr[hdr.gt(0)].sum() / period
                    if direction == '-':
                        _ = hdr[hdr.lt(0)].abs().sum() / period
                else:
                    _ = res[i - 1]
                    if direction == '+' and source[i] > 0:
                        _ = (_ * (period - 1) + source[i]) / period
                    if direction == '-' and source[i] < 0:
                        _ = (_ * (period - 1) + abs(source[i])) / period
                res.append(_)
                i += 1
            return res

        ag = avgstep(fd, '+', period)
        al = avgstep(fd, '-', period)

        data['rsi'] = np.apply_along_axis(lambda a, b: 100 - 100 / (1 + a / b),0, ag, al)
        return data.rsi

    def atr(self, period=periods['Futures']['atr'], data=None):
        if isinstance(data, type(None)):
            data = self.__data
        pc = data.close.shift()
        _ = pd.concat([data.high - data.low, (data.high - pc).abs(), (data.low - pc).abs()], axis=1).max(axis=1)
        _['atr'] = self.ema(period, _)
        _.atr.name = 'atr'
        return _.atr

    def kama(self, period=periods['Futures']['kama'], data=None):
        if isinstance(data, type(None)):
            data = self.__data.close
        sma = self.sma(period['er'], data)
        acp = data.diff(period['er']).fillna(0).abs()
        vot = data.diff().fillna(0).abs().rolling(period['er']).sum()
        er = acp / vot
        sc = (er * (2 / (period['fast'] + 1) - 2 / (period['slow'] + 1)) + 2 / (period['slow'] + 1)) ** 2
        _ = pd.concat([data, sma, sc], axis=1)
        i, tmp = 0, []
        while i < len(_):
            try:
                if pd.isna(tmp[-1]):
                    v = _.iloc[i, 1]
                else:
                    v = tmp[-1] + _.iloc[i, 2] * (_.iloc[i, 0] - tmp[-1])
            except Exception:
                v = _.iloc[i, 1]
            tmp.append(v)
            i += 1
        _['kama'] = tmp
        return _.kama


def commit(values):
    _ = Index(waf()[-1]).session
    _.add_all(values)
    _.commit()
