from sqlalchemy.orm import sessionmaker, declarative_base, load_only
from sqlalchemy import create_engine, Column, Integer, Date, String, text
from utilities import filepath, waf
from pref import periods
import datetime
import pandas as pd

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

    def sma(self, period=7):
        tmp = self.compose().close.rolling(period).mean()
        tmp.name = 'sma'
        return tmp

    def wma(self, period=7):
        _ = self.compose()
        tmp = (_.close * _.volume).rolling(period).sum() / _.volume.rolling(period).sum()
        tmp.name = 'wma'
        return tmp

    def ema(self, period=7):
        _ = self.compose()
        sma = self.sma(period)
        dp = pd.concat([_.close, sma], axis=1)
        tmp = []
        for i in range(len(dp)):
            try:
                if pd.isna(tmp[i-1]):
                    v = dp.iloc[i].sma
                else:
                    v = (tmp[-1] * (period - 1) + dp.iloc[i].close) / period
            except Exception:
                v = dp.iloc[i].sma
            tmp.append(v)
        dp['ema'] = tmp
        return dp.ema

    def rsi(self, period=periods['Futures']['rsi']):
        from numpy import nan
        _ = self.compose()
        fd = _.close.diff()
        ag, al, tmp = [], [], []

        for i in range(len(fd)):
            if i < period:
                ag.append(nan)
                al.append(nan)
            elif i == period:
                tv = fd[:period]
                ag.append(tv[tv.gt(0)].sum() / period)
                al.append(tv[tv.lt(0)].abs().sum() / period)
            else:
                __ = ag[i - 1]
                if fd.iloc[i] > 0:
                    __ = (__ * (period - 1) + fd.iloc[i]) / period
                ag.append(__)
                __ = al[i - 1]
                if fd.iloc[i] < 0:
                    __ = (__ * (period - 1) + abs(fd.iloc[i])) / period
                al.append(__)

        for i in range(len(fd)):
            t_ = nan
            if i >= period:
                t_ = 100 - 100 / (1 + ag[i] / al[i])
            tmp.append(t_)
        _['rsi'] = tmp
        return _.rsi


def commit(values):
    _ = Index(waf()[-1]).session
    _.add_all(values)
    _.commit()
