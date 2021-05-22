from sqlalchemy.orm import sessionmaker, declarative_base, deferred, defer
from sqlalchemy import create_engine, Column, Integer, Date, String, text
from utilities import filepath, waf
from pafintools import FOA
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
        self.analyser = FOA(self.__data)

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

    def sma(self, period=periods['Futures']['simple']):
        return self.analyser.sma(period)

    def wma(self, period=periods['Futures']['simple']):
        return self.analyser.wma(period)

    def ema(self, period=periods['Futures']['simple']):
        return self.analyser.ema(period)

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
        return self.analyser.kc(period)

    def apz(self, period=periods['Futures']['apz']):
        return self.analyser.apz(period)

    def dc(self, period=periods['Futures']['dc']):
        return self.analyser.dc(period)


class Equity(Securities, FOA):
    def __init__(self, code):
        self.session = Securities('Securities').session
        self.code = code
        fields = [Record.date, Record.open, Record.high, Record.low, Record.close, Record.volume]
        self.query = self.session.query(*fields).filter(text(f"eid={self.code}"))
        self.__data = self.compose()
        self.analyser = FOA(self.__data)

    def __call__(self):
        return self.compose()

    def compose(self):
        _ = pd.read_sql(self.query.statement, self.session.bind, parse_dates=['date'])
        _ = _.set_index(pd.DatetimeIndex(_.date))
        _ = _.drop('date', axis=1)
        return _

    def sma(self, period=periods['Equities']['simple']):
        return self.analyser.sma(period)

    def wma(self, period=periods['Equities']['simple']):
        return self.analyser.wma(period)

    def ema(self, period=periods['Equities']['simple']):
        return self.analyser.ema(period)

    def rsi(self, period=periods['Equities']['rsi']):
        return self.analyser.rsi(period)

    def atr(self, period=periods['Equities']['atr']):
        return self.analyser.atr(period)

    def kama(self, period=periods['Equities']['kama']):
        return self.analyser.kama(period)

    def soc(self, period=periods['Equities']['soc']):
        return self.analyser.soc(period)

    def stc(self, period=periods['Equities']['stc']):
        return self.analyser.stc(period)

    def adx(self, period=periods['Equities']['adx']):
        return self.analyser.adx(period)

    def kc(self, period=periods['Equities']['kc']):
        return self.analyser.kc(period)

    def apz(self, period=periods['Equities']['apz']):
        return self.analyser.apz(period)

    def dc(self, period=periods['Equities']['dc']):
        return self.analyser.dc(period)


def commit(values):
    _ = Index(waf()[-1]).session
    _.add_all(values)
    _.commit()
