import asyncio
import datetime
import aiosqlite
import pandas as pd
from time import sleep
from pathlib import os
from typing import Iterable, Any
from sqlalchemy import asc, desc, create_engine, Column, Integer, Date, Time, String, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
from benedict import benedict
from utilities import today, futures_type, filepath, get_start, waf, ltd, YAML
Base = declarative_base()


def trade_data(code: Any, derivative: bool = False, capitalize: bool = False) -> pd.DataFrame:
    fields = ['date', 'session'] if derivative else ['date']
    fields.extend(YAML.fields)
    if derivative:
        query = select(*[eval(f'Derivatives.{_}') for _ in fields]).where(Derivatives.code ==  code)
        holder = asyncio.run(async_fetch(query, YAML.db.Futures.name))
        tdf = pd.DataFrame(holder)
        tdf.columns = fields
        date_ = tdf.date.unique()
        open_ = []
        high_ = []
        low_ = []
        close_ = []
        volume_ = []
        for da in date_:
            t = tdf[tdf['date'] == da]
            if len(t) < 2:
                open_.append(t.iloc[-1].open)
                high_.append(t.iloc[-1].high)
                low_.append(t.iloc[-1].low)
                close_.append(t.iloc[-1].close)
                volume_.append(t.iloc[-1].volume)
            else:
                open_.append(t.open[t['session'] == 'M'].iloc[0])
                close_.append(t.close[t['session'] == 'A'].iloc[0])
                high_.append(t.high.max())
                low_.append(t.low.min())
                volume_.append(t.volume.sum())
        df = pd.DataFrame(date_)
        df.columns = ['date']
        df['open'] = open_
        df['high'] = high_
        df['low'] = low_
        df['close'] = close_
        df['volume'] = volume_
    else:
        query = select(*[eval(f'Securities.{_}') for _ in fields]).where(Securities.eid == code)
        holder = sync_fetch(query, YAML.db.Equities.name)
        df = pd.DataFrame(holder)
        df.columns = fields
    df = df.set_index(pd.DatetimeIndex(df.date))
    df = df.drop(columns=['date'])
    if capitalize:
        df.columns = [_.capitalize() for _ in YAML.fields]
    return df


""""
def entities(dbname: str = None):
    if dbname is None:
        dbname = YAML.db_Equities.name
    if dbname == YAML.db.Equities.name:
        query = select(Securities.eid.distinct()).order_by(asc(Securities.eid))
        __ = asyncio.run(async_fetch(query, dbname))
        exclude = YAML.db.Equities.exclude
        res =  [_[0] for _ in __ if _ not in exclude]
    if dbname == YAML.db.Futures.name:
        query = select(Derivatives.code.distinct()).order_by(asc(Derivatives.code))
        __ = asyncio.run(async_fetch(query, dbname))
        res =  [_[0] for _ in __]
    return res
"""

def stored_eid():
    query = select(Securities.eid.distinct()).order_by(asc(Securities.eid))
    res = asyncio.run(async_fetch(query, YAML.db.Equities.name))
    exclude = YAML.db.Equities.exclude
    return [_ for _ in res if _ not in exclude]


def web_collect(*args, **kwargs):
    import yfinance as yf
    src, lk, period, end, efor, res = 'yahoo', list(kwargs.keys()), 7, \
        datetime.datetime.today(), False, {}
    if args:
        if isinstance(args[0], (tuple, list)):
            code = list(args[0])
        if isinstance(args[0], (int, float)):
            code = [int(args[0])]
        if len(args) > 1:
            if isinstance(args[1], (int, float)):
                period = int(args[1])
    if 'code' in lk:
        if isinstance(kwargs['code'], (tuple, list)):
            code = list(kwargs['code'])
        if isinstance(kwargs['code'], (int, float)):
            code = [int(kwargs['code'])]
    if 'period' in lk:
        if isinstance(kwargs['period'], (int, float)):
            period = int(kwargs['period'])
    if 'source' in lk:
        if isinstance(kwargs['source'], str):
            src = kwargs['source']
    if 'pandas' in lk:
        if isinstance(kwargs['pandas'], bool):
            efor = kwargs['pandas']
    try:
        code
    except Exception:
        code = stored_eid()
    if src == 'yahoo':
        code = [f'{_:04d}.HK' for _ in code]
    process, start = True, get_start(period)
    if start:
        while process:
            dp = yf.download(code, start, end, group_by='ticker')
            if len(dp):
                process = not process
            else:
                print('Retry in 25 seconds')
                sleep(25)
        for c in code:
            tmp = dp[c].transpose().to_dict()
            hdr = {}
            for _ in list(tmp.keys()):
                hdr[_.to_pydatetime().date()] = tmp[_]
            res[c] = hdr
            if efor:
                res[c] = dp[c]
        return res


def mtf(*args, **kwargs):
    ftype = futures_type[:2]
    if args:
        if isinstance(args[0], str):
            ftype = [args[0]]
        else:
            ftype = list(args[0])
    if 'type' in list(kwargs.keys()):
        if isinstance(kwargs['type'], str):
            ftype = [kwargs['type']]
        else:
            ftype = list(kwargs['type'])
    fi = []
    query = select(Derivatives.volume).order_by(desc(Derivatives.date))
    awaf = waf()
    if today.day == ltd(today.year, today.month):
        awaf = waf(1)
    for _ in ftype:
        aft = []
        for __ in awaf:
            if _.upper() in __:
                aft.append(__)
        try:
            nfv = sync_fetch(query.where(Derivatives.code == aft[1]),
                    YAML.db.Futures.name)[0]

            cfv = sync_fetch(query.where(Derivatives.code == aft[0]),
                    YAML.db.Futures.name)[0]
            if cfv > nfv:
                fi.append(aft[0])
            else:
                fi.append(aft[1])
        except Exception:
            fi.append(aft[0])
    if len(fi) == 1:
        return fi.pop()
    return fi


async def async_fetch(query: str, db_name: str) -> Iterable:
    a_Session = Session(db_name, sync=False)
    async with a_Session.session() as session:
        async with session.begin():
            result = (await session.execute(query)).fetchall()
        await session.commit()
    await a_Session.engine.dispose()
    return result


def sync_fetch(query: str, db_name: str) -> Iterable:
    s_Session = Session(db_name, sync=True)
    with s_Session.session() as session:
        with session.begin():
            result = session.execute(query).fetchall()
        session.commit()
    s_Session.engine.dispose()
    return result


class Securities(Base):
    __tablename__ = 'records'
    id = Column(Integer, autoincrement=True, primary_key=True)
    eid = Column(Integer)
    _data_fields = YAML.fields
    _create_at = datetime.datetime.now()
    date = Column(Date, default=_create_at.date(), server_default=text(str(_create_at.date())))
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Integer)


class Derivatives(Base):
    __table_args__ = {'extend_existing': True}
    __tablename__ = 'records'
    id = Column(Integer, autoincrement=True, primary_key=True)
    code = Column(String)
    _data_fields = YAML.fields
    _create_at = datetime.datetime.now()
    date = Column(Date, default=_create_at.date(), server_default=text(str(_create_at.date())))
    _session = 'M' if _create_at.time() < datetime.time(12, 25, 0) else 'A'
    session = Column(String, default=_session, server_default=text(_session))
    # session = deferred(Column(String, default=_session, server_default=text(_session)))
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Integer)


class Subject(Base):
    __table_args__ = {'extend_existing': True}
    __tablename__ = 'records'
    _data_fields = ['date', 'time', 'sys', 'dia', 'pulse']
    id = Column(Integer, autoincrement=True, primary_key=True)
    instrument_id = Column(Integer, default=1, server_default=text('1'))
    subject_id = Column(Integer)
    _created_at = datetime.datetime.now()
    date = Column(Date, default=_created_at.date(), server_default=text(str(_created_at.date())))
    time = Column(Time, default=_created_at.time(), server_default=text(str(_created_at.time())))
    sys = Column(Integer)
    dia = Column(Integer)
    pulse = Column(Integer)
    remarks = Column(String)


class Session(object):
    def __init__(self, db_name: str, sync: bool = True):
        db_file = filepath(db_name)
        if os.path.isfile(db_file):
            if sync:
                self.engine = create_engine(f'sqlite+pysqlite:///{db_file}')
                self.session = sessionmaker(self.engine, expire_on_commit=False)
            else:
                self.engine = create_async_engine(f'sqlite+aiosqlite:///{db_file}')
                self.session = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
