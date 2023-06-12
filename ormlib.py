import asyncio
import datetime
from time import sleep
from pathlib import os
from typing import Iterable
from sqlalchemy import asc, create_engine, Column, Integer, Date, Time, String, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
from utilities import yaml_db_get, filepath, get_start
from pref import fields

Base = declarative_base()


def stored_eid():
    query = select(Securities.eid.distinct()).order_by(asc(Securities.eid))
    res = asyncio.run(async_fetch(query, yaml_db_get('name')))
    exclude = yaml_db_get('exclude')
    return [_ for _ in res if  _ not in  exclude]


def web_collect(*args, **kwargs):
    import yfinance as yf
    src, lk, period, end, efor, res = 'yahoo', list(kwargs.keys()), 7, \
        datetime.today(), False, {}
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

    # def stored_eid():
    #     pE = pref.db['Equities']
    #     s_engine = db.create_engine(f"sqlite:///{filepath(pE['name'])}")
    #     s_conn = s_engine.connect()
    #     rc = db.Table(
    #         pE['table'],
    #         db.MetaData(),
    #         autoload=True,
    #         autoload_with=s_engine).columns
    #     query = db.select([rc.eid.distinct()]).order_by(db.asc(rc.eid))
    #     return [__ for __ in [_[0] for _ in s_conn.execute(query).fetchall()]
    #             if __ not in pE['exclude']]

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
        # dp = data.DataReader(code, src, start, end)
        for c in code:
            # res[c] = dp.minor_xs(c).transpose().to_dict()
            # res[c] = dp[c].transpose().to_dict()
            tmp = dp[c].transpose().to_dict()
            hdr = {}
            for _ in list(tmp.keys()):
                hdr[_.to_pydatetime().date()] = tmp[_]
            res[c] = hdr
            if efor:
                res[c] = dp[c]
            # if efor: res[c] = dp.minor_xs(c)
        return res


async def async_fetch(query: str, db_name: str) -> Iterable:
    a_Session = Session(db_name, sync=False)
    async with a_Session.session() as session:
        async with session.begin():
            result = (await session.execute(query)).scalars().all()
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
    _data_fields = fields
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
    _data_fields = fields
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
                self.engine = create_engine(f'sqlite:///{db_file}')
                self.session = sessionmaker(self.engine, expire_on_commit=False)
            else:
                self.engine = create_async_engine(f'sqlite+aiosqlite:///{db_file}')
                self.session = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
