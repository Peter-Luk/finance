import datetime
from pathlib import os, sys
from typing import Iterable
from sqlalchemy import create_engine, Column, Integer, Date, Time, String, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from utilities import filepath
from pref import fields
Base = declarative_base()


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
