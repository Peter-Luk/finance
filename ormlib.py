import datetime
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, Date, Time, String, text
from pref import fields
Base = declarative_base()


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
