import datetime
from pathlib import os, sys
from sqlalchemy import create_engine, Column, Integer, Date, Time, String, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
# from utilities import filepath
from pref import fields
Base = declarative_base()


def filepath(*args, **kwargs):
    name, file_type, data_path = args[0], 'data', 'sqlite3'
    if 'type' in list(kwargs.keys()):
        file_type = kwargs['type']
    if 'subpath' in list(kwargs.keys()):
        data_path = kwargs['subpath']
    if sys.platform == 'win32':
        if sys.version_info.major > 2 and sys.version_info.minor > 3:
            return os.sep.join((os.environ.get('HOMEPATH'), file_type, data_path, name))
        else:
            file_path = os.sep.join((os.environ.get('HOMEPATH'), file_type, data_path))
            _ = os.sep.join((file_path, name))
            return _ if os.path.exists(_) else False
    # if platform == 'linux-armv7l':file_drive, file_path = '', sep.join(('mnt', 'sdcard', file_type, data_path))
    if sys.platform in ('linux', 'linux2'):
        if sys.version_info.major > 2 and sys.version_info.minor > 3:
            if os.environ.get('EXTERNAL_STORAGE'):
                _ = os.sep.join((
                    os.environ.get('HOME'), 'storage', 'external-1', file_type,
                    data_path, name))
                if os.path.exists(_):
                    return _
            _ = os.sep.join((os.environ.get('HOME'), file_type, data_path, name))
            return _ if os.path.exists(_) else False
        else:
            place = 'shared'
            if os.environ.get('ACTUAL_HOME'):
                file_path = os.sep.join((os.environ.get('HOME'), file_type, data_path))
            elif os.environ.get('EXTERNAL_STORAGE') and ('/' in os.environ['EXTERNAL_STORAGE']):
                place = 'external-1'
                file_path = os.sep.join(
                    (os.environ.get('HOME'), 'storage', place, file_type, data_path))
            _ = os.sep.join((file_path, name))
            return _ if os.path.exists(_) else False
    else:
        path_holder_list = ['..', file_type, data_path]
        path_holder = os.sep.join(path_holder_list)
        if os.path.isfile(path_holder):
            return os.path.abspath(path_holder)
        else:
            path_holder = os.sep.join(['..', '..'] + path_holder_list)
            return os.path.abspath(path_holder) if os.path.isfile(path_holder) else False


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
