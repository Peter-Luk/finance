from sqlalchemy.orm import sessionmaker, declarative_base, load_only
from sqlalchemy import create_engine, Column, Integer, Date, String, text
from utilities import filepath, waf
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


def commit(values):
    _ = Index(waf()[-1]).session
    _.all_all(values)
    _.commit()
