from sqlalchemy.orm import sessionmaker, declarative_base, load_only
from sqlalchemy import create_engine, Column, Integer, Date, Time, String, text
from utilities import filepath
import datetime
now = datetime.datetime.now()
Session = sessionmaker()
Base = declarative_base()

class Subject(Base):
    __tablename__ = 'records'
    id = Column(Integer, autoincrement=True, primary_key=True)
    instrument_id = Column(Integer, default=1, server_default=text('1'))
    subject_id = Column(Integer)
    date = Column(Date, default=now.date(), server_default=text(str(now.date())))
    time = Column(Time, default=now.time(), server_default=text(str(now.time())))
    sys = Column(Integer)
    dia = Column(Integer)
    pulse = Column(Integer)
    remarks = Column(String)

    def __repr__(self):
        _ = f"<Subject(instrument_id={self.instrument_id}, "
 #        _ = f"<Subject(id={self.id}, "
        _ += f"subject_id={self.subject_id}, date='{self.date}', time='{self.time}', "
        _ += f"sys={self.sys}, dia={self.dia}, pulse={self.pulse}, "
        _ += f"remarks='{self.remarks}')>"
        return _


class Health(Subject):
    def __init__(self, db='Health'):
        self.engine = create_engine(f"sqlite:///{filepath(db)}")
        Session.configure(bind=self.engine)
        self.session = Session()
        self.query = self.session.query(Subject)
