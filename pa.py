from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import create_engine, Column, Integer, Date, Time, String, text
from utilities import filepath

Session = sessionmaker()
Base = declarative_base()

class Subject(Base):
    __tablename__ = 'records'
    id = Column(Integer, primary_key=True)
    instrument_id = Column(Integer)
    subject_id = Column(Integer)
    date = Column(Date)
    time = Column(Time)
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
    def __init__(self, db):
        self.engine = create_engine(f"sqlite:///{filepath(db)}")
        Session.configure(bind=self.engine)
        self.session = Session()
        self.query = self.session.query(Subject)
