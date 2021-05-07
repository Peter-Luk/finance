from sqlalchemy.orm import sessionmaker, declarative_base, load_only
from sqlalchemy import create_engine, Column, Integer, String, text
from utilities import filepath
import datetime
import pandas as pd

now = datetime.datetime.now()
Session = sessionmaker()
Base = declarative_base()

class Subject(Base):
    __tablename__ = 'records'
    id = Column(Integer, autoincrement=True, primary_key=True)
    instrument_id = Column(Integer, default=1, server_default=text('1'))
    subject_id = Column(Integer)
    date = Column(String, default=now.date(), server_default=text(str(now.date())))
    time = Column(String, default=now.time(), server_default=text(str(now.time())))
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

class Person(Health):
    def __init__(self, subject_id):
        self.session = Health('Health').session
        self.query = self.session.query(Subject).filter_by(subject_id=subject_id)
        self.subject_id = subject_id

    def __call__(self, fields=None):
        format = '%Y-%m-%d %H:%M:%S'
        if fields == None:
            fields = ['date', 'time', 'sys', 'dia', 'pulse']
        _ = pd.read_sql(self.query.options(load_only(*fields)).statement, self.session.bind, parse_dates=[['date', 'time']])
        _['Datetime'] = pd.to_datetime(_['date'] + ' ' + _['time'], format=format)
        _ = _.set_index(pd.DatetimeIndex(_['Datetime']))
        _ = _.drop(['id', 'date','time', 'Datetime'], axis=1)
        return _


if __name__ == "__main__":
    from pathlib import sys

    # def process(i, s, d, p, r):
    #     _ = Record(i)
    #     _.append(int(s), int(d), int(p), r)
    sj = Subject()

    sid, confirm, dk = 1, 'Y', 'at ease prior to bed'
    if datetime.today().hour < 13:
        dk = 'wake up, washed before breakfast'

    while confirm.upper() != 'N':
        if sys.version_info.major == 2:
            sd = raw_input("Subject ID")
            try:
                sj.subject_id = int(sd)
                pn = Person(sj.subject_id)
            except Exception:
                sj.subject_id = sid
                pn = Person(sj.subject_id)
            sj.sys = raw_input("Systolic: ")
            sj.dia = raw_input("Diastolic: ")
            sj.pulse = raw_input("Pulse: ")
            rmk = raw_input("Remark: ")
            if rmk == '':
                rmk = dk
            self.remarks = rmk
            now = datetime.datetime.now()
            sj.date = str(now.date())
            sj.time = str(now.time())
            pn.session.add(sj)
            pn.session.commit()
            # process(sd, sy, dia, pul, rmk)
            confirm = raw_input("Others? (Y)es/(N)o: ")
        if sys.version_info.major == 3:
            sd = input(f"Subject ID (default: {sid}): ")
            try:
                sj.subject_id = int(sd)
                pn = Person(sj.subject_id)
            except Exception:
                sj.subject_id = sid
                pn = Person(sj.subject_id)
            sj.sys = input("Systolic: ")
            sj.dia = input("Diastolic: ")
            sj.pulse = input("Pulse: ")
            rmk = input(f"Remark (default: {dk}): ")
            if rmk == '':
                rmk = dk
            sj.remarks = rmk
            now = datetime.datetime.now()
            sj.date = str(now.date())
            sj.time = str(now.time())
            pn.session.add(sj)
            pn.session.commit()
            # process(sd, sy, dia, pul, rmk)
            confirm = input("Others? (Y)es/(N)o: ")
