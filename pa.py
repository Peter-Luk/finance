from sqlalchemy.orm import sessionmaker, declarative_base, load_only
from sqlalchemy import create_engine, Column, Integer, Date, Time, String, text
from utilities import filepath
import datetime
import pandas as pd

Session = sessionmaker()
Base = declarative_base()

def rome(data, field, period=14):
    if isinstance(data, pd.core.frame.DataFrame) and field in data.columns:
        return data[field].rolling(period).mean()

class Subject(Base):
    __tablename__ = 'records'
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

    def __repr__(self):
        _ = f"<Subject(instrument_id={self.instrument_id}, "
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
        self.subject_id = subject_id
        self.query = self.session.query(Subject).filter(Subject.subject_id==self.subject_id)

    def __call__(self):
        format = '%Y-%m-%d %H:%M:%S'
        fields = ['date', 'time', 'sys', 'dia', 'pulse']
        cols = ['date', 'time']
        _ = pd.read_sql(self.query.options(load_only(*fields)).statement, self.session.bind, parse_dates=[cols])
        _ = _.convert_dtypes()
        _[[col for col in _.columns if _[col].dtypes == object]] = _[[col for col in _.columns if _[col].dtypes == object]].astype('string')
        _['Datetime'] = pd.to_datetime(_['date'] + ' ' + _['time'], format=format)
        _ = _.set_index(pd.DatetimeIndex(_['Datetime']))
        _ = _.drop(['id', 'date','time', 'Datetime'], axis=1)
        return _

    def update(self, values, criteria):
        q = self.query
        if 'date' in criteria.keys():
            if isinstance(criteria['date'], (datetime.date, str)):
                q = q.filter(Subject.date == criteria['date'])
        if 'time' in criteria.keys() and 'lesser' in criteria.keys():
            if criteria['lesser']:
                q = q.filter(Subject.time < criteria['time'])
        if 'time' in criteria.keys() and 'greater' in criteria.keys():
            if criteria['greater']:
                q = q.filter(Subject.time < criteria['time'])
        if 'time' in criteria.keys() and 'lesser' not in criteria.keys() and 'greater' not in criteria.keys():
            q = q.filter(Subject.time == criteria['time'])
        try:
            sr = q.one()
            if 'sys' in values.keys():
                if isinstance(values['sys'], int):
                    sr.sys = values['sys']
            if 'dia' in values.keys():
                if isinstance(values['dia'], int):
                    sr.dia = values['dia']
            if 'pulse' in values.keys():
                if isinstance(values['pulse'], int):
                    sr.pulse = values['pulse']
            if 'date' in values.keys():
                if isinstance(values['date'], (str, datetime.date)):
                    sr.date = values['date']
            if 'remarks' in values.keys():
                if isinstance(values['remarks'], str):
                    sr.remarks = values['remarks']
            if 'time' in values.keys():
                if isinstance(values['time'], (list, tuple)):
                    if len(values['time']) == 2:
                        rt = datetime.time(values[0], values[-1], sr.time.second, sr.time.microsecond)
                    elif len(values['time']) == 3:
                        rt = datetime.time(values[0], values[1], values[-1], sr.time.microsecond)
                elif isinstance(values['time'], (str, datetime.time)):
                    rt = values['time']
                if sr.time < rt:
                    sr.date = datetime.date.fromordinal(sr.date.toordinal() - 1)
                sr.time = rt
        except Exception:
            return "Intend for unique record only"
        self.session.commit()


if __name__ == "__main__":
    from pathlib import sys
    sid, confirm, dk = 1, 'Y', 'at ease prior to bed'
    if datetime.datetime.today().hour < 13:
        dk = 'wake up, washed before breakfast'

    while confirm.upper() != 'N':
        sj = Subject()
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
            sj.remarks = rmk
            _modify_at = datetime.datetime.now()
            sj.date = _modify_at.date()
            sj.time = _modify_at.time()
            pn.session.add(sj)
            pn.session.commit()
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
            _modify_at = datetime.datetime.now()
            sj.date = _modify_at.date()
            sj.time = _modify_at.time()
            pn.session.add(sj)
            pn.session.commit()
            confirm = input("Others? (Y)es/(N)o: ")
