#!/usr/bin/env python3
import asyncio
import datetime
import pandas as pd
import pretty_errors
from sqlalchemy.future import select
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, Date, Time, String, text
from utilities import yaml_db_get, YAML_PREFERENCE
from ormlib import async_fetch, Session
# from finance.ormlib import async_fetch, Session

Base = declarative_base()


def rome(data, field, period=14):
    if isinstance(data, pd.core.frame.DataFrame) and field in data.columns:
        return data[field].rolling(period).mean()


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


class Person(Subject):
    def __init__(self, subject_id, db_name='Health'):
        self.db_name = db_name
        self.subject_id = subject_id
        # self.session = Health(self.db_name).session
        self.__fields = Subject._data_fields
        self.__fields.append('subject_id') if 'subject_id' not in self.__fields else None
        self.query = select(*[eval(f'Subject.{_}') for _ in Subject._data_fields]).where(Subject.subject_id == self.subject_id)

    def __call__(self):
        holder = asyncio.run(async_fetch(self.query, self.db_name))
        _ = pd.DataFrame()
        for field in Subject._data_fields:
            exec(f"_['{field}'] = [__.{field} for __ in holder]")
        _ = _.convert_dtypes()
        _[[col for col in _.columns if _[col].dtypes == object]] = _[[col for col in _.columns if _[col].dtypes == object]].astype('string')
        # _['Datetime'] = pd.to_datetime(_['date'] + ' ' + _['time'], format='ISO8601', dayfirst=True)
        _['Datetime'] = pd.to_datetime(_['date'] + ' ' + _['time'], dayfirst=True)
        _ = _.set_index(pd.DatetimeIndex(_['Datetime']))
        _ = _.drop(['subject_id', 'date', 'time', 'Datetime'], axis=1)
        return _

"""
    def update(self, values, criteria):
        q = self.query
        if 'id' in criteria.keys():
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
                vt = values['time']
                if isinstance(vt, (list, tuple)):
                    if len(vt) == 2:
                        rt = datetime.time(vt[0], vt[-1], sr.time.second, sr.time.microsecond)
                    elif len(vt) == 3:
                        rt = datetime.time(vt[0], vt[1], vt[-1], sr.time.microsecond)
                elif isinstance(vt, (str, datetime.time)):
                    rt = vt
                if sr.time < rt:
                    sr.date = datetime.date.fromordinal(sr.date.toordinal() - 1)
                sr.time = rt
        except Exception:
            return "Intend for unique record only"
        self.session.commit()
"""

if __name__ == "__main__":
    sid, confirm, dk = 1, 'Y', 'at ease prior to bed'
    if datetime.datetime.today().hour < 13:
        dk = 'wake up, washed before breakfast'

    while confirm.upper() != 'N':
        sj = Subject()
        sd = input(f"Subject ID (default: {sid}): ")
        try:
            sj.subject_id = int(sd)
        except Exception:
            sj.subject_id = sid
        sj.sys = input("Systolic: ")
        sj.dia = input("Diastolic: ")
        sj.pulse = input("Pulse: ")
        rmk = input(f"Remark (default: {dk}): ")
        sj.remarks = dk if rmk == '' else rmk
        _modify_at = datetime.datetime.now()
        sj.date = _modify_at.date()
        sj.time = _modify_at.time()
        pn = Session(yaml_db_get('name', 'Health', YAML_PREFERENCE))
        with pn.session() as session:
            with session.begin():
                session.add(sj)
            session.commit()
        pn.engine.dispose()
        confirm = input("Others? (Y)es/(N)o: ")
