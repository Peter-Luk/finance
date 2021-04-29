import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, insert, text, select, DateTime
from utilities import filepath, datetime


class Record(object):

    def __init__(self, sid, iid=None, tz='Asia/Hong_Kong'):
        if not iid:
            iid = 1
        # self.iid = iid
        if isinstance(sid, int) and isinstance(iid, int):
            self.sid, self.iid, self.tz = sid, iid, tz
            engine = create_engine(f"sqlite:///{filepath('Health')}")
            self._table = Table(
                'records',
                MetaData(),
                autoload=True,
                autoload_with=engine)
            self._columns = self._table.columns
            self._connect = engine.connect()

    def __call__(self):
        return self.grab()

    def __del__(self):
        self.sid = engine = self._table = self._columns = self._connect = None
        del(self.sid, engine, self._table, self._columns, self._connect)

    def grab(self, criteria={}):
        sc = self._columns
        rl = [sc.date, sc.time, sc.sys, sc.dia, sc.pulse]
        clist = []
        for k, v in criteria.items():
            _ = f"{k}='{v}'"
            if isinstance(v, int): _ = f'{k}={v}'
            clist.append(_)
        qstr = select(rl).filter(sc.subject_id==self.sid)
        for i in clist:
            qstr = qstr.filter(text(i))
        rd = pd.read_sql(qstr, self._connect)

        def comdt(_):
            try:
                __ = datetime.strptime(
                    f'{_[0]} {_[1]}',
                    '%Y-%m-%d %H:%M:%S.%f')
            except Exception:
                __ = datetime.strptime(f'{_[0]} {_[1]}', '%Y-%m-%d %H:%M:%S')
            return __
        rd.index = rd.apply(comdt, axis=1)
        rd.drop(['date', 'time'], axis=1, inplace=True)
        return rd

    def update(self, values, criteria, backward=True):
        import datetime

        def idtime(criteria=criteria):
            if isinstance(criteria, dict):
                criteria['subject_id'] = self.sid
                sc = self._columns
                qstr = select([sc.id, sc.time, sc.date])
                for k, v in criteria.items():
                    if k == 'date':
                        dd = datetime.datetime.strptime(v, '%Y-%m-%d').date()
                        qstr = qstr.filter(text(f"{sc[k]}='{dd}'"))
                    elif k == 'time':
                        dd = datetime.datetime.strptime(v, '%H:%M:%S').time()
                        qstr = qstr.filter(text(f"{sc[k]}='{dd}'"))
                    elif k == 'remarks':
                        qstr = qstr.filter(text(f"{s[k]}='{v}'"))
                    else:
                        qstr = qstr.filter(text(f"{sc[k]}={v}"))
            return self._connect.execute(str(qstr)).fetchall()[-1]

        c_dict = {}
        for i in ['sys', 'dia', 'pulse']:
            if i in values.keys():
                if isinstance(values[i], int):
                    c_dict[self._columns[i]] = values[i]
                if isinstance(values[i], str):
                    try:
                        c_dict[self._columns[i]] = int(values[i])
                    except Exception:
                        pass

        if 'remarks' in values.keys() and isinstance(values['remarks'], str):
            c_dict[self._columns['remarks']] = text(values['remarks'])

        if 'date' in values.keys():
            if isinstance(values['date'], datetime.date):
                c_dict[self._columns['date']] = DateTime(values['date'])
            if isinstance(values['date'], (tuple, list)):
                yr, mn, dy = [int(_) for _ in values['date']]
                c_dict[self._columns['date']] = DateTime(datetime.date(yr, mn, dy))

        if 'time' in values.keys():
            _d = idtime()
            if _d:
                yr, mt, dy = [int(_) for _ in _d[-1].split('-')]
                rd = datetime.date(yr, mt, dy)
                rt = _d[1].split(':')[:-1]
                rt.extend(_d[1].split(':')[-1].split('.'))
                rh, rm, rs, rms = [int(x) for x in rt]
                idt = datetime.time(rh, rm, rs, rms)

            if isinstance(values['time'], datetime.time):
                _ = values['time']

            if isinstance(values['time'], (tuple, list)):
                if len(values['time']) == 2:
                    _ = datetime.time(values['time'][0], values['time'][1], idt.second, idt.microsecond)
                if len(values['time']) == 3:
                    _ = datetime.time(values['time'][0], values['time'][1], values['time'][2], idt.microsecond)

            if backward and (_ > idt):
                rd = rd.fromordinal(rd.toordinal() - 1)
            c_dict[self._columns['time']] = DateTime(_).timezone
            c_dict[self._columns['date']] = DateTime(rd).timezone
        qstr = self._table.update().values(c_dict).filter(self._columns.id==_d[0])
        self._connect.execute(qstr)

    def rome(self, field, period=14):
        data = self.grab()
        return rome(data, field, period)

    def append(self, *args):
        from pytz import timezone
        if args:
            hdr, largs = {'subject_id': self.sid}, len(args)
            if largs == 4:
                if isinstance(args[0], int):
                    hdr['sys'] = args[0]
                if isinstance(args[1], int):
                    hdr['dia'] = args[1]
                if isinstance(args[2], int):
                    hdr['pulse'] = args[2]
                if isinstance(args[3], str):
                    hdr['remarks'] = args[3]
                hdr['instrument_id'] = self.iid
                now = datetime.now().astimezone(timezone(self.tz))
                hdr['date'], hdr['time'] = now.date(), now.time()
                self._connect.execute(insert(self._table), [hdr])


def rome(data, field, period=14):
    if isinstance(data, pd.core.frame.DataFrame) and field in data.columns:
        return data[field].rolling(period).mean()


if __name__ == "__main__":
    from pathlib import sys

    def process(i, s, d, p, r):
        _ = Record(i)
        _.append(int(s), int(d), int(p), r)

    sid, confirm, dk = 1, 'Y', 'at ease prior to bed'
    if datetime.today().hour < 13:
        dk = 'wake up, washed before breakfast'

    while confirm.upper() != 'N':
        if sys.version_info.major == 2:
            sd = raw_input("Subject ID")
            try:
                sd = int(sd)
            except Exception:
                sd = sid
            sy = raw_input("Systolic: ")
            dia = raw_input("Diastolic: ")
            pul = raw_input("Pulse: ")
            rmk = raw_input("Remark: ")
            if rmk == '':
                rmk = dk
            process(sd, sy, dia, pul, rmk)
            confirm = raw_input("Others? (Y)es/(N)o: ")
        if sys.version_info.major == 3:
            sd = input(f"Subject ID (default: {sid}): ")
            try:
                sd = int(sd)
            except Exception:
                sd = sid
            sy = input("Systolic: ")
            dia = input("Diastolic: ")
            pul = input("Pulse: ")
            rmk = input(f"Remark (default: {dk}): ")
            if rmk == '':
                rmk = dk
            process(sd, sy, dia, pul, rmk)
            confirm = input("Others? (Y)es/(N)o: ")
