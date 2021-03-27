import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, insert, update
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
        rf = ['date', 'time', 'sys', 'dia', 'pulse']
        criteria['subject_id'] = self.sid
        clist = []
        for k, v in criteria.items():
            _ = f"{k}='{v}'"
            if isinstance(v, int): _ = f'{k}={v}'
            clist.append(_)
        qstr = f"SELECT {', '.join(rf)} FROM records \
                WHERE {' and '.join(clist)}"
        rd = pd.read_sql(qstr, self._connect)
:         def stime(_):

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

    def alter(self, vars, criteria, backward=True):
        import datetime

        def idtime(criteria=criteria):
            if isinstance(criteria, dict):
                criteria['subject_id'] = self.sid
                clist = []
                for k, v in criteria.items():
                    __ = f"{k}='{v}'"
                    if isinstance(v, int): __ = f'{k}={v}'
                clist.append(__)
            return self._connect.execute(f"SELECT id, time, date FROM records WHERE {' and '.join(clist)}").fetchall()[-1]

        if 'time' in vars.keys() and isinstance(vars['time'], (tuple, list)):
            c_dict, _d = {}, idtime()
            if _d:
                yr, mt, dy = [int(_) for _ in _d[-1].split('-')]
                rd = datetime.date(yr, mt, dy)
                rt = _d[1].split(':')[:-1]
                rt.extend(_d[1].split(':')[-1].split('.'))
                rt = [int(x) for x in rt]
                idt = datetime.time(rt[0], rt[1], rt[2], rt[-1])
                if len(vars['time']) == 2:
                    _ = datetime.time(vars['time'][0], vars['time'][1], idt.second, idt.microsecond)
                if len(vars['time']) == 3:
                    _ = datetime.time(vars['time'][0], vars['time'][1], vars['time'][2], idt.microsecond)
                if backward and (_ > idt):
                    rd = rd.fromordinal(rd.toordinal() - 1)
                c_dict['time'] = _
                c_dict['date'] = rd
        cstr = ', '.join([f"{k}='{v}'" for k, v in c_dict.items()])
        qstr = f"UPDATE records SET {c_dict} WHERE id={int(_d[0])}"
        self._connect.execute(qstr)

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
                query = insert(self._table)
                now = datetime.now().astimezone(timezone(self.tz))
                hdr['date'], hdr['time'] = now.date(), now.time()
                self._connect.execute(query, [hdr])
