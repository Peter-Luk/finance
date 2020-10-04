from sqlalchemy import create_engine, MetaData, Table, insert, update
from utilities import filepath, datetime
import pandas as pd
class Record(object):
    def __init__(self, sid, iid=None, tz='Asia/Hong_Kong'):
        if not iid:
            iid = 1
        # self.iid = iid
        if isinstance(sid, int) and isinstance(iid, int):
            self.sid, self.iid, self.tz = sid, iid, tz
            engine = create_engine(f"sqlite:///{filepath('Health')}")
            self._table = Table('records', MetaData(), autoload=True, autoload_with=engine)
            self._columns = self._table.columns
            self._connect = engine.connect()

    def __call__(self):
        return self.grab()

    def __del__(self):
        self.sid = engine = self._table = self._columns = self._connect = None
        del(self.sid, engine, self._table, self._columns, self._connect)

    def grab(self):
        qstr = f"SELECT date, time, sys, dia, pulse FROM records WHERE subject_id={self.sid}"
        rd = pd.read_sql(qstr, self._connect)
#         def stime(_):
#             if isinstance(_, str):
#                 try:
#                     __ = datetime.strptime(_, '%H:%M:%S').time()
#                 except:
#                     __ = datetime.strptime(_, '%H%M%S').time()
#             return __

        def comdt(_):
            try:
                __ = datetime.strptime(f'{_[0]} {_[1]}', '%Y-%m-%d %H:%M:%S.%f')
            except:
                __ = datetime.strptime(f'{_[0]} {_[1]}', '%Y-%m-%d %H:%M:%S')
            return __
        rd.index = rd.apply(comdt, axis=1)
        rd.drop(['date', 'time'], axis=1, inplace=True)
        return rd

    def append(self, *args):
        from pytz import timezone
        if args:
            hdr, largs = {'subject_id':self.sid}, len(args)
            if largs == 4:
                if isinstance(args[0], int): hdr['sys'] = args[0]
                if isinstance(args[1], int): hdr['dia'] = args[1]
                if isinstance(args[2], int): hdr['pulse'] = args[2]
                if isinstance(args[3], str): hdr['remarks'] = args[3]
                hdr['instrument_id'] = self.iid
                query = insert(self._table)
                now = datetime.now().astimezone(timezone(self.tz))
                hdr['date'], hdr['time'] = now.date(), now.time()
                self._connect.execute(query, [hdr])

    def update(self, *args):
        if args:
            largs = len(args)
            if largs == 2:
                query = update(self._table)
                if isinstance(args[0], dict):
                    hdr = args[0]
                    query = query.where(eval('and_(' + ', '.join([f"self._columns.{_}==hdr['{_}']" for _ in hdr.keys() if _ in self._columns.keys()]) + ')'))
                    return str(query)
                # if isinstance(args[1], dict):
                #     self._connect.execute(query, args[1])

def process(i, s, d, p, r):
    _ = Record(i)
    _.append(int(s), int(d), int(p), r)

if __name__ == "__main__":
    from pathlib import sys
    sid, confirm, dk = 1, 'Y', 'at ease priot to bed'
    if datetime.today().hour < 13:
        dk = 'wake up, washed before breakfast'
    while confirm.upper() != 'N':
        if sys.version_info.major == 2:
            sd = raw_input("Subject ID")
            try:
                sd = int(sd)
            except:
                sd = sid
            sy = raw_input("Systolic: ")
            dia = raw_input("Diastolic: ")
            pul = raw_input("Pulse: ")
            rmk = raw_input("Remark: ")
            if rmk == '':
                rmk = dk
            process(sd, sy, dia, pul, rmk)
            confirm = raw_input("Continue? (Y)es/(N)o: ")
        if sys.version_info.major == 3:
            sd = input(f"Subject ID (default: {sid}): ")
            try:
                sd = int(sd)
            except:
                sd = sid
            sy = input("Systolic: ")
            dia = input("Diastolic: ")
            pul = input("Pulse: ")
            rmk = input(f"Remark (default: {dk}): ")
            if rmk == '':
                rmk = dk
            process(sd, sy, dia, pul, rmk)
            confirm = input("Continue? (Y)es/(N)o: ")
