from sqlalchemy import create_engine, MetaData, Table, select, insert, update, and_
from datetime import datetime
from pathlib import Path, os, sys

def filepath(*args, **kwargs):
    name, file_type, data_path = args[0], 'data', 'sqlite3'
    if 'type' in list(kwargs.keys()): file_type = kwargs['type']
    if 'subpath' in list(kwargs.keys()): data_path = kwargs['subpath']
    if sys.platform == 'win32':
        if sys.version_info.major > 2 and sys.version_info.minor > 3:
            return os.sep.join((str(Path.home()), file_type, data_path, name))
        else:
            file_path = os.sep.join((str(Path.home()), file_type, data_path))
    # if sys.platform == 'linux-armv7l': file_drive, file_path = '', sep.join(('mnt', 'sdcard', file_type, data_path))
    if sys.platform in ('linux', 'linux2'):
        if sys.version_info.major > 2 and sys.version_info.minor > 3:
            if 'EXTERNAL_STORAGE' in os.environ.keys(): return os.sep.join((str(Path.home()), 'storage', 'external-1', file_type, data_path, name))
            return os.sep.join((str(Path.home()), file_type, data_path, name))
        else:
            place = 'shared'
            if 'ACTUAL_HOME' in os.environ.keys(): file_path = os.sep.join((str(Path.home()), file_type, data_path))
            elif ('EXTERNAL_STORAGE' in os.environ.keys()) and ('/' in os.environ['EXTERNAL_STORAGE']):
                place = 'external-1'
                file_path = os.sep.join((str(Path.home()), 'storage', place, file_type, data_path))
    return os.sep.join((file_path, name))

class Record(object):
    def __init__(self, sid, iid=None):
        if not iid: iid = 1
        # self.iid = iid
        if isinstance(sid, int) and isinstance(iid, int):
            self.sid, self.iid = sid, iid
            engine = create_engine(f"sqlite:///{filepath('Health')}")
            self._table = Table('records', MetaData(), autoload=True, autoload_with=engine)
            self._columns = self._table.columns
            self._connect = engine.connect()

#     def __call__(self):
#         return self.grab()

    def __del__(self):
        self.sid = engine = self._table = self._columns = self._connect = None
        del(self.sid, engine, self._table, self._columns, self._connect)

#     def grab(self):
#         qstr = f"SELECT date, time, sys, dia, pulse FROM records WHERE subject_id={self.sid}"
#         rd = pd.read_sql(qstr, self._connect)
#         def stime(_):
#             if isinstance(_, str):
#                 try:
#                     __ = datetime.strptime(_, '%H:%M:%S').time()
#                 except:
#                     __ = datetime.strptime(_, '%H%M%S').time()
#             return __
#         def comdt(_):
#             try:
#                 __ = datetime.strptime(f'{_[0]} {_[1]}', '%Y-%m-%d %H:%M:%S.%f')
#             except:
#                 __ = datetime.strptime(f'{_[0]} {_[1]}', '%Y-%m-%d %H:%M:%S')
#             return __
#         rd.index = rd.apply(comdt, axis=1)
#         rd.drop(['date', 'time'], axis=1, inplace=True)
#         return rd

    def append(self, *args):
        if args:
            hdr, largs = {'subject_id':self.sid}, len(args)
            if largs == 4:
                if isinstance(args[0], int): hdr['sys'] = args[0]
                if isinstance(args[1], int): hdr['dia'] = args[1]
                if isinstance(args[2], int): hdr['pulse'] = args[2]
                if isinstance(args[3], str): hdr['remarks'] = args[3]
                hdr['instrument_id'] = self.iid
                query = insert(self._table)
                now = datetime.now()
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

