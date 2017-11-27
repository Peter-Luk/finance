import sqlite3 as lite
from datetime import datetime
from utilities import filepath

db_table, db_name = 'records', 'Health'

class Hypertension(object):
    def __init__(self, *args, **kwargs):
        self.iid, self.sid = 1, 1
        if args:
            lag = len(args)
            self.sid = args[0]
            if lag > 1: self.iid = args[1]
        if 'user_id' in list(kwargs.keys()): self.uid = kwargs['user_id']
        if 'instrument_id' in list(kwargs.keys()): self.iid = kwargs['instrument_id']
        self.conn = lite.connect(filepath(db_name))
        self.conn.row_factory = lite.Row

    def __del__(self):
        self.conn.close()
        self.uid = self.iid = self.conn = None
        del self.uid, self.iid, self.conn

    def append(self, *args, **kwargs):
        data = {}
        if args:
            lag = len(args)
            if lag > 3: data['remarks'] = args[3]
            if lag > 2: data['pulse'] = args[2]
            if lag > 1:
                try:
                    data['dia'] = args[1]
                    data['sys'] = args[0]
                except: pass
        try:
            lkkeys = list(kwargs.keys())
            if 'data' in lkkeys: data = kwargs['data']
            if 'condition' in lkkeys: data['remarks'] = kwargs['condition']
        except: pass
        td = datetime.today()
        date, time = td.strftime('%Y-%m-%d'), td.strftime('%H:%M:%S')
        data['date'] = date
        data['time'] = time
        data['instrument_id'] = self.iid
        data['subject_id'] = self.sid
        fields = list(data.keys())
        vstr = []
        for _ in fields:
            if _ in ['date', 'time', 'remarks']: vstr.append("'{{{}}}'".format(_))
            else: vstr.append('{{{}}}'.format(_))
        iqstr = 'INSERT INTO {} ({}) VALUES ({})'.format(db_table, ','.join(fields), ','.join(vstr))
        try:
            self.conn.cursor().execute(iqstr.format(**data))
            self.conn.commit()
        except: return False
        return True
