import pandas as pd
import sqlite3 as lite
from datetime import datetime
from utilities import filepath

db_table, db_name = 'records', 'Health'

class Hypertension(object):
    def __init__(self, *args, **kwargs):
        lks = list(kwargs.keys())
        self.iid, self.sid = 1, 1
        if args:
            if isinstance(args[0], str):
                try: self.sid = int(args[0])
                except: pass
            if isinstance(args[0], float): self.sid = int(args[0])
            if isinstance(args[0], int): self.sid = args[0]
            if len(args) > 1: self.iid = args[1]
        if 'subject_id' in lks: self.sid = kwargs['subject_id']
        if 'instrument_id' in lks: self.iid = kwargs['instrument_id']
        self.conn = lite.connect(filepath(db_name))
        # self.conn.row_factory = lite.Row

    def __del__(self):
        self.conn.close()
        self.uid = self.iid = self.conn = None
        del self.uid, self.iid, self.conn

    def __call__(self, *args, **kwargs):
        mode = 'append'
        if args:
            if isinstance(args[0], str):
                try: self.sid = int(args[0])
                except: pass
            if isinstance(args[0], int): self.sid = args[0]
            if len(args) > 1:
                try: sys = int(args[1])
                except: pass
            if len(args) > 2:
                try: dia = int(args[2])
                except: pass
            if len(args) > 3:
                try: pulse = int(args[3])
                except: pass
            if len(args) > 4:
                try: remarks = '{}'.format(args[4])
                except: pass
        if 'mode' in list(kwargs.keys()):
            try: mode = kwargs['mode'].lower()
            except: pass
        if mode == 'append':
            try: return self.append(sys, dia, pulse, remarks)
            except: pass

    def summary(self, *args, **kwargs):
        lks, limits = list(kwargs.keys()), {'morning':datetime(2000, 1, 1, 10, 45).time(), 'evening':datetime(2000, 1, 1, 18, 30).time()}
        if 'subject_id' in lks:
            if isinstance(kwargs['subject_id'], str):
                try: sid = int(kwargs['subject_id'])
                except: pass
            if isinstance(kwargs['subject_id'], float): sid = int(kwargs['subject_id'])
            if isinstance(kwargs['subject_id'], int): sid = kwargs['subject_id']
        if 'period' in lks:
            if isinstance(kwargs['period'], str): period = kwargs['period']
        if 'limits' in lks:
            if isinstance(kwargs['limits'], dict): limits = kwargs['limits']
        if datetime.now().time() <= limits['morning']: period = 'morning'
        if datetime.now().time() >= limits['evening']: period = 'evening'
        if args:
            if isinstance(args[0], str): period = args[0]
        if period in ['morning', 'evening']:
            tcond = "<'{:%H:%M:%S}'"
            if period == 'evening': tcond = ">'{:%H:%M:%S}'"
            tcond = tcond.format(limits[period])
        hf = ['date', 'sys', 'dia', 'pulse', 'time']
        hstr = "SELECT {} FROM {} WHERE subject_id={{:d}} AND time{{}} ORDER BY date ASC".format(', '.join(hf), db_table)
        return pd.read_sql_query(hstr.format(self.sid, tcond), self.conn, parse_dates=['date'])

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
