from alchemy import mapper, load
from datetime import datetime

import pandas as pd

class FR(object): pass
db_name = 'Health'
db = load(db_name)
mapper(FR, db[db_name]['table'])
h_session = db[db_name]['session']
h_query = h_session.query(FR)

class Hyper(object):
    def __init__(self, *args):
        self.session, self.query = h_session, h_query
        if isinstance(args[0], int): self.sid = args[0]

    def __del__(self):
        self.sid = self.query = self.session = None
        del(self.sid, self.session, self.query)

    def summary(self, *args, **kwargs):
        inc_time, inc_remarks, cs = False, False, '< datetime(1900,1,1,{:d},{:d},0).time()'.format(11,45)
        if datetime.now().time() > datetime(1900,1,1,18,0,0).time(): cs = '> datetime(1900,1,1,{:d},{:d},0).time()'.format(18,0)
        if args:
            if isinstance(args[0], str):
                if args[0][0].upper() == 'E': cs = '> datetime(1900,1,1,{:d},{:d},0).time()'.format(18,0)
                if args[0][0].upper() == 'M': cs = '< datetime(1900,1,1,{:d},{:d},0).time()'.format(11,45)
        if kwargs:
            lkeys = list(kwargs.keys())
            if 'time_period' in lkeys:
                if kwargs['time_period'][0].upper() == 'E': cs = '> datetime(1900,1,1,{:d},{:d},0).time()'.format(18,0)
                if kwargs['time_period'][0].upper() == 'M': cs = '< datetime(1900,1,1,{:d},{:d},0).time()'.format(11,45)
            if 'flag' in lkeys:
                if isinstance(kwargs['flag'], dict):
                    flag = kwargs['flag']
                    try:
                        if isinstance(flag['time'], bool): inc_time = flag['time']
                    except: pass
                    try:
                        if isinstance(flag['remarks'], bool): inc_remarks = flag['remarks']
                    except: pass
        phr = self.query.filter_by(subject_id=self.sid).all()
        hdr = eval("[_ for _ in phr if _.time {}]".format(cs))
        if hdr:
            d_sys, d_dia, d_pulse, d_time, d_remarks = {}, {}, {}, {}, {}
            for __ in hdr:
                d_sys[__.date] = __.sys
                d_dia[__.date] = __.dia
                d_pulse[__.date] = __.pulse
                d_time[__.date] = __.time
                d_remarks[__.date] = __.remarks
            p_sys = pd.DataFrame.from_dict(d_sys, orient='index')
            p_sys.rename(columns={'index':'Date', 0:'sys'}, inplace=True)
            p_dia = pd.DataFrame.from_dict(d_dia, orient='index')
            p_dia.rename(columns={'index':'Date', 0:'dia'}, inplace=True)
            p_pulse = pd.DataFrame.from_dict(d_pulse, orient='index')
            p_pulse.rename(columns={'index':'Date', 0:'pulse'}, inplace=True)
            plist = [p_sys, p_dia, p_pulse]
            if inc_time:
                p_time = pd.DataFrame.from_dict(d_time, orient='index')
                p_time.rename(columns={'index':'Date', 0:'time'}, inplace=True)
                plist.append(p_time)
            if inc_remarks:
                p_remarks = pd.DataFrame.from_dict(d_remarks, orient='index')
                p_remarks.rename(columns={'index':'Date', 0:'remarks'}, inplace=True)
                plist.append(p_remarks)
            return pd.concat(plist, axis=1, join='inner', ignore_index=False)

    def append(self, *args, **kwargs):
        nr = FR()
        nr.instrument_id, nr.subject_id, nr.date = 1, self.sid, datetime.today().date()
        if args:
            largs = len(args)
            if isinstance(args[0], int): nr.sys = args[0]
            if largs > 1:
                if isinstance(args[1], int): nr.dia = args[1]
            if largs > 2:
                if isinstance(args[2], int): nr.pulse = args[2]
            if largs > 3:
                if isinstance(args[3], str): nr.remarks = args[3]
        if kwargs:
            lkeys = list(kwargs.keys())
            if 'date' in lkeys: nr.date = kwargs['date']
            if 'remarks' in lkeys:
                if isinstance(kwargs['remarks'], str): nr.remarks = kwargs['remarks']
            if 'instrument_id' in lkeys:
                if isinstance(kwargs['instrument_id'], int): nr.instrument_id = kwargs['instrument_id']
            if 'time' in lkeys: nr.time = kwargs['time']
        if not nr.time: nr.time = datetime.now().time()
        h_session.add(nr)
        h_session.commit()
        h_session.flush()
