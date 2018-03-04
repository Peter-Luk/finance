from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker
from os import sep, environ, listdir
from sys import platform
from utilities import datetime, nitem, gslice, mtf, gr
from statistics import mean

import pandas as pd

if platform == 'win32': home = (''.join([environ['HOMEDRIVE'], environ['HOMEPATH']]))
if platform in ['linux', 'linux2']:
    subpath = 'shared'
    try:
        ext = environ['EXTERNAL_STORAGE']
        subpath = 'external-1'
    except: pass
    home = sep.join([environ['HOME'], 'storage', subpath])

class FD(object):
    def __init__(self, *args):
        self.date = None
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.volume = 0
        if args:
            if isinstance(args[0], dict):
                self.date = list(args[0].keys())[0]
                self.open = args[0][self.date]['open']
                self.high = args[0][self.date]['high']
                self.low = args[0][self.date]['low']
                self.close = args[0][self.date]['close']
                self.volume = args[0][self.date]['volume']

    def __del__(self):
        self.date = self.open = self.high = self.low = self.close = self.volume = None
        del(self.date, self.open, self.high, self.low, self.close, self.volume)

    def __call__(self):
        hdr = {'open': self.open}
        hdr['high'] = self.high
        hdr['low'] = self.low
        hdr['close'] = self.close
        hdr['volume'] = self.volume
        hdr['date'] = self.date
        return hdr

def get_db(*args, **kwargs):
    if isinstance(args[0], str): db_path = sep.join([home, args[0]])
    if isinstance(args[0], tuple): db_path = sep.join((home,) + args[0])
    if isinstance(args[0], list):
        args[0].insert(0, home)
        db_path = sep.join(args[0])
    if kwargs:
        try:
            if isinstance(kwargs['exclude'], str): exclude = [kwargs['exclude']]
            if isinstance(kwargs['exclude'], tuple): exclude = list(kwargs['exclude'])
            if isinstance(kwargs['exclude'], list): exclude = kwargs['exclude']
            if exclude: return [f for f in listdir(db_path) if not ('.git' in f or f in exclude)]
        except: pass
    return [f for f in listdir(db_path) if '.git' not in f]

def load(*args):
    res, db_path, exclude = {}, ('data','sqlite3'), 'Sec12'
    db_name = get_db(db_path, exclude=exclude)
    if args:
        if isinstance(args[0], str): db_name = [args[0]]
        if isinstance(args[0], list): db_name = args[0]
        if isinstance(args[0], tuple): db_name = list(args[0])
    for dbn in db_name:
        exec('class {}(object): pass'.format(dbn))
        engine = create_engine('sqlite:///{}'.format(sep.join((home,) + db_path + (dbn,))))
        metadata = MetaData(engine)
        records = Table('records', metadata, autoload=True)
        eval('mapper({}, records)'.format(dbn))
        Session = sessionmaker(bind=engine)
        session = Session()
        if dbn in ['Futures', 'Securities']:
            ifs = ['date', 'open', 'high', 'low', 'close', 'volume']
            if dbn == 'Futures':
                [ifs.insert(0, _) for _ in ['session', 'code']]
            else:
                [ifs.insert(0, _) for _ in ['eid']]
        else: ifs = ['subject_id', 'date', 'time', 'sys', 'dia', 'pulse', 'remarks']
        ifs = ['.'.join([dbn, _]) for _ in ifs]
        data = eval("session.query({}).all()".format(','.join(ifs)))
        res[dbn] = {'engine':engine, 'table':records, 'session':session, 'data':data}
    return res

def get_stored_eid(*args):
    db_name = 'Securities'
    if args:
        if isinstance(args[0], str): db_name = args[0]
    class SD(object): pass
    db = load(db_name)
    mapper(SD, db[db_name]['table'])
    aid = [_[0] for _ in db[db_name]['session'].query(SD.eid).distinct()]
    aid.sort()
    return aid

def query(*args):
    db = load(args[0])
    class FR(object): pass
    mapper(FR, db[args[0]]['table'])
    return db[args[0]]['session'].query(FR)

avail_eid = get_stored_eid()
sqa = query('Securities')

def daily(*args):
    if args:
        if isinstance(args[0], str): code = args[0]
    fqa = query('Futures')
    td, res = [], []
    try:
        rfd = fqa.filter_by(code=code).all()
        for _ in rfd:
            if _.date not in td: td.append(_.date)
        for _ in td:
            tmp = {}
            itd = fqa.filter_by(code=code, date=_).all()
            if len(itd) == 2:
                volume, open, close, high, low = 0, 0, 0, 0, 0
                for __ in itd:
                    if __.session == 'A':
                        if __.high > high: high = __.high
                        if __.low < low: low = __.low
                        close = __.close
                        volume += __.volume
                    else:
                        open, high, low, close = __.open, __.high, __.low, __.close
                        volume += __.volume
            if len(itd) == 1:
                volume, open, high, low, close = itd[0].volume, itd[0].open, itd[0].high, itd[0].low, itd[0].close
            tmp[_] = {'open':open, 'high':high, 'low':low, 'close':close, 'volume':volume}
            res.append(FD(tmp))
    except: pass
    return res
