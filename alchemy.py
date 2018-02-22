from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker
from os import sep, environ, listdir
from sys import platform
from utilities import datetime, mtf, gr
from statistics import mean

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
    db = load(db_name)
    return [_[0] for _ in db[db_name]['engine'].execute("SELECT DISTINCT eid FROM records ORDER BY eid ASC").fetchall()]

def aquery(*args):
    db = load(args[0])
    class FR(object): pass
    mapper(FR, db[args[0]]['table'])
    return db[args[0]]['session'].query(FR)

avail_eid = get_stored_eid()
sqa = aquery('Securities')
fqa = aquery('Futures')
hqa = aquery('Health')

def hm(*args, **kwargs):
    cs = '< datetime(1900,1,1,{:d},{:d},0).time()'.format(11,45)
    if datetime.now().time() > datetime(1900,1,1,18,0,0).time(): cs = '> datetime(1900,1,1,{0:d},{0:d},0).time()'.format(18,0)
    if isinstance(args[0], int): sid = args[0]
    if 'time_period' in list(kwargs.keys()):
        if kwargs['time_period'][0].upper() == 'E': cs = '> datetime(1900,1,1,{:d},{:d},0).time()'.format(18,0)
        if kwargs['time_period'][0].upper() == 'M': cs = '< datetime(1900,1,1,{:d},{:d},0).time()'.format(11,45)
    phr = hqa.filter_by(subject_id=sid).all()
    hdr = eval("[_ for _ in phr if _.time {}]".format(cs))
    return hdr

def daily(*args):
    if args:
        if isinstance(args[0], str): code = args[0]
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

class Danta(object):
    def __init__(self, *args, **kwargs):
        self.data, self.__max_n = [], 500
        if 'max_n' in list(kwargs.keys()): self.__max_n = kwargs['max_n']
        if args[0] in avail_eid:
            self.data = sqa.filter_by(eid=args[0]).all()
            if len(self.data) > self.__max_n: self.data = self.data[-self.__max_n:]
        else:
            try: self.data = daily(args[0])
            except: pass

    def __del__(self):
        self.__max_n = self.data = None
        del(self.__max_n, self.data)

    def sma(self, *args):
        data, period = self.data, 20
        if args:
            if isinstance(args[0], list): data = args[0]
            if len(args) > 1:
                if isinstance(args[1], int): period = args[1]
                if isinstance(args[1], float): period = int(args[1])
        if not (len(data) > period): return mean([_.close for _ in data])
        return mean([_.close for _ in data[-period:]])

    def wma(self, *args):
        data, period = self.data, 20
        if args:
            if isinstance(args[0], list): data = args[0]
            if len(args) > 1:
                if isinstance(args[1], int): period = args[1]
                if isinstance(args[1], float): period = int(args[1])
        if not (len(data) > period): return sum([_.close * _.volume for _ in data]) / sum([_.volume for _ in data])
        return sum([_.close * _.volume for _ in data[-period:]]) / sum([_.volume for _ in data[-period:]])

    def rsi(self, *args):
        data, period = self.data, 14
        if args:
            if isinstance(args[0], list): data = args[0]
            if len(args) > 1:
                if isinstance(args[1], int): period = args[1]
                if isinstance(args[1], float): period = int(args[1])
        def delta(*args):
            i, hdr = 1, []
            while i < len(args[0]):
                hdr.append(args[0][i].close - args[0][i - 1].close)
                i += 1
            return hdr
        i = period
        while i < len(data):
            if i == period:
                ag = sum([_ for _ in delta(data[i-period:i]) if _ > 0]) / period
                al = sum([abs(_) for _ in delta(data[i-period:i]) if _ < 0]) / period
            else:
                cdelta = data[i].close - data[i-1].close
                if cdelta > 0:
                    ag = (cdelta + ag * (period - 1)) / period
                    al = al * (period - 1) / period
                else:
                    ag = ag * (period - 1) / period
                    al = (abs(cdelta) + al * (period - 1)) / period
            i += 1
        try: return 100 - 100 / (1 + ag / al)
        except: pass

    def ema(self, *args):
        data, numtype, period = self.data, False, 20
        if args:
            if isinstance(args[0], list):
                if isinstance(args[0][0], int): numtype = True
                if isinstance(args[0][0], float): numtype = True
                data = args[0]
            if len(args) > 1:
                if isinstance(args[1], int): period = args[1]
                if isinstance(args[1], float): period = int(args[1])
        if not (len(data) > period):
            if numtype: return mean(data)
            return mean([_.close for _ in data])
        if numtype: res = mean(data[:period])
        else: res = mean([_.close for _ in data[:period]])
        i = period
        while i < len(data):
            if numtype: res = (data[i] + res * (period - 1)) / period
            else: res = (data[i].close + res * (period - 1)) / period
            i += 1
        return res

    def kama(self, *args):
        data, period, fast, slow = self.data, 10, 2, 30
        if args:
            if isinstance(args[0], list): data = args[0]
            if len(args) > 1:
                if isinstance(args[1], int): period = args[1]
                if isinstance(args[1], float): period = int(args[1])
                try:
                    if isinstance(args[2], int): fast = args[2]
                    if isinstance(args[2], float): fast = int(args[2])
                except: pass
                try:
                    if isinstance(args[3], int): slow = args[3]
                    if isinstance(args[3], float): slow = int(args[3])
                except: pass
        def asum(*args):
            i, hdr = 1, []
            while i < len(args[0]):
                hdr.append(args[0][i] - args[0][i - 1])
                i += 1
            return sum([abs(_) for _ in hdr])
        if not (len(data) > period): return mean([_.close for _ in data])
        res, i, fc, sc = mean([_.close for _ in data[:period]]), period, 2 / (fast + 1), 2 / (slow + 1)
        while i < len(data):
            volatility = asum([_.close for _ in data[i-period:i]])
            if volatility:
                er = (data[i].close - data[i-period].close) / volatility
                alpha = (er * (fc - sc) + sc) ** 2
                res += alpha * (data[i].close - res)
            i += 1
        return res

    def apz(self, *args):
        data, period = self.data, 5
        if args:
            if isinstance(args[0], list): data = args[0]
            if len(args) > 1:
                if isinstance(args[1], int): period = args[1]
                if isinstance(args[1], float): period = int(args[1])
        vl = [_.high - _.low for _ in data]
        i, vpl, epl = period, [], []
        while i < len(data):
            vpl.append(self.ema(vl[:i], period))
            epl.append(self.ema(data[:i], period))
            i += 1
        evp, eep = self.ema(vpl, period), self.ema(epl, period)
        return [eep + evp, eep - evp]

    def __tr(self, *args):
        i, res = 0, []
        while i < len(args[0]):
            gap = False
            if i == 0: res.append(args[0][i].high - args[0][i].low)
            if i > 0:
                if args[0][i-1].close > args[0][i].high:
                    gap = True
                    res.append(args[0][i-1].close - args[0][i].low)
                if args[0][i-1].close < args[0][i].low:
                    gap = True
                    res.append(args[0][i].high - args[0][i-1].close)
                if not gap: res.append(args[0][i].high - args[0][i].low)
            i += 1
        return res

    def __dm(self, *args):
        i, res = 1, []
        if args:
            if args[1] == '+':
                while i < len(args[0]):
                    value = 0
                    if args[0][i].high - args[0][i-1].high > args[0][i-1].low -args[0][i].low:
                        if args[0][i].high > args[0][i-1].high:
                            value = args[0][i].high - args[0][i-1].high
                    res.append(value)
                    i += 1
            if args[1] == '-':
                while i < len(args[0]):
                    value = 0
                    if args[0][i-1].low - args[0][i].low > args[0][i].high -args[0][i-1].high:
                        if args[0][i-1].low > args[0][i].low:
                            value = args[0][i-1].low - args[0][i].low
                    res.append(value)
                    i += 1
        return res

    def adx(self, *args):
        data, period = self.data, 14
        if args:
            if isinstance(args[0], list): data = args[0]
            if len(args) > 1:
                if isinstance(args[1], int): period = args[1]
                if isinstance(args[1], float): period = int(args[1])
        i, trd, dmp, dmm = period, self.__tr(data), self.__dm(data, '+'), self.__dm(data, '-')
        while i < len(data):
            if i == period:
                trs = mean(trd[:i])
                dmps = mean(dmp[:i-1])
                dmms = mean(dmm[:i-1])
                dip = dmps / trs
                dim = dmms / trs
                dx = abs(dip - dim) / (dip + dim) * 100
                res = dx
            else:
                trs = (trs * (period - 1) + trd[i]) / period
                dmps = (dmps * (period - 1) + dmp[i-1]) / period
                dmms = (dmms * (period - 1) + dmm[i-1]) / period
                dip = dmps / trs
                dim = dmms / trs
                dx = abs(dip - dim) / (dip + dim) * 100
                res = (res * (period - 1) + dx) / period
            i += 1
        return res

    def atr(self, *args):
        data, period = self.data, 14
        if args:
            if isinstance(args[0], list): data = args[0]
            if len(args) > 1:
                if isinstance(args[1], int): period = args[1]
                if isinstance(args[1], float): period = int(args[1])
        i, trd = period, self.__tr(data)
        while i < len(data):
            if i == period: res = mean(trd[:i])
            else: res = (trd[i] + res * (period - 1)) / period
            i += 1
        return res

    def kc(self, *args):
        data, ma_period, tr_period = self.data, 20, 10
        if args:
            if isinstance(args[0], list): data = args[0]
            if len(args) > 1:
                if isinstance(args[1], list): ma_period, tr_period = args[1]
                if isinstance(args[1], tuple): ma_period, tr_period = list(args[1])
        axis, delta = self.kama(data, ma_period), self.atr(data, tr_period)
        return [axis + gr * delta, axis - gr * delta]

    def stc(self, *args):
        data, period = self.data, 14
        if args:
            if isinstance(args[0], list): data = args[0]
            if len(args) > 1:
                if isinstance(args[1], int): period = args[1]
                if isinstance(args[1], float): period = int(args[1])
        def pk(*args):
            data, period = args[0], args[1]
            pma, pmi = max([_.high for _ in data[-period:]]), min([_.low for _ in data[-period:]])
            return (data[-1].close - pmi) / (pma - pmi) * 100
        return [pk(data, period), mean([pk(data, period), pk(data[:-1], period), pk(data[:-2], period)])]

    def macd(self, *args):
        data, mf_period, ms_period, s_period = self.data, 12, 26, 9
        if args:
            if isinstance(args[0], list): data = args[0]
            if len(args) > 1:
                if isinstance(args[1], int): s_period = args[1]
                if isinstance(args[1], float): s_period = int(args[1])
        mfl, msl, ml = [], [], []
        i = ms_period
        while i < len(data):
            mfl.append(self.ema(data[:i], mf_period))
            msl.append(self.ema(data[:i], ms_period))
            i += 1
        i = 0
        while i < (len(data) - ms_period):
            ml.append(mfl[i] - msl[i])
            i += 1
        s = self.ema(ml, s_period)
        return ml[-1], s, ml[-1] - s
