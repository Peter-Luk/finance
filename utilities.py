import socket
from datetime import datetime
from sys import platform, version_info
from os import linesep, sep, environ
from functools import reduce
from time import sleep
#from pandas_datareader import data
import sqlite3 as lite
import fix_yahoo_finance as yf
from scipy.constants import golden_ratio as gr
# gr = 1.61803399

today = datetime.today()
year, month, month_string = today.year, today.month, today.strftime('%B')

ph = {2015:{1:(1,), 2:(19, 20), 4:(3, 6, 7), 5:(25,), 7:(1,), 9:(28,), 10:(1, 21), 12:(25,)}}
ph[2016] = {1:(1,), 2:(8, 9, 10), 3:(25, 28), 4:(4,), 5:(2,), 6:(9,), 7:(1,), 9:(16,), 10:(10,), 12:(26, 27)}
ph[2017] = {1:(2, 30, 31), 4:(4, 14, 17), 5:(1, 3, 30), 10:(2, 5), 12:(25, 26)}
ph[2018] = {1:(1,), 2:(16, 19), 3:(30,), 4:(2, 5), 5:(1, 22), 6:(18,), 7:(2,), 9:(25,), 10:(1, 17), 12:(25, 26)}
ph[2019] = {1:(1,), 2:(5, 6, 7), 4:(5, 19, 20, 22), 5:(1, 12, 13), 6:(7,), 7:(1,), 9:(14,), 10:(1, 7), 12:(25, 26)}
ph[2020] = {1:(1, 25, 27, 28), 2:(14,), 3:(20,), 4:(4, 10, 11, 12, 13, 30), 5:(1, 10), 6:(20, 21, 25), 7:(1,), 9:(22,), 10:(1, 2, 26), 12:(21, 25, 26)}
def nitem(*args):
    i, hdr = 0, []
    while i < len(args[1]):
        hdr.append(abs(args[1][i] - args[0]))
        i += 1
    return hdr.index(min(hdr))

def gslice(*args):
    if isinstance(args[0], list): lag = args[0]
    if isinstance(args[0], tuple): lag = list(args[0])
    try:
        lag.sort()
        diff = lag[-1] - lag[0]
        return [lag[0] + diff * (1 - 1 / gr), lag[0] + diff / gr]
    except: pass

def in_limit(*args, **kwargs):
    try:
        num, limits = args[0], args[1]
        if 'num' in list(kwargs.keys()):
            if isinstance(num, (int, float)): num = kwargs['num']
        if 'limits' in list(kwargs.keys()):
            if isinstance(limits, (tuple, list)): limits = kwargs['limits']
        ll = len(limits)
        if ll == 2:
            u, l = limits
            if limits[0] < limits[-1]: l, u = limits
            if num < u and num > l: return True
        return False
    except: pass

def dictfcomp(*args, **kwargs):
    res = {}
    if isinstance(args[0], dict): ad = args[0]
    if isinstance(args[1], dict): rd = args[1]
    for _ in list(rd.keys()):
        try:
            if not reduce((lambda x, y: x and y), ['{:.3f}'.format(ad[_][__]) == '{:.3f}'.format(rd[_][__]) for __ in list(rd[_].keys())]):
                res[_] = ad[_]
        except: pass
    return res

def ltd(year=year, month=month, excluded={}):
    t, ld = 0, [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if not(year % 4):ld[1] += 1
    day = ld[month - 1]
    if excluded:
        # if '%'%year in excluded.keys():
        if year in excluded.keys():
            if month in excluded[year].keys():
                if day not in excluded[year][month]:t += 1
    while t < 1:
        if datetime(year, month, day).weekday() < 5:
            if month in ph[year].keys():
                 if day not in ph[year][month]:t += 1
            else:t += 1
        day -= 1
    return day + 1

futures_type, month_initial = ('HSI', 'MHI', 'HHI', 'MCH'), {'January':'F', 'February':'G', 'March':'H', 'April':'J', 'May':'K', 'June':'M', 'July':'N', 'August':'Q', 'September':'U', 'October':'V', 'November':'X', 'December':'Z'}
avail_indicators, cal_month = ('wma','kama','ema','hv'), (3, 6, 9, 12)

def get_start(*args, **kwargs):
    end, period, mode, lk = datetime.today(), args[0], 'm', list(kwargs.keys())
    if len(args) > 1:
        if isinstance(args[1], str): mode = args[1][0].lower()
    if len(args) > 2: end = args[2]
    if 'end_date' in lk:
        if isinstance(kwargs['end_date'], datetime): end = kwargs['end_date']
        elif isinstance(kwargs['end_date'], str):
            try:
                end = datetime.strptime(kwargs['end_date'], '%Y-%m-%d')
            except: pass
    if 'mode' in lk:
        if isinstance(kwargs['mode'], str): mode = kwargs['mode'][0].lower()
    if 'period' in lk:
        if isinstance(kwargs['period'], int): period = kwargs['period']
        elif isinstance(kwargs['period'], float): period = int(kwargs['period'])
        elif isinstance(kwargs['period'], str):
            try:
                period = int(float(kwargs['period']))
            except: pass
    if mode == 'y':
        period = int(round(period * 12, 0))
        mode = 'm'
    if mode == 'm':
        y, m = end.year, end.month
        if period > 12:
            y -= int(period / 12)
            period %= 12
        if m - period <= 0:
            y -= 1
            m += 12
        m -= period
        return datetime.strptime('{}-{}-01'.format(y, m), '%Y-%m-%d')
    if mode == 'd':
        eo = end.toordinal()
        return datetime.fromordinal(eo - period)

def web_collect(*args, **kwargs):
    src, lk, period, end, efor, res = 'yahoo', list(kwargs.keys()), 7, datetime.today(), False, {}
    if args:
        if isinstance(args[0], (tuple, list)): code = list(args[0])
        if isinstance(args[0], (int, float)): code = [int(args[0])]
        if len(args) > 1:
            if isinstance(args[1], (int, float)): period = int(args[1])
    if 'code' in lk:
        if isinstance(kwargs['code'], (tuple, list)): code = list(kwargs['code'])
        if isinstance(kwargs['code'], (int, float)): code = [int(kwargs['code'])]
    if 'period' in lk:
        if isinstance(kwargs['period'], (int, float)): period = int(kwargs['period'])
    if 'source' in lk:
        if isinstance(kwargs['source'], str): src = kwargs['source']
    if 'pandas' in lk:
        if isinstance(kwargs['pandas'], bool): efor = kwargs['pandas']
    def stored_eid():
        conn = lite.connect(filepath('Securities'))
        cur = conn.cursor()
        return [__ for __ in [_[0] for _ in cur.execute("SELECT DISTINCT eid FROM records ORDER BY eid ASC").fetchall()] if __ not in [805]]
    try: code
    except: code = stored_eid()
    if src == 'yahoo':
        code = ['{:04d}.HK'.format(_) for _ in code]
    process, start = True, get_start(period)
    if start:
        while process:
            dp = yf.download(code, start, end, group_by='ticker')
            if len(dp): process = not process
            else:
                print('Retry in 10 seconds')
                sleep(10)
#        dp = data.DataReader(code, src, start, end)
        for c in code:
#            res[c] = dp.minor_xs(c).transpose().to_dict()
#            res[c] = dp[c].transpose().to_dict()
            tmp = dp[c].transpose().to_dict()
            hdr = {}
            for _ in list(tmp.keys()):
                hdr[_.to_pydatetime().date()] = tmp[_]
            res[c] = hdr
            if efor: res[c] = dp[c].transpose()
#            if efor: res[c] = dp.minor_xs(c)
        return res

def get_month(index):
    if index.upper() in month_initial.values():
        for i in list(month_initial.items()):
            if i[-1] == index.upper(): return i[0]

def dex(n=0):
    if n in range(12):
        n_month, n_year = month + n, today.year
        if n_month > 12 and n_month != n_month % 12:n_month, n_year = n_month % 12, n_year + 1
        # return month_initial[datetime(n_year, n_month, 1).strftime('%B')] + ('%i' % n_year)[-1]
        return month_initial[datetime(n_year, n_month, 1).strftime('%B')] + '{:d}'.format(n_year)[-1]
    else:print("Out of range (0 - 11)")

def waf(delta=0):
    futures = [''.join((f,dex(delta))) for f in futures_type[:-2]]
    futures += [''.join((f,dex(delta+1))) for f in futures_type[:-2]]
    return tuple(futures)

def filepath(*args, **kwargs):
    name, file_type, data_path = args[0], 'data', 'sqlite3'
    if 'type' in list(kwargs.keys()): file_type = kwargs['type']
    if 'subpath' in list(kwargs.keys()): data_path = kwargs['subpath']
    if platform == 'win32':
        file_drive, file_path = environ['HOMEDRIVE'], environ['HOMEPATH']
#         reqval = ('drive', 'path')
#         for i in reqval:
#             if i in args.keys():exec("file_%s = '%s'" % (i, args[i]))
        file_path = sep.join((file_drive, file_path, file_type, data_path))
    if platform == 'linux-armv7l':file_drive, file_path = '', sep.join(('mnt', 'sdcard', file_type, data_path))
    if platform in ('linux', 'linux2'):
        place = 'shared'
        if 'ACTUAL_HOME' in environ.keys():file_path = sep.join((environ['HOME'], file_type, data_path))
        elif ('EXTERNAL_STORAGE' in environ.keys()) and ('/' in environ['EXTERNAL_STORAGE']):
            place = 'external-1'
            file_path = sep.join((environ['HOME'], 'storage', place, file_type, data_path))
    return sep.join((file_path, name))

def mtf(*args, **kwargs):
    ftype = futures_type[:2]
    if args:
        if isinstance(args[0], str): ftype = [args[0]]
        else: ftype = list(args[0])
    if 'type' in list(kwargs.keys()):
        if isinstance(kwargs['type'], str): ftype = [kwargs['type']]
        else: ftype = list(kwargs['type'])
    fi, conn = [], lite.connect(filepath('Futures'))
    conn.row_factory = lite.Row
    qstr = "SELECT volume FROM records WHERE code={} ORDER BY date DESC"
    awaf = waf()
    if today.day == ltd(today.year, today.month): awaf = waf(1)
    for _ in ftype:
        aft = []
        # for __ in waf():
        for __ in awaf:
            if _.upper() in __: aft.append(__)
        try:
            nfv = conn.cursor().execute(qstr.format(aft[1])).fetchall()[0][0]
            cfv = conn.cursor().execute(qstr.format(aft[0])).fetchall()[0][0]
            if cfv > nfv: fi.append(aft[0])
            else:fi.append(aft[1])
        except:fi.append(aft[0])
    if len(fi) == 1: return fi.pop()
    return fi

def rnd(n, decimal_place=0):
    try:
        if decimal_place:return round(n, decimal_place)
        elif version_info.major > 2:return round(n)
        else:return int(round(n))
    except:pass

def gratio(n1, n2, ratio=None, enhanced=False):
    if not(ratio):ratio = gr
    try:
        res = []
        if n1 > n2:t = [n1, n2]
        try:
            t.reverse()
            n1, n2 = t
        except:pass
        delta = n2 - n1
        if enhanced:
            trange = [rnd(delta / ratio), rnd(delta * (1 - 1 / ratio)), -rnd(delta * (1 - 1 / ratio))]
            res += [n1 - i for i in trange]
            res += [n2 + i for i in trange]
            res.sort()
        else:
            trange = [rnd(delta * (1 - 1 / ratio)), rnd(delta / ratio)]
            res += [n1 + i for i in trange]
        return tuple(res)
    except:pass

def average(v):return sum(v) / len(v)

def product(v):
    a = 1
    for i in v:a *= i
    return a

class IP():
    def __init__(self, *args, **kwargs):
        self.mode, self.address = 'public', None
        if args:
            if isinstance(args[0], str): self.mode = args[0].lower()
        if 'mode' in list(kwargs.keys()):
            if isinstance(kwargs['mode'], str): self.mode = kwargs['mode'].lower()
        if self.mode == 'public':
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            self.address = s.getsockname()[0]

    def __del__(self):
        self.address = self.mode = None
        del self.mode, self.address

    def __call__(self):
        return self.address

def dvs(d):
    res, values = [], list(d.values())
    values.sort()
    for v in values: res.extend([i[0] for i in list(d.keys()) if d[i] == v])
    return ''.join(res)

def lique(*args):
    if isinstance(args[0], (tuple, list)): target = list(args[0])
    res = []
    while len(target):
        _ = target.pop()
        if _ not in target: res.append(_)
    res.sort()
    return res
