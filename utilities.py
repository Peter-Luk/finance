import socket
from pytz import timezone
import pref
sep, environ, linesep, platform, version_info, Path, db, gr, sleep, \
    datetime, driver, reduce, ph, subject = pref.utils

today = datetime.today().astimezone(timezone('Asia/Hong_Kong'))
year, month, month_string = today.year, today.month, today.strftime('%B')
futures_type = ('HSI', 'MHI', 'HHI', 'MCH')
month_initial = dict(zip(
    [datetime(year, _+1, 1).strftime('%B') for _ in list(range(12))],
    [_.upper() for _ in list('fghjkmnquvxz')]))
avail_indicators, cal_month = ('wma', 'kama', 'ema', 'hv'), (3, 6, 9, 12)


def driver_path(browser):
    _ = [str(Path.home())]
    __ = driver[browser.capitalize()]
    _.extend(__['path'])
    _.append(__['name'])
    return sep.join(_)


def filepath(*args, **kwargs):
    name, file_type, data_path = args[0], 'data', 'sqlite3'
    if 'type' in list(kwargs.keys()):
        file_type = kwargs['type']
    if 'subpath' in list(kwargs.keys()):
        data_path = kwargs['subpath']
    if platform == 'win32':
        if version_info.major > 2 and version_info.minor > 3:
            return sep.join((str(Path.home()), file_type, data_path, name))
        else:
            file_path = sep.join((str(Path.home()), file_type, data_path))
    # if platform == 'linux-armv7l':file_drive, file_path = '', sep.join(('mnt', 'sdcard', file_type, data_path))
    if platform in ('linux', 'linux2'):
        if version_info.major > 2 and version_info.minor > 3:
            if 'EXTERNAL_STORAGE' in environ.keys():
                return sep.join((
                    str(Path.home()), 'storage', 'external-1', file_type,
                    data_path, name))
            return sep.join((str(Path.home()), file_type, data_path, name))
        else:
            place = 'shared'
            if 'ACTUAL_HOME' in environ.keys():
                file_path = sep.join((str(Path.home()), file_type, data_path))
            elif (('EXTERNAL_STORAGE' in environ.keys())
                    and ('/' in environ['EXTERNAL_STORAGE'])):
                place = 'external-1'
                file_path = sep.join(
                    (str(Path.home()), 'storage', place, file_type, data_path))
    return sep.join((file_path, name))


def nitem(*args):
    i, hdr = 0, []
    while i < len(args[1]):
        hdr.append(abs(args[1][i] - args[0]))
        i += 1
    return hdr.index(min(hdr))


def gslice(*args):
    if isinstance(args[0], (list, tuple)):
        lag = list(args[0])
    try:
        lag.sort()
        diff = lag[-1] - lag[0]
        return [lag[0] + diff * (1 - 1 / gr), lag[0] + diff / gr]
    except Exception:
        pass


def in_limit(*args, **kwargs):
    try:
        num, limits = args[0], args[1]
        if 'num' in list(kwargs.keys()):
            if isinstance(num, (int, float)):
                num = kwargs['num']
        if 'limits' in list(kwargs.keys()):
            if isinstance(limits, (tuple, list)):
                limits = kwargs['limits']
        ll = len(limits)
        if ll == 2:
            u, _ = limits
            if limits[0] < limits[-1]:
                _, u = limits
            if num < u and num > _:
                return True
        return False
    except Exception:
        pass


def dictfcomp(*args, **kwargs):
    res = {}
    if isinstance(args[0], dict):
        ad = args[0]
    if isinstance(args[1], dict):
        rd = args[1]
    for _ in list(rd.keys()):
        try:
            if not reduce(
                (lambda x, y: x and y),
                ['{:.3f}'.format(ad[_][__]) == '{:.3f}'.format(rd[_][__])
                    for __ in list(rd[_].keys())]):
                res[_] = ad[_]
        except Exception:
            pass
    return res


def ltd(year=year, month=month, excluded={}):
    t, ld = 0, [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if not year % 4:
        ld[1] += 1
    day = ld[month - 1]
    if excluded:
        if year in excluded.keys():
            if month in excluded[year].keys():
                if day not in excluded[year][month]:
                    t += 1
    while t < 1:
        if datetime(year, month, day).weekday() < 5:
            if month in ph[year].keys():
                if day not in ph[year][month]:
                    t += 1
            else:
                t += 1
        day -= 1
    return day + 1


def get_start(*args, **kwargs):
    end, period, mode, lk = datetime.today(), args[0], 'm', list(kwargs.keys())
    if len(args) > 1:
        if isinstance(args[1], str):
            mode = args[1][0].lower()
    if len(args) > 2:
        end = args[2]
    if 'end_date' in lk:
        if isinstance(kwargs['end_date'], datetime):
            end = kwargs['end_date']
        elif isinstance(kwargs['end_date'], str):
            try:
                end = datetime.strptime(kwargs['end_date'], '%Y-%m-%d')
            except Exception:
                pass
    if 'mode' in lk:
        if isinstance(kwargs['mode'], str):
            mode = kwargs['mode'][0].lower()
    if 'period' in lk:
        if isinstance(kwargs['period'], int):
            period = kwargs['period']
        elif isinstance(kwargs['period'], float):
            period = int(kwargs['period'])
        elif isinstance(kwargs['period'], str):
            try:
                period = int(float(kwargs['period']))
            except Exception:
                pass
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
        return datetime(y, m, 1)
    if mode == 'd':
        eo = end.toordinal()
        return datetime.fromordinal(eo - period)


def web_collect(*args, **kwargs):
    import yfinance as yf
    src, lk, period, end, efor, res = 'yahoo', list(kwargs.keys()), 7, \
        datetime.today(), False, {}
    if args:
        if isinstance(args[0], (tuple, list)):
            code = list(args[0])
        if isinstance(args[0], (int, float)):
            code = [int(args[0])]
        if len(args) > 1:
            if isinstance(args[1], (int, float)):
                period = int(args[1])
    if 'code' in lk:
        if isinstance(kwargs['code'], (tuple, list)):
            code = list(kwargs['code'])
        if isinstance(kwargs['code'], (int, float)):
            code = [int(kwargs['code'])]
    if 'period' in lk:
        if isinstance(kwargs['period'], (int, float)):
            period = int(kwargs['period'])
    if 'source' in lk:
        if isinstance(kwargs['source'], str):
            src = kwargs['source']
    if 'pandas' in lk:
        if isinstance(kwargs['pandas'], bool):
            efor = kwargs['pandas']

    def stored_eid():
        pE = pref.db['Equities']
        s_engine = db.create_engine(f"sqlite:///{filepath(pE['name'])}")
        s_conn = s_engine.connect()
        rc = db.Table(
            pE['table'],
            db.MetaData(),
            autoload=True,
            autoload_with=s_engine).columns
        query = db.select([rc.eid.distinct()]).order_by(db.asc(rc.eid))
        return [__ for __ in [_[0] for _ in s_conn.execute(query).fetchall()]
                if __ not in pE['exclude']]

    try:
        code
    except Exception:
        code = stored_eid()
    if src == 'yahoo':
        code = [f'{_:04d}.HK' for _ in code]
    process, start = True, get_start(period)
    if start:
        while process:
            dp = yf.download(code, start, end, group_by='ticker')
            if len(dp):
                process = not process
            else:
                print('Retry in 25 seconds')
                sleep(25)
        # dp = data.DataReader(code, src, start, end)
        for c in code:
            # res[c] = dp.minor_xs(c).transpose().to_dict()
            # res[c] = dp[c].transpose().to_dict()
            tmp = dp[c].transpose().to_dict()
            hdr = {}
            for _ in list(tmp.keys()):
                hdr[_.to_pydatetime().date()] = tmp[_]
            res[c] = hdr
            if efor:
                res[c] = dp[c]
            # if efor: res[c] = dp.minor_xs(c)
        return res


def get_month(index):
    if index.upper() in month_initial.values():
        for i in list(month_initial.items()):
            if i[-1] == index.upper():
                return i[0]


def dex(n=0):
    if n in range(12):
        n_month, n_year = month + n, today.year
        if n_month > 12 and n_month != n_month % 12:
            n_month, n_year = n_month % 12, n_year + 1
        _ = f"{month_initial[datetime(n_year, n_month, 1).strftime('%B')]}{f'{n_year}'[-1]}"
        return _
    else:
        print("Out of range (0 - 11)")


def waf(delta=0):
    futures = [''.join((f, dex(delta))) for f in futures_type[:-2]]
    futures += [''.join((f, dex(delta+1))) for f in futures_type[:-2]]
    return tuple(futures)


def mtf(*args, **kwargs):
    ftype = futures_type[:2]
    if args:
        if isinstance(args[0], str):
            ftype = [args[0]]
        else:
            ftype = list(args[0])
    if 'type' in list(kwargs.keys()):
        if isinstance(kwargs['type'], str):
            ftype = [kwargs['type']]
        else:
            ftype = list(kwargs['type'])
    fi = []
    f_engine = db.create_engine(
        f"sqlite:///{filepath(pref.db['Futures']['name'])}")
    f_conn = f_engine.connect()
    rc = db.Table(
        pref.db['Futures']['table'],
        db.MetaData(),
        autoload=True,
        autoload_with=f_engine).columns
    query = db.select([rc.volume]).order_by(db.desc(rc.date))
    awaf = waf()
    if today.day == ltd(today.year, today.month):
        awaf = waf(1)
    for _ in ftype:
        aft = []
        for __ in awaf:
            if _.upper() in __:
                aft.append(__)
        try:
            nfv = f_conn.execute(query.where(rc.code == aft[1])).fetchall()[0]
            cfv = f_conn.execute(query.where(rc.code == aft[0])).fetchall()[0]
            if cfv > nfv:
                fi.append(aft[0])
            else:
                fi.append(aft[1])
        except Exception:
            fi.append(aft[0])
    if len(fi) == 1:
        return fi.pop()
    return fi


def rnd(n, decimal_place=0):
    try:
        if decimal_place:
            return round(n, decimal_place)
        elif version_info.major > 2:
            return round(n)
        else:
            return int(round(n))
    except Exception:
        pass


def gratio(n1, n2, ratio=None, enhanced=False):
    if not ratio:
        ratio = gr
    try:
        res = []
        if n1 > n2:
            t = [n1, n2]
        try:
            t.reverse()
            n1, n2 = t
        except Exception:
            pass
        delta = n2 - n1
        if enhanced:
            trange = [
                    rnd(delta / ratio),
                    rnd(delta * (1 - 1 / ratio)),
                    -rnd(delta * (1 - 1 / ratio))]
            res += [n1 - i for i in trange]
            res += [n2 + i for i in trange]
            res.sort()
        else:
            trange = [rnd(delta * (1 - 1 / ratio)), rnd(delta / ratio)]
            res += [n1 + i for i in trange]
        return tuple(res)
    except Exception:
        pass


def average(v):
    return sum(v) / len(v)


def product(v):
    a = 1
    for i in v:
        a *= i
    return a


class IP():

    def __init__(self, *args, **kwargs):
        self.mode, self.address = 'public', None
        if args:
            if isinstance(args[0], str):
                self.mode = args[0].lower()
        if 'mode' in list(kwargs.keys()):
            if isinstance(kwargs['mode'], str):
                self.mode = kwargs['mode'].lower()
        if self.mode == 'public':
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            self.address = s.getsockname()[0]

    def __del__(self):
        self.address = self.mode = None
        del self.mode, self.address

    def __str__(self):
        return self.address

def getcode(code, boarse='HKEx', type='yahoo'):
    if type == 'yahoo':
        if boarse == 'HKEx' and isinstance(code, int):
            return f"{code:04}.HK"
        if boarse == 'TSE' and isinstance(code, int):
            return f"{code:04}.T"
        if boarse == 'LSE' and isinstance(code, int):
            return f"{code}.L"
        if boarse in ['Dow', 'Nasdaq']:
            return code

def dvs(d):
    res, values = [], list(d.values())
    values.sort()
    for v in values:
        res.extend([i[0] for i in list(d.keys()) if d[i] == v])
    return ''.join(res)


def lique(*args):
    if isinstance(args[0], (tuple, list)):
        target = list(args[0])
    res = []
    while len(target):
        _ = target.pop()
        if _ not in target:
            res.append(_)
    res.sort()
    return res
