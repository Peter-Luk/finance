import socket
from pytz import timezone
# from scipy.optimize import newton
from datetime import datetime
from typing import Final, Any
from benedict import benedict
from pathlib import os, re, Path, sys, functools

YAML_PREFERENCE: Final[str] = 'pref.yaml'
base_path = Path(__file__).resolve().parent
PYTHON_PATH = re.split(';|:', str(base_path))
YAML = benedict.from_yaml(f"{base_path}{os.sep}settings{os.sep}{YAML_PREFERENCE}")
PORTFOLIO_PATH = f"{base_path}{os.sep}settings{os.sep}portfolio.yaml"
driver = YAML.driver
ph = YAML.public_holiday

try:
    from scipy.constants import golden_ratio as gr
except ImportError:
    gr = 1.618

today = datetime.today().astimezone(timezone('Asia/Hong_Kong'))
year, month, month_string = today.year, today.month, today.strftime('%B')
futures_type = ('HSI', 'MHI', 'HHI', 'MCH')
month_initial = dict(zip(
    [datetime(year, _, 1).strftime('%B') for _ in range(1, 13)],
    [_.upper() for _ in 'fghjkmnquvxz']))
avail_indicators = ('wma', 'kama', 'ema', 'hv')
cal_month = list(filter(lambda _: _ % 3 == 0, range(1, 13)))


def tse_stock_code(name: str) -> Any:
    _tse = YAML.prefer_stock.TSE
    _code = [_tse.get(_) for _ in _tse.keys() if name.title() in _.title()]
    if len(_code) == 1:
        return f'{_code.pop():04d}.T'


# def yaml_get(field: str, file: str) -> Any:
#     """ Basic yaml config reader """
#     if file.split('.')[-1] == 'yaml':
#         import yaml
#         fpaths = [os.getcwd()]
#         fpaths.extend(PYTHON_PATH)
#         for f_p in fpaths:
#             _f = f'{f_p}{sep}{file}'
#             if os.path.isfile(_f):
#                 with open(_f, encoding='utf-8') as y_f:
#                     _ = yaml.load(y_f, Loader=yaml.FullLoader)
#                 res = _.get(field)
#     return res


# def yaml_db_get(
#         field: str,
#         entity: str = 'Equities',
#         file: str = YAML_PREFERENCE) -> Any:
#     """ yaml db config reader """
#     _ = yaml_get('db', file)
#     return _.get(entity).get(field)


def driver_path(browser, file="pref.yaml"):
    # import yaml
    fpaths = [os.getcwd()]
    fpaths.extend(PYTHON_PATH)
    for fp in fpaths:
        _f = f'{fp}{os.sep}{file}'
        if os.path.isfile(_f):
            break
    # with open(_f, encoding='utf-8') as f:
    #     _ = yaml.load(f, Loader=yaml.FullLoader)
    # __ = _.get('driver').get(browser.capitalize())
    # __ = yaml_get('driver', file).get(browser.capitalize())
    __ = benedict.from_yaml(f"{base_path}{os.sep}settings{os.sep}{file}").driver.browser.capitalize()
    _ = []
    if sys.platform == 'win32':
        _.append(os.environ.get('HOMEPATH'))
    else:
        _.append(os.environ.get('HOME'))
    # __ = driver.get(browser.capitalize())
    _.extend(__.get('path'))
    _.append(__.get('name'))
    return os.sep.join(_)


def filepath(*args, **kwargs):
    name, file_type, data_path = args[0], 'data', 'sqlite3'
    if 'type' in list(kwargs.keys()):
        file_type = kwargs['type']
    if 'subpath' in list(kwargs.keys()):
        data_path = kwargs['subpath']
    if sys.platform == 'win32':
        if sys.version_info.major > 2 and sys.version_info.minor > 3:
            return os.sep.join((os.environ.get('HOMEPATH'), file_type, data_path, name))
        else:
            file_path = os.sep.join((os.environ.get('HOMEPATH'), file_type, data_path))
            _ = os.sep.join((file_path, name))
            return _ if os.path.exists(_) else False
    # if platform == 'linux-armv7l':file_drive, file_path = '', sep.join(('mnt', 'sdcard', file_type, data_path))
    if sys.platform in ('linux', 'linux2'):
        if sys.version_info.major > 2 and sys.version_info.minor > 3:
            if os.environ.get('EXTERNAL_STORAGE'):
                _ = os.sep.join((
                    os.environ.get('HOME'), 'storage', 'external-1', file_type,
                    data_path, name))
                if os.path.exists(_):
                    return _
            _ = os.sep.join((os.environ.get('HOME'), file_type, data_path, name))
            return _ if os.path.exists(_) else False
        else:
            place = 'shared'
            if os.environ.get('ACTUAL_HOME'):
                file_path = os.sep.join((os.environ.get('HOME'), file_type, data_path))
            elif os.environ.get('EXTERNAL_STORAGE') and ('/' in os.environ['EXTERNAL_STORAGE']):
                place = 'external-1'
                file_path = os.sep.join(
                    (os.environ.get('HOME'), 'storage', place, file_type, data_path))
            _ = os.sep.join((file_path, name))
            return _ if os.path.exists(_) else False
    else:
        path_holder_list = ['..', file_type, data_path]
        path_holder = os.sep.join(path_holder_list)
        if os.path.isfile(path_holder):
            return os.path.abspath(path_holder)
        else:
            path_holder = os.sep.join(['..', '..'] + path_holder_list)
            return os.path.abspath(path_holder) if os.path.isfile(path_holder) else False


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
            if not functools.reduce(
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
            # if month in ph[year].keys():
            #     if day not in ph[year][month]:
            if month in ph[str(year)].keys():
                if day not in ph[str(year)][str(month)]:
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

"""
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
        s_engine = db.create_engine(f"sqlite+pysqlite:///{filepath(pE['name'])}")
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
"""

def get_month(index):
    if index.upper() in month_initial.values():
        for i in list(month_initial.items()):
            if i[-1] == index.upper():
                return i[0]


def waf(delta=0):
    def dex(n=0):
        if n in range(12):
            n_month, n_year = month + n, today.year
            if n_month > 12 and n_month != n_month % 12:
                n_month, n_year = n_month % 12, n_year + 1
            _ = f"{month_initial[datetime(n_year, n_month, 1).strftime('%B')]}{f'{n_year}'[-1]}"
            return _
        else:
            print("Out of range (0 - 11)")


    futures = [''.join((f, dex(delta))) for f in futures_type[:-2]]
    futures += [''.join((f, dex(delta+1))) for f in futures_type[:-2]]
    return tuple(futures)

"""
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
        f"sqlite+pysqlite:///{filepath(pref.db['Futures']['name'])}")
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
"""

def rnd(n, decimal_place=0):
    try:
        if decimal_place:
            return round(n, decimal_place)
        elif sys.version_info.major > 2:
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

def getcode(code: Any,
        boarse: str = 'HKEx',
        source: str = 'yahoo') -> str:
    if source.lower() == 'yahoo':
        if boarse == 'HKEx' and isinstance(code, int):
            return f"{code:04}.HK"
        if boarse == 'SHE' and isinstance(code, int):
            return f"{code:06}.SS"
        if boarse == 'SZE' and isinstance(code, int):
            return f"{code:06}.SZ"
        if boarse == 'TSE' and isinstance(code, str):
            # return f"{code:04}.T"
            return tse_stock_code(code)
        if boarse == 'DAX':
            return f"{code}.DE".upper()
        if boarse == 'LSE':
            return f"{code}.L".upper()
        if boarse in ['NYSE', 'Nasdaq']:
            return code.upper()

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

def xnpv(rate, values, dates):
    min_date =min(dates)
    return sum([value / (1 + rate)**((date - min_date).days / 365) for value, date in zip(values, dates)])

def xirr(values, dates):
    try:
        from scipy.optimize import newton
        return newton(lambda r: xnpv(r, values, dates), 0)
    except ImportError:
        pass

def order_report(stock_code, price, quantity, previous, /, *, sold, equity):
    action = '買入'
    require = '需'
    def bcls(price, quantity, equity, brokerage='.25/100', ccass='3/.01/300'):
        import math
        amount = price * quantity
        b_rate, b_min = [float(_) for _ in brokerage.split('/')]
        broker = amount * b_rate / 100
        if broker < b_min:
            broker = b_min
        s_duty = math.ceil(.13 * amount / 100) if equity else 0
        t_levy = .0027 * amount / 100
        t_fee = .005 * amount / 100
        c_min, c_rate, c_max = [float(_) for _ in ccass.split('/')]
        cas = c_rate * amount / 100
        if cas < c_min:
            cas = c_min
        if cas > c_max:
            cas = c_max
        return round(broker + s_duty + t_levy + t_fee + cas, 2)

    amount = price * quantity + bcls(price, quantity, equity)
    balance = previous - amount
    if sold:
        action = '沽出'
        require ='實收'
        amount = price * quantity - bcls(price, quantity, equity)
        balance = previous + amount
    msg = f"(交收後): {balance:,.2f}" if balance > 0 else f"實需存入 {abs(balance):,.2f}"
    return f"{action} {quantity:,}股 #{stock_code} @ {price:,.3f} 連手續費及政府費用{require} {amount:,.2f}。 戶口結餘(交收前): {previous:,.2f}, {msg}。"


def push2git(file_path, msg, *, login='Peter-Luk'):
    from github import Github
    from dotenv import load_dotenv
    if load_dotenv():
        git_token = os.getenv('GHK')
        if isinstance(git_token, str):
            git = Github(git_token)
            user = git.get_user()
            if user == login:
                absolute_path = f'{str(Path.home())}{file_path}'
                if os.path.isfile(absolute_path):
                    filename = os.path.basename(absolute_path)
                    repo = os.path.dirname(absolute_path).split(os.sep)[-1]
                    req_repo = user.get_repo(repo)
                    contents = req_repo.get_contents(filename, ref="test")
                    req_repo.update_file(contents.path, msg, filename, contents.sha, branch="test")


class conditional_decorator(object):
    def __init__(self, dec, condition):
        self.decorator = dec
        self.condition = condition

    def __call__(self, func):
        if not self.condition:
            # Return the function unchanged, not decorated.
            return func
        return self.decorator(func)
