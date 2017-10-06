from utilities import filepath
from datetime import datetime
from statistics import mean, stdev
import sqlite3 as lite
import sys
sys.setrecursionlimit(10000)

db_name, db_table = 'Securities', 'records'

class Equities(object):
    def __init__(self, *args, **kwargs):
        self.period, self.digits = 20, -1
        self.conn = lite.connect(filepath(db_name))
        self.conn.row_factory = lite.Row
        if args:
            try:
                if isinstance(args[0], str): self.code = int(args[0])
                if isinstance(args[0], int): self.code = args[0]
            except: pass
        if len(args) > 1: self.digits = int(args[1])
        if 'code' in list(kwargs.keys()):
            try:
                if isinstance(kwargs['code'], int): self.code = kwargs['code']
                if isinstance(kwargs['code'], str): self.code = int(kwargs['code'])
            except: pass
        if 'digits' in list(kwargs.keys()): self.digits = int(kwargs['digits'])
        self.__data = self.conn.cursor().execute("SELECT * FROM %s WHERE eid=%i ORDER BY date ASC" % (db_table, self.code)).fetchall()
        self.span = self.dataspan()
        self.trade_date = [_['date'] for _ in self.span]
        self.trade_date.sort()
        self.latest = self.trade_date[-1]
        self.close = [_ for _ in self.span if _['date'] == self.trade_date[-1]][0]['close']
        self.__all_close = [_['close'] for _ in self.span]

    def __del__(self):
        self.conn.close()
        self.conn = self.span = self.code = self.period = self.digits = self.__data = self.trade_date = self.latest = self.close = self.__all_close = None
        del self.conn, self.span, self.code, self.period, self.digits, self.__data, self.trade_date, self.latest, self.close, self.__all_close

    def __nvalues(self, *args):
        """
Convert n mutable with m datas into mutable of m datas of n values.
Key-value pair not supported.
        """
        res, tl = [], [args[0]]
        i = 1
        while i < len(args):
            if len(args[i]) == len(args[i-1]): tl.append(args[i])
            i += 1
        i = 0
        while i < len(tl[0]):
            res.append(tuple([tl[_][i] for _ in range(len(args))]))
            i += 1
        return res

    def dataspan(self, *args, **kwargs):
        unit, nY = 'Year', 3
        if args:
            if isinstance(args[0], str): nY = int(args[0])
            if isinstance(args[0], int): nY = args[0]
        if 'unit' in list(kwargs.keys()): unit = kwargs['unit']
        if unit == 'Year':
            cY = datetime.today().year
            return [_ for _ in self.__data if datetime.strptime(_['Date'], '%Y-%m-%d').year > (cY - nY - 1)]

    def delta(self, *args):
        """
Helper function for difference of (integer/float) values in single dimension list.
        """
        i, res, values = 0, [], args[0]
        while i < len(values) - 1:
            res.append(values[i + 1] - values[i])
            i += 1
        return res

    def std(self, *args, **kwargs):
        field, date, period = 'close', self.latest, self.period
        if args:
            if isinstance(args[0], list): src = args[0]
            if isinstance(args[0], str): field = args[0]
        if len(args) > 1:
            if isinstance(args[1], str):
                try: date = datetime.strptime(args[1], '%Y-%m-%d').date()
                except: pass
            else: date = args[1]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str):
                try: date = datetime.strptime(kwargs['date'], '%Y-%m-%d').date()
                except: pass
            else: date = kwargs['date']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        if 'field' in list(kwargs.keys()): field = kwargs['field']
        if field in ['rsi', 'ema', 'sma', 'wma', 'kama', 'atr', 'adx']:
            src, i, ac = [], period, [_['close'] for _ in self.span]
            if field == 'rsi':
                while i < len(ac):
                    src.append(self.rsi(ac[:i+1], period))
                    i += 1
            if field == 'wma':
                av = [_['volume'] for _ in self.span if not _['date'] > date]
                i -= 1
                while i < len(ac):
                    src.append(self.wma(self.__nvalues(ac[:i+1], av[:i+1]), period))
                    i += 1
            if field == 'atr':
                i -= 1
                di = self.trade_date.index(date)
                src.extend([self.atr(self.trade_date[_], period) for _ in range(i, di)])
            if field == 'adx':
                i += 1
                di = self.trade_date.index(date)
                src.extend([self.adx(self.trade_date[_], period) for _ in range(i, di)])
            if field == 'ema':
                i -= 1
                while i < len(ac):
                    src.append(self.ema(ac[:i+1], period))
                    i += 1
            if field == 'sma':
                i -= 1
                while i < len(ac):
                    src.append(self.sma(ac[:i+1], period))
                    i += 1
            if field == 'kama':
                i -= 1
                while i < len(ac):
                    src.append(self.kama(ac[:i+1], period))
                    i += 1
        else: src = [_[field] for _ in self.span if not _['date'] > date]
        return stdev(src)

    def mean(self, *args, **kwargs):
        field, date, period = 'close', self.latest, self.period
        if args:
            if isinstance(args[0], list): src = args[0]
            if isinstance(args[0], str): field = args[0]
        if len(args) > 1:
            if isinstance(args[1], str):
                try: date = datetime.strptime(args[1], '%Y-%m-%d').date()
                except: pass
            else: date = args[1]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str):
                try: date = datetime.strptime(kwargs['date'], '%Y-%m-%d').date()
                except: pass
            else: date = kwargs['date']
        if 'field' in list(kwargs.keys()): field = kwargs['field']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        if field in ['rsi', 'ema', 'sma', 'wma', 'kama', 'atr', 'adx']:
            src, i, ac = [], period, [_['close'] for _ in self.span]
            if field == 'rsi':
                while i < len(ac):
                    src.append(self.rsi(ac[:i+1], period))
                    i += 1
            if field == 'wma':
                av = [_['volume'] for _ in self.span if not _['date'] > date]
                i -= 1
                while i < len(ac):
                    src.append(self.wma(self.__nvalues(ac[:i+1], av[:i+1]), period))
                    i += 1
            if field == 'atr':
                i -= 1
                di = self.trade_date.index(date)
                src.extend([self.atr(self.trade_date[_], period) for _ in range(i, di)])
            if field == 'adx':
                i += 1
                di = self.trade_date.index(date)
                src.extend([self.adx(self.trade_date[_], period) for _ in range(i, di)])
            if field == 'ema':
                i -= 1
                while i < len(ac):
                    src.append(self.ema(ac[:i+1], period))
                    i += 1
            if field == 'sma':
                i -= 1
                while i < len(ac):
                    src.append(self.sma(ac[:i+1], period))
                    i += 1
            if field == 'kama':
                i -= 1
                while i < len(ac):
                    src.append(self.kama(ac[:i+1], period))
                    i += 1
        else: src = [_[field] for _ in self.span if not _['date'] > date]
        return mean(src)

    def ema(self, *args, **kwargs):
        """
Exponential Moving Average
-- accept values(data series or date) as first positional variable and steps as second positional variable,
values (default: all available) on record -- optional
steps (default: period) -- optional
--> float
        """
        period, values = self.period, self.__all_close
        if args:
            if isinstance(args[0], list): values = args[0]
            elif isinstance(args[0], str):
                try: values = [_['close'] for _ in self.span if not datetime.strptime(args[0], '%Y-%m-%d').date() < _['date']]
                except: pass
            else:
                try: values = [_['close'] for _ in self.span if not args[0] < _['date']]
                except: pass
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str):
                try: values = [_['close'] for _ in self.span if not datetime.strptime(kwargs['date'], '%Y-%m-%d').date() < _['date']]
                except: pass
            else:
                try: values = [_['close'] for _ in self.span if not kwargs['date'] < _['date']]
                except: pass
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        count = len(values)
        if count >= period:
            while count > period:
                if not self.digits < 0: return round((self.ema(values[:-1], period) * (period - 1) + values[-1]) / period, self.digits)
                return (self.ema(values[:-1], period) * (period - 1) + values[-1]) / period
            if not self.digits < 0: return round(mean(values), self.digits)
            return mean(values)

    def kama(self, *args, **kwargs):
        """
Kaufman's Adaptive Moving Average
-- accept values(data series or date) as first positional variable and steps as second positional variable,
values (default: all available) on record -- optional
steps (default: period) -- optional
--> float
        """
        period, values = self.period, self.__all_close
        fast, slow = period, 2
        if args:
            if isinstance(values, list): values = args[0]
            elif isinstance(values, str):
                try: values = [_['close'] for _ in self.span if not datetime.strptime(args[0], '%Y-%m-%d').date() < _['date']]
                except: pass
            else:
                try: values = [_['close'] for _ in self.span if not args[0] < _['date']]
                except: pass
        if len(args) > 1: period = args[1]
        if len(args) > 2: fast = args[2]
        if len(args) > 3: slow = args[3]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str):
                try: values = [_['close'] for _ in self.span if not datetime.strptime(kwargs['date'], '%Y-%m-%d').date() < _['date']]
                except: pass
            else:
                try: values = [_['close'] for _ in self.span if not kwargs['date'] < _['date']]
                except: pass
            # values = self.extract(date=kwargs['date'])
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        if 'fast' in list(kwargs.keys()): fast = kwargs['fast']
        if 'slow' in list(kwargs.keys()): slow = kwargs['slow']

        def absum(*args):
            i, res, values = 0, 0, args[0]
            while i < len(values):
                if values[i] > 0: res += values[i]
                if values[i] < 0: res += -values[i]
                i += 1
            return res

        count = len(values)
        if count >= period:
            fc = 2. / (fast + 1)
            sc = 2. / (slow + 1)
            while count > period:
                er = (values[-1] - values[-period]) / absum(self.delta(values[-period:]))
                alpha = (er * (fc - sc) + sc) ** 2
                pk = self.kama(values[:-1], period)
                if not self.digits < 0: return round(alpha * (values[-1] - pk) + pk, self.digits)
                return alpha * (values[-1] - pk) + pk
            if not self.digits < 0: return round(mean(values), self.digits)
            return mean(values)

    def rsi(self, *args, **kwargs):
        """
Relative Strength Index
-- accept date and/or steps variables,
date (default: last trade date) on record -- optional
steps (default: period) -- optional
--> float
        """
        values, period  = [_['close'] for _ in self.span], self.period
        if args:
            if isinstance(args[0], list): values = args[0]
            elif isinstance(args[0], str):
                try: values = [_['close'] for _ in self.span if not datetime.strptime(args[0], '%Y-%m-%d').date() < _['date']]
                except: pass
            else:
                try: values = [_['close'] for _ in self.span if not args[0] < _['date']]
                except: pass
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str):
                try: values = [_['close'] for _ in self.span if not datetime.strptime(kwargs['date'], '%Y-%m-%d').date() < _['date']]
                except: pass
            else:
                try: values = [_['close'] for _ in self.span if not kwargs['date'] < _['date']]
                except: pass
            # values = self.extract(date=kwargs['date'])
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        dl = self.delta(values)

        def ag(*args):
            gs = i = t = 0
            values, period = args[0], self.period
            if len(args) > 1: period = args[1]
            while period < len(values):
                if values[-1] > 0: t = values[-1]
                return (ag(values[:-1], period) * (period - 1) + t) / period
            while i < period:
                if values[i] > 0: gs += values[i]
                i += 1
            return gs / period

        def al(*args):
            ls = i = t = 0
            values, period = args[0], self.period
            if len(args) > 1: period = args[1]
            while period < len(values):
                if values[-1] < 0: t = abs(values[-1])
                return (al(values[:-1], period) * (period - 1) + t) / period
            while i < period:
                if values[i] < 0: ls += abs(values[i])
                i += 1
            return ls / period

        rs = ag(dl, period) / al(dl, period)
        if not self.digits < 0: return round(100 - 100 / (1 + rs), self.digits)
        return 100 - 100 / (1 + rs)

    def sma(self, *args, **kwargs):
        """
Simple Moving Average
-- accept date and/or steps variables,
date (default: last trade date) on record -- optional
steps (default: period) -- optional
--> float
        """
        period, values = self.period, self.__all_close
        if args:
            if isinstance(args[0], list): values= args[0]
            elif isinstance(args[0], str):
                try: values = [_['close'] for _ in self.span if not datetime.strptime(args[0], '%Y-%m-%d').date() < _['date']]
                except: pass
            else:
                try: values = [_['close'] for _ in self.span if not args[0] < _['date']]
                except: pass
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str):
                try: values = [_['close'] for _ in self.span if not datetime.strptime(kwargs['date'], '%Y-%m-%d').date() < _['date']]
                except: pass
            else:
                try: values = [_['close'] for _ in self.span if not kwargs['date'] < _['date']]
                except: pass
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        if len(values) >= period:
            if not self.digits < 0: return round(mean(values[-period:]), self.digits)
            return mean(values[-period:])

    def wma(self, *args, **kwargs):
        """
Weighted Moving Average
-- accept date and/or steps variables,
date (default: last trade date) on record -- optional
steps (default: period) -- optional
--> float
        """
        steps, values = self.period, self.__all_close
        if args:
            if isinstance(args[0], list): values = args[0]
            elif isinstance(args[0], str):
                try: values = [(_['close'], _['volume']) for _ in self.span if not datetime.strptime(args[0], '%Y-%m-%d').date() < _['date']]
                except: pass
            else:
                try: values = [(_['close'], _['volume']) for _ in self.span if not args[0] < _['date']]
                except: pass
        if len(args) > 1: steps = args[1]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str):
                try: values = [(_['close'], _['volume']) for _ in self.span if not datetime.strptime(kwargs['date'], '%Y-%m-%d').date() < _['date']]
                except: pass
            else:
                try: values = [(_['close'], _['volume']) for _ in self.span if not kwargs['date'] < _['date']]
                except: pass
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        if len(values) >= steps:
            res, ys = [], []
            for x, y in values:
                res.append(x * y)
                ys.append(y)
            if not self.digits < 0: return round(sum(res[-steps:]) / sum(ys[-steps:]), self.digits)
            return sum(res[-steps:]) / sum(ys[-steps:])

    def tr(self, *args, **kwargs):
        res = []
        if args:
            if isinstance(args[0], str): date = datetime.strptime(args[0], '%Y-%m-%d').date()
            else: date = args[0]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str): date = datetime.strptime(kwargs['date'], '%Y-%m-%d').date()
            else: date = kwargs['date']
        cs, hs, ls = [_['close'] for _ in self.span if not _['date'] > date], [_['high'] for _ in self.span if not _['date'] > date], [_['low'] for _ in self.span if not _['date'] > date]
        if len(cs) == len(hs) == len(ls):
            i = 1
            res.append((hs[0], ls[0]))
            while i < len(cs):
                h, l = hs[i], ls[i]
                if cs[i-1] > h: h = cs[i-1]
                if cs[i-1] < l: l = cs[i-1]
                res.append((h, l))
                i += 1
        return res

    def __dx(self, *args, **kwargs):
        period, date = self.period, self.trade_date[-1]
        if args:
            if isinstance(args[0], str): date = datetime.strptime(args[0], '%Y-%m-%d').date()
            else: date = args[0]
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str): date = datetime.strptime(kwargs['date'], '%Y-%m-%d').date()
            else: date = kwargs['date']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        ah, al = [_['high'] for _ in self.span if not _['date'] > date], [_['low'] for _ in self.span if not _['date'] > date]
        pdm, mdm, i, tr = [], [], 1, [_[0]-_[-1] for _ in self.tr(date, period)]
        if len(ah) == len(al):
            while i < len(ah):
                th, tl = ah[i] - ah[i-1], al[i-1] - al[i]
                if th > tl: pdm.append(th)
                else: pdm.append(0)
                if tl > th: mdm.append(tl)
                else: mdm.append(0)
                i += 1

        def sdm(*args, **kwargs):
            period = self.period
            if args: src = args[0]
            if len(args) > 1: period = args[1]
            if 'source' in list(kwargs.keys()): src = kwargs['source']
            if 'period' in list(kwargs.keys()): period = kwargs['period']
            count = len(src)
            while count > period:
                return (sdm(src[:-1], period) * (period - 1) / 100 + (src[-1] / tr[-1])) / period * 100
            return mean(src) / mean(tr) * 100

        spd, smd = sdm(pdm, period), sdm(mdm, period)
        return abs(spd - smd) / (spd + smd) * 100

    def adx(self, *args, **kwargs):
        """
Average Directional indeX
        """
        period, date = self.period, self.trade_date[-1]
        if args:
            if isinstance(args[0], list): src = args[0]
            elif isinstance(args[0], str): date = datetime.strptime(args[0], '%Y-%m-%d').date()
            else: date = args[0]
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str): date = datetime(kwargs['date'], '%Y-%m-%d').date()
            else: date = kwargs['date']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        if args:
            if isinstance(args[0], str) or ('date' in list(kwargs.keys())):
                try: src = [self.__dx(_, period) for _ in self.trade_date[period:self.trade_date.index(date)]]
                except: pass
        else:
            try: src = [self.__dx(_, period) for _ in self.trade_date[period:self.trade_date.index(date)]]
            except: pass
        count = len(src)
        while count > period:
            if not self.digits < 0: return round((self.adx(src[:-1], period) * (period - 1) + src[-1]) / period, self.digits)
            return (self.adx(src[:-1], period) * (period - 1) + src[-1]) / period
        if not self.digits < 0: return round(mean(src), self.digits)
        return mean(src)
