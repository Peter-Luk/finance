db_name, db_table = 'Futures', 'records'
him = getattr(__import__('handy'), 'him')
iml = [{'utilities':('gr', 'filepath', 'mtf', 'waf', 'in_limit'), 'statistics':('mean', 'stdev'), 'datetime':('datetime',), 'os':('sep', 'linesep')}, ({'sqlite3':()}, "alias='lite'")]
__ = him(iml)
for _ in list(__.keys()):exec("%s=__['%s']" % (_, _))

class Futures(object):
    """
Primary object for local futures ('HSI', 'MHI') of current and next calendar month.
Included technical analysis (sma, ema, wma, kama, rsi, atr, adx, bb, apz, kc, stc and sar under construction).
Addition statistical method (mean, standard derivation) is provided to (sma, ema, wma, kama, rsi, atr, and adx).
Statistic range (srange) also provided for all 'addition statistical method' supported.
    """
    def __init__(self, *args, **kwargs):
        self.in_limit, self.code, self.period, self.digits, self.session, self.__field = in_limit, args[0], 12, -1, 'F', 'close'
        if len(args) > 1: self.period = args[1]
        if len(args) > 2: self.__field = args[2]
        if len(args) > 3: self.session = args[3]
        if 'code' in list(kwargs.keys()): self.code = kwargs['code']
        if 'period' in list(kwargs.keys()): self.period = kwargs['period']
        if 'session' in list(kwargs.keys()): self.session = kwargs['session']
        if 'prefer_field' in list(kwargs.keys()): self.__field = kwargs['prefer_field']
        self.conn = lite.connect(filepath(db_name))
        self.conn.row_factory = lite.Row
        self.__raw_data = self.conn.cursor().execute("SELECT * FROM %s WHERE code='%s' ORDER BY date ASC, session DESC" % (db_table, self.code)).fetchall()
        self.close = self.__raw_data[-1]['close']
        self.latest = self.__raw_data[-1]['date']
        self.__data = self.extract(field=self.__field)
        self.trade_date = self.extract(field='date')

    def __call__(self, *args, **kwargs):
        option, res = 'basic', {}
        if args: option = args[0]
        if 'option' in list(kwargs.keys()): option = kwargs['option']
        if option == 'programmatic':
            res['basic'] = {'code': self.code, 'date': self.latest, 'value': self.close}
            res['indicators'] = {'SMA': self.sma(), 'WMA': self.wma(), 'EMA': self.ema(), 'KAMA': self.kama(), 'ATR': self.atr(), 'ADX': self.adx()}
            res['overlays'] = {'STC': self.stc(), 'KC': self.kc(), 'BB': self.bb(), 'APZ': self.apz()}
            return res
        return self.code, self.latest, self.close

    def __del__(self):
        """
Standard cleanup (garbage collection) method.
        """
        self.conn.close()
        self.in_limit = self.conn = self.code = self.__raw_data = self.period = self.close = self.trade_date = self.latest = self.digits = self.session = self.__field = self.__data = None
        del self.in_limit, self.conn, self.latest, self.code, self.trade_date, self.close, self.period, self.__raw_data, self.digits, self.session, self.__field, self.__data

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

    def extract(self, *args, **kwargs):
        """
Main method to extract data from backend database.
Usage:
    First positional argument 'src' for raw data from database,
    Second positional argument 'field' for intended field (column) name (default: 'close'), and
    Third positional argument 'session' (default: '(F)ull'),
All can be override with key-value pair. Acceptable keys are 'source' for 'src' type 'list', 'field' type 'string', 'session' type 'string' (one of 'A', 'F' or 'M'), and 'programmatic' type 'boolean'.
Also in order to extract all available trade date from backend database, 'close' clause is pseudonymous used.
        """
        hdr, field, programmatic, src, session, req_date = {}, 'close', False, self.__raw_data, self.session, datetime.strptime(self.latest, '%Y-%m-%d')
        if args: src = args[0]
        if len(args) > 1: field = args[1]
        if len(args) > 2: session = args[2].upper()
        if 'field' in list(kwargs.keys()): field = kwargs['field']
        if 'source' in list(kwargs.keys()): src = kwargs['source']
        if 'date' in list(kwargs.keys()): req_date = datetime.strptime(kwargs['date'], '%Y-%m-%d')
        if 'programmatic' in list(kwargs.keys()): programmatic = kwargs['programmatic']
        if 'session' in list(kwargs.keys()): session = kwargs['session'].upper()
        for i in src:
            if session == 'F':
                if field == 'open':
                    if i['date'] in list(hdr.keys()):
                        if i['session'] == 'M': hdr[i['date']] = i['open']
                    else: hdr[i['date']] = i['open']
                if field == 'high':
                    if i['date'] in list(hdr.keys()):
                        if i['high'] > hdr[i['date']]: hdr[i['date']] = i['high']
                    else: hdr[i['date']] = i['high']
                if field == 'low':
                    if i['date'] in list(hdr.keys()):
                        if i['low'] < hdr[i['date']]: hdr[i['date']] = i['low']
                    else: hdr[i['date']] = i['low']
                if field in ['date', 'close']:
                    if i['date'] in list(hdr.keys()):
                        if i['session'] == 'A': hdr[i['date']] = i['close']
                    else: hdr[i['date']] = i['close']
                if field == 'volume':
                    if i['date'] in list(hdr.keys()):
                        if i['session'] == 'A': hdr[i['date']] += i['volume']
                    else: hdr[i['date']] = i['volume']
            if session in ['A', 'M']:
                if field == 'open':
                    if i['date'] in list(hdr.keys()):
                        if i['session'] == session: hdr[i['date']] = i['open']
                if field == 'high':
                    if i['date'] in list(hdr.keys()):
                        if i['session'] == session: hdr[i['date']] = i['high']
                if field == 'low':
                    if i['date'] in list(hdr.keys()):
                        if i['session'] == session: hdr[i['date']] = i['low']
                if field in ['date', 'close']:
                    if i['date'] in list(hdr.keys()):
                        if i['session'] == session: hdr[i['date']] = i['close']
                if field == 'volume':
                    if i['date'] in list(hdr.keys()):
                        if i['session'] == session: hdr[i['date']] = i['volume']
        _ = {}
        for i in list(hdr.keys()):
            if datetime.strptime(i, '%Y-%m-%d') <= req_date: _[i] = hdr[i]
        okeys = list(_.keys())
        okeys.sort()
        if programmatic:
            if field == 'date': return okeys
            return _
        if field == 'date': return okeys
        return [_[i] for i in okeys]

    def std(self, *args, **kwargs):
        field, date, period = 'close', self.latest, self.period
        if args:
            if isinstance(args[0], list): src = args[0]
            if isinstance(args[0], str): field = args[0]
        if len(args) > 1: date = args[1]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        if 'field' in list(kwargs.keys()): field = kwargs['field']
        if field in ['rsi', 'ema', 'sma', 'wma', 'kama', 'atr', 'adx']:
            src, i, ac = [], period, self.extract(date=date)
            if field == 'rsi':
                while i < len(ac):
                    src.append(self.rsi(ac[:i+1], period))
                    i += 1
            if field == 'wma':
                av = self.extract(field='volume', date=date)
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
        else: src = self.extract(field=field, date=date)
        return stdev(src)

    def mean(self, *args, **kwargs):
        field, date, period = 'close', self.latest, self.period
        if args:
            if isinstance(args[0], list): src = args[0]
            if isinstance(args[0], str): field = args[0]
        if len(args) > 1: date = args[1]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
        if 'field' in list(kwargs.keys()): field = kwargs['field']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        if field in ['rsi', 'ema', 'sma', 'wma', 'kama', 'atr', 'adx']:
            src, i, ac = [], period, self.extract(date=date)
            if field == 'rsi':
                while i < len(ac):
                    src.append(self.rsi(ac[:i+1], period))
                    i += 1
            if field == 'wma':
                av = self.extract(field='volume', date=date)
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
        else: src = self.extract(field=field, date=date)
        return mean(src)

    def srange(self, *args, **kwargs):
        field, date, period = 'close', self.latest, self.period
        if args: field = args[0]
        if len(args) > 1: date = args[1]
        if len(args) > 2: period = args[2]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        if 'field' in list(kwargs.keys()): field = kwargs['field']
        afield = ['open', 'high', 'low', 'close', 'volume']
        afield.extend(['rsi', 'ema', 'sma', 'wma', 'kama', 'atr', 'adx'])
        if field in afield:
            ub = eval("self.mean('%s', date='%s', period=%i) + self.std('%s', date='%s', period=%i)" % (field, date, period, field, date, period))
            lb = eval("self.mean('%s', date='%s', period=%i) - self.std('%s', date='%s', period=%i)" % (field, date, period, field, date, period))
            return ub, lb

    def delta(self, *args):
        """
Helper function for difference of (integer/float) values in single dimension list.
        """
        i, res, values = 0, [], args[0]
        while i < len(values) - 1:
            res.append(values[i + 1] - values[i])
            i += 1
        return res

    def wma(self, *args, **kwargs):
        """
Weighted Moving Average
-- accept date and/or steps variables,
date (default: last trade date) on record -- optional
steps (default: period) -- optional
--> float
        """
        steps, values = self.period, self.__nvalues(self.__data, self.extract(field='volume'))
        if args:
            if isinstance(args[0], list): values = args[0]
            if isinstance(args[0], str):
                try: values = self.__nvalues(self.extract(date=args[0]), self.extract(field='volume', date=args[0]))
                except: pass
        if len(args) > 1: steps = args[1]
        if 'date' in list(kwargs.keys()): values = self.__nvalues(self.extract(date=kwargs['date']), self.extract(field='volume', date=kwargs['date']))
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        if len(values) >= steps:
            res, ys = [], []
            for x, y in values:
                res.append(x * y)
                ys.append(y)
            if not self.digits < 0: return round(sum(res[-steps:]) / sum(ys[-steps:]), self.digits)
            return sum(res[-steps:]) / sum(ys[-steps:])

    def kama(self, *args, **kwargs):
        """
Kaufman's Adaptive Moving Average
-- accept values(data series or date) as first positional variable and steps as second positional variable,
values (default: all available) on record -- optional
steps (default: period) -- optional
--> float
        """
        period, values = self.period, self.__data
        fast, slow = period, 2
        if args:
            if isinstance(values, list): values = args[0]
            if isinstance(values, str):
                try: values = self.extract(date=args[0])
                except: pass
        if len(args) > 1: period = args[1]
        if len(args) > 2: fast = args[2]
        if len(args) > 3: slow = args[3]
        if 'date' in list(kwargs.keys()): values = self.extract(date=kwargs['date'])
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
        values, period  = self.__data, self.period
        if args:
            if isinstance(args[0], list): values = args[0]
            if isinstance(args[0], str):
                try: values = self.extract(date=args[0])
                except: pass
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()): values = self.extract(date=kwargs['date'])
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

    def ema(self, *args, **kwargs):
        """
Exponential Moving Average
-- accept values(data series or date) as first positional variable and steps as second positional variable,
values (default: all available) on record -- optional
steps (default: period) -- optional
--> float
        """
        period, values = self.period, self.__data
        if args:
            if isinstance(args[0], list): values = args[0]
            if isinstance(args[0], str):
                try: values = self.extract(date=args[0])
                except: pass
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()): values = self.extract(date=kwargs['date'])
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        count = len(values)
        if count >= period:
            while count > period:
                if not self.digits < 0: return round((self.ema(values[:-1], period) * (period - 1) + values[-1]) / period, self.digits)
                return (self.ema(values[:-1], period) * (period - 1) + values[-1]) / period
            if not self.digits < 0: return round(mean(values), self.digits)
            return mean(values)

    def apz(self, *args, **kwargs):
        """
Adaptive Price Zone
-- accept date and/or steps variables,
date (default: last trade date) on record -- optional
steps (default: period) -- optional
--> tuple
        """
        period, date = int(self.period / gr), self.latest
        if args: date = args[0]
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        cs = self.extract(date=date)
        hs = self.extract(field='high', date=date)
        ls = self.extract(field='low', date=date)
        dhl = [_[0]-_[-1] for _ in self.__nvalues(hs, ls)]
        estep, i = [], period
        while i < len(dhl):
            estep.append(self.ema(dhl[:i], period))
            i += 1
        vol = self.ema(estep, period)
        ap = self.ema(cs, self.period)
        ubw = (gr + 1) * vol
        lbw = (gr - 1) * vol
        if not self.digits < 0: return round(ap + ubw, self.digits), round(ap - lbw, self.digits)
        return ap + ubw, ap - lbw

    def tr(self, *args, **kwargs):
        res = []
        if args: date = args[0]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
        cs, hs, ls = self.extract(date=date), self.extract(field='high', date=date), self.extract(field='low', date=date)
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

    def atr(self, *args, **kwargs):
        """
Average True Range
-- accept date variables,
date (default: last trade date) on record -- optional
--> float
        """
        date, period = self.latest, self.period
        values = [x[0]-x[-1] for x in self.tr(date)]
        if args:
            if isinstance(args[0], list): values = args[0]
            if isinstance(args[0], str):
                try:
                    date = args[0]
                    values = [x[0]-x[-1] for x in self.tr(date)]
                except: pass
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()):
            date = kwargs['date']
            values = [x[0]-x[-1] for x in self.tr(date)]
        if 'period' in list(kwargs.keys()): period = kwargs['period']

        count = len(values)
        if count >= period:
            while count > period:
                if not self.digits < 0: return round((self.atr(values[:-1], period) * (period - 1) + values[-1]) / period, self.digits)
                return (self.atr(values[:-1], period) * (period - 1) + values[-1]) / period
            if not self.digits < 0: return round(mean(values), self.digits)
            return mean(values)

    def kc(self, *args, **kwargs):
        """
Keltner Channels
-- accept date and/or steps variables,
date (default: last trade date) on record -- optional
steps (default: period) -- optional
--> tuple
        """
        date, period = self.latest, self.period
        if args: date = args[0]
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        ml = self.kama(date, period)
        if not self.digits < 0: return round(ml + gr * self.atr(date, int(self.period/gr)) / 2, self.digits), round(ml - gr * self.atr(date, int(self.period/gr)) / 2, self.digits)
        return ml + gr * self.atr(date, int(self.period/gr)) / 2, ml - gr * self.atr(date, int(self.period/gr)) / 2

    def stc(self, *args, **kwargs):
        """
Stochastic Oscillator
-- accept date and/or steps variables,
date (default: last trade date) on record -- optional
steps (default: period) -- optional
--> %k and %d as tuple in respective order.
        """
        date, period = self.latest, self.period
        if args: date = args[0]
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        def pk(*args, **kwargs):
            date, period = self.latest, self.period
            if args: date = args[0]
            if len(args) > 1: period = args[1]
            if 'date' in list(kwargs.keys()): date = kwargs['date']
            if 'period' in list(kwargs.keys()): period = kwargs['period']
            hs = self.extract(field='high', date=date)
            ls = self.extract(field='low', date=date)
            lc = self.extract(date=date)[-1]
            return (lc - min(ls[-period:])) / (max(hs[-period:]) - min(ls[-period:])) * 100
        ds = self.extract(field='date', date=date)
        pks = [pk(x, period) for x in ds[-3:]]
        pd = mean(pks)
        if len(self.trade_date) >= period:
            if not self.digits < 0: return round(pks[-1], self.digits), round(pd, self.digits)
            return pks[-1], pd

    def sma(self, *args, **kwargs):
        """
Simple Moving Average
-- accept date and/or steps variables,
date (default: last trade date) on record -- optional
steps (default: period) -- optional
--> float
        """
        period, values = self.period, self.__data
        if args:
            if isinstance(args[0], list): values= args[0]
            if isinstance(args[0], str):
                try: values = self.extract(date=args[0])
                except: pass
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()): values = self.extract(date=kwargs['date'])
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        if len(values) >= period:
            if not self.digits < 0: return round(mean(values[-period:]), self.digits)
            return mean(values[-period:])

    def bb(self, *args, **kwargs):
        """
Bollinger Band
        """
        period, values = self.period, self.__data
        if args:
            if isinstance(args[0], list): values= args[0]
            if isinstance(args[0], str):
                try: values = self.extract(date=args[0])
                except: pass
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()): values = self.extract(date=kwargs['date'])
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        if not self.digits < 0: return round(self.sma(values, period) + gr / 2 * self.std(values, period=period), self.digits), round(self.sma(values, period) - gr / 2 * self.std(values, period=period), self.digits)
        return self.sma(values, period) + gr / 2 * self.std(values, period=period), self.sma(values, period) - gr / 2 * self.std(values, period=period)

    def __dx(self, *args, **kwargs):
        period, date = self.period, self.latest
        if args:
            if isinstance(args[0], str):
                date = args[0]
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        ah, al = self.extract(field='high', date=date), self.extract(field='low', date=date)
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
        period, date = self.period, self.latest
        if args:
            if isinstance(args[0], list): src = args[0]
            if isinstance(args[0], str): date = args[0]
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
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

    def sar(self, *args, **kwargs):
        period, date = self.period, self.latest
        if args:
            if isinstance(args[0], list): src = args[0]
            if isinstance(args[0], str): date = args[0]
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
        if 'period' in list(kwargs.keys()): period = kwargs['period']
        src = self.__nvalues(self.extract(field='high', date=date), self.extract(field='low', date=date), self.extract(date=date))
        hdiff, ldiff, i = self.delta([_[0] for _ in src]), self.delta([_[-1] for _ in src]), 1
        # while i < self.trade_date.index(date):
            # return self.sar(src[:-1],date=date)
            # i += 1
        if ldiff[0] > 0:
            # ep, af = src[0][0], .02
            # res = mean(src[0][:-1]) + af * (ep - mean(src[0][:-1]))
            res = src[0][-1]
            ep, af = src[0][0], .02
        elif hdiff[0] > 0:
            # ep, af = src[0][-1], .02
            # res = mean(src[0][:-1]) - af * (mean(src[0][:-1]) - ep)
            res = src[0][0]
            ep, af = src[0][-1], .02
        return res
