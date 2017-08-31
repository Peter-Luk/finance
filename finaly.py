db_name, db_table = 'Futures', 'records'
him = getattr(__import__('handy'), 'him')
iml = [{'utilities':('gr', 'filepath', 'mtf', 'waf'), 'statistics':('mean', 'stdev'), 'datetime':('datetime',), 'os':('sep', 'linesep')}, ({'sqlite3':()}, "alias='lite'")]
__ = him(iml)
for _ in list(__.keys()):exec("%s=__['%s']"%(_,_))

conn = lite.connect(filepath(db_name))
conn.row_factory = lite.Row

class Futures(object):
    def __init__(self, *args, **kwargs):
        self.code, self.period = args[0], 12
        if len(args) > 1: self.period = args[1]
        if 'code' in list(kwargs.keys()): self.code = kwargs['code']
        if 'period' in list(kwargs.keys()): self.period = kwargs['period']
        self.__data = conn.cursor().execute("SELECT * FROM %s WHERE code='%s' ORDER BY date ASC, session DESC" % (db_table, self.code)).fetchall()
        self.close = self.__data[-1]['close']
        self.latest = self.__data[-1]['date']
        self.trade_date = self.extract(field='date')

    def __del__(self):
        self.code = self.__data = self.period = self.close = self.trade_date = self.latest = None
        del(self.latest)
        del(self.trade_date)
        del(self.close)
        del(self.period)
        del(self.__data)
        del(self.code)

    def __bi_values(self, *args):
        res, tl = [], [args[0]]
        if len(args) > 1: tl.append(args[1])
        if len(tl[0]) == len(tl[1]):
            i = 0
            while i < len(tl[0]):
                res.append((tl[0][i], tl[1][i]))
                i += 1
            return res

    def extract(self, *args, **kwargs):
        hdr, field, programmatic, src, req_date = {}, 'close', False, self.__data, datetime.strptime(self.latest, '%Y-%m-%d')
        if args: src = args[0]
        if len(args) > 1: field = args[1]
        if 'field' in list(kwargs.keys()): field = kwargs['field']
        if 'source' in list(kwargs.keys()): src = kwargs['source']
        if 'date' in list(kwargs.keys()): req_date = datetime.strptime(kwargs['date'], '%Y-%m-%d')
        if 'programmatic' in list(kwargs.keys()): programmatic = kwargs['programmatic']
        for i in src:
            if field == 'open':
                if i['date'] in list(hdr.keys()) and i['session'] == 'M': hdr[i['date']] = i['open']
                hdr[i['date']] = i['open']
            if field == 'high':
                if i['date'] in list(hdr.keys()) and i['high'] > hdr[i['date']]: hdr[i['date']] = i['high']
                hdr[i['date']] = i['high']
            if field == 'low':
                if i['date'] in list(hdr.keys()) and i['low'] < hdr[i['date']]: hdr[i['date']] = i['low']
                hdr[i['date']] = i['low']
            if field == 'close':
                if i['date'] in list(hdr.keys()) and i['session'] == 'A': hdr[i['date']] = i['close']
                hdr[i['date']] = i['close']
            if field in ['date', 'volume']:
                if i['date'] in list(hdr.keys()) and i['session'] == 'A': hdr[i['date']] += i['volume']
                hdr[i['date']] = i['volume']
        _ = {}
        for i in list(hdr.keys()):
            if not (datetime.strptime(i, '%Y-%m-%d') > req_date): _[i] = hdr[i]
        if programmatic:
            if field == 'date': return list(_.keys())
            return _
        if field == 'date': return list(_.keys())
        return list(_.values())

    def std(self, *args, **kwargs):
        field, date = 'close', self.latest
        if args: src = args[0]
        if len(args) > 1: field = args[1]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
        if 'field' in list(kwargs.keys()): field = kwargs['field']
        if not args or ('field' in list(kwargs.keys())):
            if field in ['rsi', 'ema', 'sma', 'wma', 'kama']:
                src, i, ac = [], self.period, self.extract(date=date)
                if field == 'rsi':
                    while i < len(ac):
                        src.append(self.rsi(ac[:i+1], self.period))
                        i += 1
                if field == 'wma':
                    av = self.extract(field='volume', date=date)
                    i -= 1
                    while i < len(ac):
                        src.append(self.wma(self.__bi_values(ac[:i+1], av[:i+1]), self.period))
                        i += 1
                if field == 'ema':
                    i -= 1
                    while i < len(ac):
                        src.append(self.ema(ac[:i+1], self.period))
                        i += 1
                if field == 'sma':
                    i -= 1
                    while i < len(ac):
                        src.append(self.sma(ac[:i+1], self.period))
                        i += 1
                if field == 'kama':
                    i -= 1
                    while i < len(ac):
                        src.append(self.kama(ac[:i+1], self.period))
                        i += 1
            else: src = self.extract(field=field, date=date)
        return stdev(src)

    def mean(self, *args, **kwargs):
        field, date = 'close', self.latest
        if args: src = args[0]
        if len(args) > 1: field = args[1]
        if 'date' in list(kwargs.keys()): date = kwargs['date']
        if 'field' in list(kwargs.keys()): field = kwargs['field']
        if not args or ('field' in list(kwargs.keys())):
            if field in ['rsi', 'ema', 'sma', 'wma', 'kama']:
                src, i, ac = [], self.period, self.extract(date=date)
                if field == 'rsi':
                    while i < len(ac):
                        src.append(self.rsi(ac[:i+1], self.period))
                        i += 1
                if field == 'wma':
                    av = self.extract(field='volume', date=date)
                    i -= 1
                    while i < len(ac):
                        src.append(self.wma(self.__bi_values(ac[:i+1], av[:i+1]), self.period))
                        i += 1
                if field == 'ema':
                    i -= 1
                    while i < len(ac):
                        src.append(self.ema(ac[:i+1], self.period))
                        i += 1
                if field == 'sma':
                    i -= 1
                    while i < len(ac):
                        src.append(self.sma(ac[:i+1], self.period))
                        i += 1
                if field == 'kama':
                    i -= 1
                    while i < len(ac):
                        src.append(self.kama(ac[:i+1], self.period))
                        i += 1
            else: src = self.extract(field=field, date=date)
        return mean(src)

    def delta(self, *args):
        i, res, values = 0, [], args[0]
        while i < len(values) - 1:
            res.append(values[i + 1] - values[i])
            i += 1
        return res

    def wma(self, *args, **kwargs):
        steps, values = self.period, self.__bi_values(self.extract(), self.extract(field='volume'))
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if 'date' in list(kwargs.keys()): values = self.__bi_values(self.extract(date=kwargs['date']), self.extract(field='volume', date=kwargs['date']))
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        if len(values) >= steps:
            res, ys = [], []
            for x, y in values:
                res.append(x * y)
                ys.append(y)
            return sum(res[-steps:]) / sum(ys[-steps:])
    def kama(self, *args, **kwargs):
        steps, values = self.period, self.extract()
        fast, slow = steps, 2
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if len(args) > 2: fast = args[2]
        if len(args) > 3: slow = args[3]
        if 'date' in list(kwargs.keys()): values = self.extract(date=kwargs['date'])
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
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
        if count >= steps:
            fc = 2. / (fast + 1)
            sc = 2. / (slow + 1)
            while count > steps:
                er = (values[-1] - values[-steps]) / absum(self.delta(values[-steps:]))
                alpha = (er * (fc - sc) + sc) ** 2
                pk = self.kama(values[:-1], steps)
                return alpha * (values[-1] - pk) + pk
            return mean(values)

    def rsi(self, *args, **kwargs):
        values, steps  = self.extract(), self.period
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if 'date' in list(kwargs.keys()): values = self.extract(date=kwargs['date'])
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        dl = self.delta(values)

        def ag(*args):
            gs = i = t = 0
            values, steps = args[0], self.period
            if len(args) > 1: steps = args[1]
            while steps < len(values):
                if values[-1] > 0: t = values[-1]
                return (ag(values[:-1], steps) * (steps - 1) + t) / steps
            while i < steps:
                if values[i] > 0: gs += values[i]
                i += 1
            return gs / steps

        def al(*args):
            ls = i = t = 0
            values, steps = args[0], self.period
            if len(args) > 1: steps = args[1]
            while steps < len(values):
                if values[-1] < 0: t = abs(values[-1])
                return (al(values[:-1], steps) * (steps - 1) + t) / steps
            while i < steps:
                if values[i] < 0: ls += abs(values[i])
                i += 1
            return ls / steps

        rs = ag(dl, steps) / al(dl, steps)
        return 100 - 100 / (1 + rs)

    def ema(self, *args, **kwargs):
        steps, values = self.period, self.extract()
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if 'date' in list(kwargs.keys()): values = self.extract(date=kwargs['date'])
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        count = len(values)
        if count >= steps:
            while count > steps: return (self.ema(values[:-1], steps) * (steps - 1) + values[-1]) / steps
            return mean(values)

    def apz(self, *args, **kwargs):
        steps, cs, hs, ls = int(self.period / gr), self.extract(), self.extract(field='high'), self.extract(field='low')
        if args: steps = args[0]
        if 'date' in list(kwargs.keys()):
            cs = self.extract(date=kwargs['date'])
            hs = self.extract(field='high', date=kwargs['date'])
            ls = self.extract(field='low', date=kwargs['date'])
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        dhl = [_[0]-_[-1] for _ in self.__bi_values(hs, ls)]
        esteps, i = [], steps
        while i < len(dhl):
            esteps.append(self.ema(dhl[:i], steps))
            i += 1
        vol = self.ema(esteps, steps)
        ap = self.ema(cs, self.period)
        ubw = (gr + 1) * vol
        lbw = (gr - 1) * vol
        return ap + ubw, ap - lbw

    def atr(self, *args, **kwargs):
        def tr(*args, **kwargs):
            res, date = [], self.latest
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
        steps, values = self.period, [_[0]-_[-1] for _ in tr()]
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if 'date' in list(kwargs.keys()): values = [_[0]-_[-1] for _ in tr(kwargs['date'])]
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        count = len(values)
        if count >= steps:
            while count > steps: return (self.atr(values[:-1], steps) * (steps - 1) + values[-1]) / steps
            return mean(values)

    def kc(self, *args, **kwargs):
        steps = self.period
        if args: steps = args[0]
        ml = self.ema(steps=steps)
        if 'date' in list(kwargs.keys()):
            ml = self.ema(steps=steps, date=kwargs['date'])
            return ml + gr * self.atr(steps=int(self.period/gr), date=kwargs['date']), ml - self.atr(steps=int(self.period/gr), date=kwargs['date'])
        return ml + gr * self.atr(steps=int(self.period/gr)), ml - gr * self.atr(steps=int(self.period/gr))

    def ema(self, *args, **kwargs):
        steps, values = self.period, self.extract()
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if 'date' in list(kwargs.keys()): values = self.extract(date=kwargs['date'])
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        count = len(values)
        if count >= steps:
            while count > steps: return (self.ema(values[:-1], steps) * (steps - 1) + values[-1]) / steps
            return mean(values)

    def sma(self, *args, **kwargs):
        steps, values = self.period, self.extract()
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if 'date' in list(kwargs.keys()): values = self.extract(date=kwargs['date'])
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        if len(values) >= steps: return mean(values[-steps:])
