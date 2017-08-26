db_name, db_table = 'Futures', 'records'
him = getattr(__import__('handy'), 'him')
iml = [{'utilities':('gr', 'filepath', 'mtf', 'waf'), 'statistics':('mean', 'stdev'), 'os':('sep', 'linesep')}, ({'sqlite3':()}, "alias='lite'")]
__ = him(iml)
for _ in list(__.keys()):exec("%s=__['%s']"%(_,_))

conn = lite.connect(filepath(db_name))
conn.row_factory = lite.Row

class Futures(object):
    def __init__(self, *args, **kwargs):
        self.code = args[0]
        if 'code' in list(kwargs.keys()): self.code = kwargs['code']
        self.__data = conn.cursor().execute("SELECT * FROM %s WHERE code='%s' ORDER BY date ASC, session DESC" % (db_table, self.code)).fetchall()

    def __del__(self):
        self.code = self.__data = None
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
        hdr, field, programmatic, src = {}, 'close', False, self.__data
        if args: args[0]
        if len(args) > 1: field = args[1]
        if 'field' in list(kwargs.keys()): field = kwargs['field']
        if 'source' in list(kwargs.keys()): src = kwargs['source']
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
            if field == 'volume':
                if i['date'] in list(hdr.keys()) and i['session'] == 'A': hdr[i['date']] += i['volume']
                hdr[i['date']] = i['volume']
        if programmatic: return hdr
        return list(hdr.values())

    def delta(self, *args):
        i, res, values = 0, [], args[0]
        while i < len(values) - 1:
            res.append(values[i + 1] - values[i])
            i += 1
        return res

    def wma(self, *args, **kwargs):
        steps, values = 12, self.__bi_values(self.extract(), self.extract(field='volume'))
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        if len(values) >= steps:
            res, ys = [], []
            for x, y in values:
                res.append(x * y)
                ys.append(y)
            return sum(res[-steps:]) / sum(ys[-steps:])
    def kama(self, *args, **kwargs):
        steps, values = 12, self.extract()
        fast, slow = steps, 2
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if len(args) > 2: fast = args[2]
        if len(args) > 3: slow = args[3]
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
            return sum(values) / steps

    def rsi(self, *args, **kwargs):
        values, steps  = self.extract(), 12
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        dl = self.delta(values)

        def ag(*args):
            gs = i = t = 0
            values, steps = args[0], 12
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
            values, steps = args[0], 12
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
        steps, values = 12, self.extract()
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        count = len(values)
        if count >= steps:
            while count > steps: return (self.ema(values[:-1], steps) * (steps - 1) + values[-1]) / steps
            return sum(values) / steps

    def sma(self, *args, **kwargs):
        steps, values = 12, self.extract()
        if args: values = args[0]
        if len(args) > 1: steps = args[1]
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        if len(values) >= steps: return sum(values[-steps:]) / steps
