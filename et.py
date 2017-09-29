from utilities import filepath
from datetime import datetime
from sqlite3 import connect
from statistics import mean

class Equities(object):
    def __init__(self, *args, **kwargs):
        self.period, self.digits = 20, 2
        if args: self.code = args[0]
        if len(args) > 1: self.digits = int(args[1])
        if 'code' in list(kwargs.keys()): self.code = kwargs['code']
        if 'digits' in list(kwargs.keys()): self.digits = int(kwargs['digits'])
        self.__data = self.get(self.code, self.digits)
        self.trade_date = [_['Date'] for _ in self.__data]
        self.trade_date.sort()
        self.latest = self.trade_date[-1].strftime('%d-%m-%Y')
        self.close = [_ for _ in self.__data if _['Date'] == self.trade_date[-1]][0]['Close']

    def __del__(self):
        self.code = self.period = self.digits = self.__data = self.trade_date = self.latest = self.close = None
        del self.code, self.period, self.digits, self.__data, self.trade_date, self.latest, self.close

    def get(self, *args, **kwargs):
        digits = 2
        if 'code' in list(kwargs.keys()): self.code = kwargs['code']
        if args: code = args[0]
        if len(args) > 1: digits = int(args[1])
        if 'code' in list(kwargs.keys()): code = kwargs['code']
        if 'digits' in list(kwargs.keys()): digits = int(kwargs['digits'])
        tmp = open(filepath('.'.join((code, 'csv')), subpath='csv'))
        data = [_[:-1] for _ in tmp.readlines()]
        tmp.close()
        fields = data[0].split(',')
        i, values = 1, []
        while i < len(data):
            tmp = data[i].split(',')
            try:
                tmp[0] = datetime.strptime(tmp[0], '%Y-%m-%d').date()
                tmp[-1] = int(tmp[-1])
                j = 1
                while j < len(tmp) - 1:
                    tmp[j] = round(float(tmp[j]), digits)
                    j += 1
                values.append(tmp)
            except: pass
            i += 1
        el  = ['Adj Close']
        rfl = [_ for _ in fields if _ not in el]
        i, tmp  = 0, []
        while i < len(values):
            hdr = {}
            for _ in rfl:
                j = fields.index(_)
                hdr[fields[j]] = values[i][j]
            tmp.append(hdr)
            i += 1
        return tmp

    def put(self, *args, **kwargs):
        data_type = 'sql'
        if args: table_name = args[0]
        if len(args) > 1: data_type = args[1]
        if 'name' in list(kwargs.keys()): table_name = kwargs['name']
        if 'type' in list(kwargs.keys()): data_type = kwargs['type']
        i = 0
        while i < len(self.__data):
            sqstr = "INSERT INTO %s (%s) VALUES" % (table_name, ','.join(list(self.__data[i].keys())))
            i += 1

    def sma(self, *args, **kwargs):
        """
Simple Moving Average
-- accept date and/or steps variables,
date (default: last trade date) on record -- optional
steps (default: period) -- optional
--> float
        """
        period, values = self.period, [_['Close'] for _ in self.__data]
        if args:
            if isinstance(args[0], list): values= args[0]
            elif isinstance(args[0], str):
                try: values = [_['Close'] for _ in self.__data if not datetime.strptime(args[0], '%Y-%m-%d').date() < _['Date']]
                except: pass
            else:
                try: values = [_['Close'] for _ in self.__data if not args[0] < _['Date']]
                except: pass
        if len(args) > 1: period = args[1]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str):
                try: values = [_['Close'] for _ in self.__data if not datetime.strptime(kwargs['date'], '%Y-%m-%d').date() < _['Date']]
                except: pass
            else:
                try: values = [_['Close'] for _ in self.__data if not kwargs['date'] < _['Date']]
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
        steps, values = self.period, [(_['Close'], _['Volume']) for _ in self.__data]
        if args:
            if isinstance(args[0], list): values = args[0]
            elif isinstance(args[0], str):
                try: values = [(_['Close'], _['Volume']) for _ in self.__data if not datetime.strptime(args[0], '%Y-%m-%d').date() < _['Date']]
                except: pass
            else:
                try: values = [(_['Close'], _['Volume']) for _ in self.__data if not args[0] < _['Date']]
                except: pass
        if len(args) > 1: steps = args[1]
        if 'date' in list(kwargs.keys()):
            if isinstance(kwargs['date'], str):
                try: values = [(_['Close'], _['Volume']) for _ in self.__data if not datetime.strptime(kwargs['date'], '%Y-%m-%d').date() < _['Date']]
                except: pass
            else:
                try: values = [(_['Close'], _['Volume']) for _ in self.__data if not kwargs['date'] < _['Date']]
                except: pass
        if 'steps' in list(kwargs.keys()): steps = kwargs['steps']
        if len(values) >= steps:
            res, ys = [], []
            for x, y in values:
                res.append(x * y)
                ys.append(y)
            if not self.digits < 0: return round(sum(res[-steps:]) / sum(ys[-steps:]), self.digits)
            return sum(res[-steps:]) / sum(ys[-steps:])
