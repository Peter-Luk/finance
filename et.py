from utilities import filepath
from datetime import datetime
class Equities(object):
    def __init__(self, *args, **kwargs):
        self.digits = 2
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
        self.code = self.digits = self.__data = self.trade_date = self.latest = self.close = None
        del self.code, self.digits, self.__data, self.trade_date, self.latest, self.close

    def get(self, *args, **kwargs):
        digits = 2
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
                tmp[0] = datetime.strptime(tmp[0], '%Y-%m-%d')
                tmp[-1] = int(tmp[-1])
                j = 1
                while j < len(tmp) - 1:
                    tmp[j] = round(float(tmp[j]), digits)
                    j += 1
                values.append(tmp)
            except: pass
            i += 1
        i, tmp, el  = 0, [], ['Adj Close']
        rfl = [_ for _ in fields if _ not in el]
        while i < len(values):
            hdr = {}
            for _ in rfl:
                j = fields.index(_)
                hdr[fields[j]] = values[i][j]
            tmp.append(hdr)
            i += 1
        return tmp
