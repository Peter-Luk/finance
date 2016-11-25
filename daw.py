from utilities import rnd, gratio
from derivatives import Analyser
from daa import Methods

class Indicators(Analyser):
    def __init__(self, code):
        self.code, self.__methods = code.upper(), Methods()
        self.__an = Analyser(self.code)
        self.__unique_date, self.__cur = self.__an._Analyser__unique_date(), self.__an._Analyser__cur
        self.data = self.__raw_data()

    def __del__(self):
        self.__methods = self.code = self.__an = self.__unique_date = self.__cur = self.data = None
        del(self.__methods)
        del(self.code)
        del(self.__an)
        del(self.__unique_date)
        del(self.__cur)
        del(self.data)

    def __raw_data(self, fields=('open', 'high', 'low', 'close', 'volume')):
        r, qstr = {}, "SELECT %s, %s, %s, %s, %s FROM '%s' WHERE code='%s' AND date='%s' ORDER BY session DESC"
        for da in self.__unique_date:
            d, th = {}, self.__cur.execute(qstr % (fields + ('records', self.code, da))).fetchall()
            if len(th) > 1:
                d[fields[0]] = th[0][0]
                d[fields[1]] = max(th[0][1], th[-1][1])
                d[fields[2]] = min(th[0][2], th[-1][2])
                d[fields[3]] = th[-1][3]
                d[fields[4]] = th[0][4] + th[-1][4]
            elif len(th) == 1:
                for i in fields:d[i] = th[0][fields.index(i)]
            r[str(da)] = d
        return r

    def bbw(self, date=None, period=None):return self.__methods.bbw(self.data, date, period)
    def rsi(self, date=None, period=None):return self.__methods.rsi(self.data, date, period)
    def ema(self, date=None, atype='c', period=None):return self.__methods.ema(self.data, date, atype, period)
    def kama(self, date=None, atype='c', period=None, fast=None, slow=None):return self.__methods.kama(self.data, date, atype, period, fast, slow)
    def sma(self, date=None, atype='c', period=None):return self.__methods.sma(self.data, date, atype, period)
    def wma(self, date=None, atype='c', period=None):return self.__methods.wma(self.data, date, atype, period)
    def bb(self, date=None):return self.__methods.bb(self.data, date)
    def hv(self, date=None):return self.__methods.hv(self.data, date)
    def rsi(self, date=None, atype='c', period=None):return self.__methods.rsi(self.data, date, atype, period)
    def apz(self, date=None, period=5):
        unique_date = [str(v) for v in self.__unique_date]
        if not(date):date = unique_date[-1]
        dateindex = unique_date.index(date)
        if dateindex > (2 * period):
            eh = [self.ema(date = unique_date[dateindex - i], atype = 'h', period=period) for i in range(period)]
            el = [self.ema(date = unique_date[dateindex - i], atype = 'l', period=period) for i in range(period)]
            vv = self.__methods.ema([eh[i] for i in range(len(eh))], period = len(eh)) - self.__methods.ema([el[i] for i in range(len(el))], period = len(el))
            ema = self.ema(date=date, atype='hl', period=period)
            return rnd(ema - (vv / 2)), rnd(ema + (vv / 2))

    def __prvdate(self, date):
        udate = self.__unique_date
        if date in udate:
            pos = udate.index(date)
            return udate[:pos].pop(), udate[pos]
        elif date > udate[-1]:
            return udate[-2:]
        elif date > udate[0]:
            j = 0
            while j in range(len(udate)):
                if udate[j] > date:break
                j += 1
            return udate[:j][-2:]

    def dhv(self, date=None):
        if not(date):date = self.__unique_date[-1]
        daterange = self.__prvdate(date)
        return self.hv(date=daterange[-1].strftime('%Y-%m-%d')) - self.hv(date=daterange[0].strftime('%Y-%m-%d'))

    def drs(self, atype='c', date=None):
        if not(date):date = self.__unique_date[-1]
        daterange = self.__prvdate(date)
        return self.rsi(date=daterange[-1].strftime('%Y-%m-%d'), atype=atype) - self.rsi(date=daterange[0].strftime('%Y-%m-%d'), atype=atype)

    def kfs(self, atype='c', date=None, fast=5, slow=0):
        if date:
            if not(type(date) is str):date = date.strftime('%Y-%m-%d')
        else:date = self.__unique_date[-1].strftime('%Y-%m-%d')
        if not(slow):slow = 12
        return self.kama(atype=atype, date=date, period=fast) - self.kama(atype=atype, date=date, period=slow)

    def grapz(self, date=None):
        if date:
            if not(type(date) is str):date = date.strftime('%Y-%m-%d')
        else:date = self.__unique_date[-1].strftime('%Y-%m-%d')
        res = self.apz(date=date)
        return gratio(res[0], res[-1], enhanced=True)

    def grbb(self, date=None):
        if date:
            if not(type(date) is str):date = date.strftime('%Y-%m-%d')
        else:date = self.__unique_date[-1].strftime('%Y-%m-%d')
        res = self.bb(date=date)
        return gratio(res[0], res[-1], enhanced=True)
