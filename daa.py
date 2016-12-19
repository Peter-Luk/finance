from utilities import rnd, gr, average
from statistics import stdev

class Methods():
    def __init__(self, ratio=None):
        self.ratio = ratio
        if not(ratio):self.ratio = gr
        self.__sf, self.__period = 2 / self.ratio, rnd(20 / self.ratio)

    def __del__(self):
        self.ratio = self.__sf = self.__period = None
        del(self.ratio)
        del(self.__sf)
        del(self.__period)

    def __delta(self, v):return max(v) - min(v)

    def date_range(self, data, date=None, period=None):
        sdate = list(data.keys())
        sdate.sort()
        if not(period):period = self.__period
        if date in sdate:
            sidate = sdate.index(date)
            if sidate < period:return "Not enough data ( min. %s)" % period
            else:rdate = sdate[sidate - period + 1:sidate + 1]
        else:rdate = sdate[-period:]
        return rdate

    def bbw(self, data, date=None, period=None):
        res, rdate = [], self.date_range(data, date, period)
        if type(rdate) is str:return rdate
        else:
            for di in rdate:
                res.append(self.__delta((data[di]['high'], data[di]['low'])))
            return 2. * self.__sf * stdev(res)

    def sar(self, data, date=None, atype='c', step=0.02, max=.2):
        trend, res, mdays = False, {},  list(data.keys())
        for di in mdays:
            if mdays.index(di) == 1:
                af, rh, rl  = step, data[di]['high'], data[di]['low']
                if data[di]['close'] > data[di]['open']:
                    trend, sp = True, data[di]['low'], data[di]['high'], data[di]['low']
                    res[di] = data[di]['close'] + (af * (sp - data[di]['close']))
                else:
                    trend, sp = False, data[di]['high']
                    res[di] = data[di]['close'] - (af * (sp - data[di]['close']))
            else:
# others
                 pass
#
    def ema(self, data, date=None, atype='c', period=None):
        if not(period):period = self.__period
        res, kratio = [], 2. / (1 + period)
        if type(data) is list:
            for i in range(len(data)):
                if i == 0:res.append(data[i])
                else:res.append(data[i] * kratio + res[-1] * (1. - kratio))
        elif type(data) is dict:
            rdate = self.date_range(data, date, period)
            if type(rdate) is str:return rdate
            else:
                for di in rdate:
                    if atype.lower() == 'hl':
                        if rdate.index(di) == 0:res.append(average((data[di]['high'], data[di]['low'])))
                        else:res.append(average((data[di]['high'], data[di]['low'])) * kratio + res[-1] * (1. - kratio))
                    elif atype.lower() == 'h':
                        if rdate.index(di) == 0:res.append(data[di]['high'])
                        else:res.append(data[di]['high'] * kratio + res[-1] * (1. - kratio))
                    elif atype.lower() == 'l':
                        if rdate.index(di) == 0:res.append(data[di]['low'])
                        else:res.append(data[di]['low'] * kratio + res[-1] * (1. - kratio))
                    else:
                        if rdate.index(di) == 0:res.append(data[di]['close'])
                        else:res.append(data[di]['close'] * kratio + res[-1] * (1. - kratio))
        return res[-1]

    def kama(self, data, date=None, atype='c', period=None, fast=None, slow=None):
        if not(period):period = self.__period
        res, rdate = [], self.date_range(data, date, period)
        d_change = data[rdate[-1]]['close'] - data[rdate[0]]['close']
        if atype.lower() == 'hl':d_change = average((data[rdate[-1]]['high'], data[rdate[-1]]['low'])) - average((data[rdate[0]]['high'], data[rdate[0]]['low']))
        i, tmp = 0, []
        while i < len(rdate) - 1:
            tmp.append(abs(data[rdate[i + 1]]['close'] - data[rdate[i]]['close']))
            if atype.lower() == 'hl':tmp.append(abs(average((data[rdate[i + 1]]['high'], data[rdate[i + 1]]['low'])) - average((data[rdate[i]]['high'], data[rdate[i]]['low']))))
            i += 1
        t_range = sum(tmp)
        e_ratio = abs(d_change / t_range)
        if not(slow):slow = period
        if not(fast):fast = slow / self.ratio
        fsc, ssc = 2. / (fast + 1), 2. / (slow + 1)
        wsc = e_ratio * (fsc - ssc) + ssc
        kratio = wsc * wsc
        if type(rdate) is str:return rdate
        else:
            for di in rdate:
                if atype.lower() == 'hl':
                    if rdate.index(di) == 0:res.append(average((data[di]['high'], data[di]['low'])))
                    else:res.append(average((data[di]['high'], data[di]['low'])) * kratio + res[-1] * (1 - kratio))
                else:
                    if rdate.index(di) == 0:res.append(data[di]['close'])
                    else:res.append(data[di]['close'] * kratio + res[-1] * (1 - kratio))
            return res[-1]

    def sma(self, data, date=None, atype='c', period=None):
        if not(period):period = self.__period
        res, rdate = [], self.date_range(data, date, period)
        if type(rdate) is str:return rdate
        else:
            for di in rdate:
                if atype.lower() == 'hl':
                    res.append(average((data[di]['high'], data[di]['low'])))
                else:res.append(data[di]['close'])
            return float(average(res))

    def wma(self, data, date=None, atype='c', period=None):
        if not(period):period = self.__period
        r1, r2, rdate = [], [], self.date_range(data, date, period)
        if type(rdate) is str:return rdate
        else:
            for di in rdate:
                do = sum((data[di]['high'], data[di]['low']))
                r2.append(do)
                if atype.lower() == 'hl':
                    r1.append(float(average((data[di]['high'], data[di]['low']))) * do)
                else:r1.append(float(data[di]['close']) * do)
            return sum(r1) / sum(r2)

    def bb(self, data, date=None):
        md = self.sma(data, date, 'hl')
        if type(md) is float:hr = self.bbw(data, date) / 2
        else:return md
        return rnd(md - hr), rnd(md + hr)

    def hv(self, data, date=None):
        datakeys = list(data.keys())
        if date not in datakeys:date = datakeys[-1]
        ddate = data[date]
        return self.__delta((ddate['high'], ddate['low'])) / float(ddate['open'])

    def rsi(self, data, date=None, atype='c', period=None):
        if not(period):period = self.__period
        r1, r2, rdate = [], [], self.date_range(data, date, period)
        if type(rdate) is str:return rdate
        else:
            for di in rdate:
                datakeys = list(data.keys())
                datakeys.sort()
                if atype.lower() == 'hl':
                    chl = average((data[di]['high'], data[di]['low']))
                    t = chl - average((data[datakeys[datakeys.index(di) - 1]]['high'], data[datakeys[datakeys.index(di) - 1]]['low']))
                    if t > 0:r1.append(chl)
                    else:r2.append(chl)
                else:
                    t = data[di]['close'] - data[datakeys[datakeys.index(di) - 1]]['close']
#                    if t > 0:r1.append(data[di]['close'])
#                    else:r2.append(data[di]['close'])
                    if t > 0:r1.append(float(t))
                    else:r2.append(float(-t))
            return 100 - 100 / (1 + average(r1) / average(r2))
