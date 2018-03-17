from alchemy import load, mapper
from utilities import datetime, nitem, gslice, mtf, gr
from statistics import mean

import pandas as pd

class FD(object):
    def __init__(self, *args):
        self.date = None
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.volume = 0
        if args:
            if isinstance(args[0], dict):
                self.date = list(args[0].keys())[0]
                self.open = args[0][self.date]['open']
                self.high = args[0][self.date]['high']
                self.low = args[0][self.date]['low']
                self.close = args[0][self.date]['close']
                self.volume = args[0][self.date]['volume']

    def __del__(self):
        self.date = self.open = self.high = self.low = self.close = self.volume = None
        del(self.date, self.open, self.high, self.low, self.close, self.volume)

    def __call__(self):
        hdr = {'open': self.open}
        hdr['high'] = self.high
        hdr['low'] = self.low
        hdr['close'] = self.close
        hdr['volume'] = self.volume
        hdr['date'] = self.date
        return hdr

class SRD(object): pass
class FRD(object): pass
db_name = ['Securities', 'Futures']
db = load(db_name)
mapper(SRD, db['Securities']['table'])
mapper(FRD, db['Futures']['table'])

avail_eid = [_[0] for _ in db['Securities']['session'].query(SRD.eid).distinct()]
avail_eid.sort()
sqa = db['Securities']['session'].query(SRD)
fqa = db['Futures']['session'].query(FRD)

def daily(*args):
    if args:
        if isinstance(args[0], str): code = args[0]
    # fqa = query('Futures')
    td, res = [], []
    try:
        rfd = fqa.filter_by(code=code).all()
        for _ in rfd:
            if _.date not in td: td.append(_.date)
        for _ in td:
            tmp = {}
            itd = fqa.filter_by(code=code, date=_).all()
            if len(itd) == 2:
                volume, open, close, high, low = 0, 0, 0, 0, 0
                for __ in itd:
                    if __.session == 'A':
                        if __.high > high: high = __.high
                        if __.low < low: low = __.low
                        close = __.close
                        volume += __.volume
                    else:
                        open, high, low, close = __.open, __.high, __.low, __.close
                        volume += __.volume
            if len(itd) == 1:
                volume, open, high, low, close = itd[0].volume, itd[0].open, itd[0].high, itd[0].low, itd[0].close
            tmp[_] = {'open':open, 'high':high, 'low':low, 'close':close, 'volume':volume}
            res.append(FD(tmp))
    except: pass
    return res

class Danta(object):
    class __BD(object):
        def __init__(self, *args):
            if isinstance(args[0], (int, float)): self.Upper = args[0]
            if isinstance(args[1], (int, float)): self.Lower = args[1]
            if self.Upper < self.Lower:
                t = self.Upper
                self.Upper = self.Lower
                self.Lower = t
        def __del__(self):
            self.Upper = self.Lower = None
            del(self.Upper, self.Lower)
        def __call__(self):
            return {'upper':self.Upper, 'lower':self.Lower}

    class __SCD(object):
        def __init__(self, *args, **kwargs):
            if isinstance(args[0], (int, float)): self.K = args[0]
            if isinstance(args[1], (int, float)): self.D = args[1]
            # if 'step_value' in list(kwargs.keys()): sv = kwargs['step_value']

        def __del__(self):
            self.K = self.D = None
            del(self.K, self.D)

        def __call__(self, *args, **kwargs):
            digit = 3
            if args:
                if isinstance(args[0], (int, float)): digit = int(args[0])
            if 'digit' in list(kwargs.keys()):
                if isinstance(kwargs['digit'], (int, float)): digit = int(kwargs['digit'])
            return {'K':round(self.K, digit), 'D':round(self.D, digit)}

    def __init__(self, *args, **kwargs):
        self.data, self.__max_n = [], 500
        if 'max_n' in list(kwargs.keys()): self.__max_n = kwargs['max_n']
        if args[0] in avail_eid:
            self.data = sqa.filter_by(eid=args[0]).all()
            if len(self.data) > self.__max_n: self.data = self.data[-self.__max_n:]
            self.__trade_date = [_.date for _ in self.data]
        else:
            try:
                self.data = daily(args[0])
                self.__trade_date = [_.date for _ in self.data]
            except: pass

    def __del__(self):
        self.__max_n = self.data = self.__trade_date = None
        del(self.__max_n, self.data, self.__trade_date)

    def __call__(self, *args):
        period = 20
        if args:
            if isinstance(args[0], (int, float)): period = int(args[0])
        mas = pd.concat([self.SMA(period), self.WMA(period), self.EMA(period), self.KAMA()], axis=1, join='inner', ignore_index=False)
        ids = pd.concat([self.STC(), self.RSI(), self.ADX()], axis=1, join='inner', ignore_index=False)
        return {'MA': mas, 'Ind':ids}

    def sma(self, *args):
        data, period = self.data, 20
        if args:
            if isinstance(args[0], (tuple, list)): data = list(args[0])
            if len(args) > 1:
                if isinstance(args[1], (int, float)): period = int(args[1])
        if not (len(data) > period): return mean([_.close for _ in data])
        return mean([_.close for _ in data[-period:]])

    def wma(self, *args):
        data, period = self.data, 20
        if args:
            if isinstance(args[0], (tuple, list)): data = list(args[0])
            if len(args) > 1:
                if isinstance(args[1], (int, float)): period = int(args[1])
        if not (len(data) > period): return sum([_.close * _.volume for _ in data]) / sum([_.volume for _ in data])
        return sum([_.close * _.volume for _ in data[-period:]]) / sum([_.volume for _ in data[-period:]])

    def rsi(self, *args):
        data, period = self.data, 14
        if args:
            if isinstance(args[0], (tuple, list)): data = list(args[0])
            if len(args) > 1:
                if isinstance(args[1], (int, float)): period = int(args[1])

        def __delta(*args):
            i, hdr = 1, []
            while i < len(args[0]):
                hdr.append(args[0][i].close - args[0][i - 1].close)
                i += 1
            return hdr

        i = period
        while i < len(data):
            if i == period:
                ag = sum([_ for _ in __delta(data[i-period:i]) if _ > 0]) / period
                al = sum([abs(_) for _ in __delta(data[i-period:i]) if _ < 0]) / period
            else:
                cdelta = data[i].close - data[i-1].close
                if cdelta > 0:
                    ag = (cdelta + ag * (period - 1)) / period
                    al = al * (period - 1) / period
                else:
                    ag = ag * (period - 1) / period
                    al = (abs(cdelta) + al * (period - 1)) / period
            i += 1
        try: return 100 - 100 / (1 + ag / al)
        except: pass

    def ema(self, *args):
        data, numtype, period = self.data, False, 20
        if args:
            if isinstance(args[0], list):
                if isinstance(args[0][0], (int, float)): numtype = True
                data = args[0]
            if len(args) > 1:
                if isinstance(args[1], (int, float)): period = int(args[1])
        if not (len(data) > period):
            if numtype: return mean(data)
            return mean([_.close for _ in data])
        if numtype: res = mean(data[:period])
        else: res = mean([_.close for _ in data[:period]])
        i = period
        while i < len(data):
            if numtype: res = (data[i] + res * (period - 1)) / period
            else: res = (data[i].close + res * (period - 1)) / period
            i += 1
        return res

    def kama(self, *args):
        data, period, fast, slow = self.data, 10, 2, 30
        if args:
            if isinstance(args[0], (tuple, list)): data = list(args[0])
            if len(args) > 1:
                if isinstance(args[1], (int, float)): period = int(args[1])
            if len(args) > 2:
                if isinstance(args[2], (int, float)): fast = int(args[2])
            if len(args) > 3:
                if isinstance(args[3], (int, float)): slow = int(args[3])

        def asum(*args):
            i, hdr = 1, []
            while i < len(args[0]):
                hdr.append(args[0][i] - args[0][i - 1])
                i += 1
            return sum([abs(_) for _ in hdr])

        if not (len(data) > period): return mean([_.close for _ in data])
        res, i, fc, sc = mean([_.close for _ in data[:period]]), period, 2 / (fast + 1), 2 / (slow + 1)
        while i < len(data):
            volatility = asum([_.close for _ in data[i-period:i]])
            if volatility:
                er = (data[i].close - data[i-period].close) / volatility
                alpha = (er * (fc - sc) + sc) ** 2
                res += alpha * (data[i].close - res)
            i += 1
        return res

    def apz(self, *args):
        data, period = self.data, 5
        if args:
            if isinstance(args[0], (tuple, list)): data = list(args[0])
            if len(args) > 1:
                if isinstance(args[1], (int, float)): period = int(args[1])
        vl = [_.high - _.low for _ in data]
        i, vpl, epl = period, [], []
        while i < len(data):
            vpl.append(self.ema(vl[:i], period))
            epl.append(self.ema(data[:i], period))
            i += 1
        evp, eep = self.ema(vpl, period), self.ema(epl, period)
        return self.__BD(eep + evp, eep - evp)

    def __tr(self, *args):
        i, res = 0, []
        while i < len(args[0]):
            gap = False
            if i == 0: res.append(args[0][i].high - args[0][i].low)
            if i > 0:
                if args[0][i-1].close > args[0][i].high:
                    gap = True
                    res.append(args[0][i-1].close - args[0][i].low)
                if args[0][i-1].close < args[0][i].low:
                    gap = True
                    res.append(args[0][i].high - args[0][i-1].close)
                if not gap: res.append(args[0][i].high - args[0][i].low)
            i += 1
        return res

    def __dm(self, *args):
        i, res = 1, []
        if args:
            if args[1] == '+':
                while i < len(args[0]):
                    value = 0
                    if args[0][i].high - args[0][i-1].high > args[0][i-1].low -args[0][i].low:
                        if args[0][i].high > args[0][i-1].high:
                            value = args[0][i].high - args[0][i-1].high
                    res.append(value)
                    i += 1
            if args[1] == '-':
                while i < len(args[0]):
                    value = 0
                    if args[0][i-1].low - args[0][i].low > args[0][i].high -args[0][i-1].high:
                        if args[0][i-1].low > args[0][i].low:
                            value = args[0][i-1].low - args[0][i].low
                    res.append(value)
                    i += 1
        return res

    def adx(self, *args):
        data, period = self.data, 14
        if args:
            if isinstance(args[0], (tuple, list)): data = list(args[0])
            if len(args) > 1:
                if isinstance(args[1], (int, float)): period = int(args[1])
        i, trd, dmp, dmm = period, self.__tr(data), self.__dm(data, '+'), self.__dm(data, '-')
        while i < len(data):
            if i == period:
                trs = mean(trd[:i])
                dmps = mean(dmp[:i-1])
                dmms = mean(dmm[:i-1])
                dip = dmps / trs
                dim = dmms / trs
                dx = abs(dip - dim) / (dip + dim) * 100
                res = dx
            if i > period:
                trs = (trs * (period - 1) + trd[i]) / period
                dmps = (dmps * (period - 1) + dmp[i-1]) / period
                dmms = (dmms * (period - 1) + dmm[i-1]) / period
                dip = dmps / trs
                dim = dmms / trs
                dx = abs(dip - dim) / (dip + dim) * 100
                res = (res * (period - 1) + dx) / period
            i += 1
        return res

    def atr(self, *args, **kwargs):
        data, period = self.data, 14
        if args:
            if isinstance(args[0], (tuple, list)): data = list(args[0])
            if len(args) > 1:
                if isinstance(args[1], (int, float)): period = int(args[1])
        if 'period' in list(kwargs.keys()):
            if isinstance(kwargs['period'], (int, float)): period = int(kwargs['period'])
        i, trd = period, self.__tr(data)
        while i < len(data):
            if i == period: res = mean(trd[:i])
            else: res = (trd[i] + res * (period - 1)) / period
            i += 1
        return res

    def kc(self, *args, **kwargs):
        data, ma_period, tr_period = self.data, 20, 10
        if args:
            if isinstance(args[0], (tuple, list)): data = list(args[0])
            if len(args) > 1:
                if isinstance(args[1], (list, tuple)): ma_period, tr_period = list(args[1])
        if 'period' in list(kwargs.keys()):
            if isinstance(kwargs['period'], dict):
                if 'MA' in list(kwargs['period'].keys()):
                    if isinstance(kwargs['period']['MA'], (int, float)): ma_period = int(kwargs['period']['MA'])
                if 'TR' in list(kwargs['period'].keys()):
                    if isinstance(kwargs['period']['TR'], (int, float)): tr_period = int(kwargs['period']['TR'])
            if isinstance(kwargs['period'], (list, tuple)): ma_period, tr_period = list(kwargs['period'])
        axis, delta = self.kama(data, ma_period), self.atr(data, tr_period)
        return self.__BD(axis + gr * delta, axis - gr * delta)

    def stc(self, *args, **kwargs):
        data, period, mean_n = self.data, 14, 3
        if args:
            if isinstance(args[0], (tuple, list)): data = list(args[0])
            if len(args) > 1:
                if isinstance(args[1], (int, float)): period = int(args[1])
            if len(args) > 2:
                if isinstance(args[2], (int, float)): mean_n = int(args[2])
        if 'period' in list(kwargs.keys()):
            if isinstance(kwargs['period'], (int, float)): period = int(kwargs['period'])
        if 'n' in list(kwargs.keys()):
            if isinstance(kwargs['n'], (int, float)): mean_n = int(kwargs['n'])

        def pk(*args):
            data, period = args[0], args[1]
            pma, pmi = max([_.high for _ in data[-period:]]), min([_.low for _ in data[-period:]])
            return (data[-1].close - pmi) / (pma - pmi) * 100

        i, lnpk = 1, [pk(data, period)]
        while i < mean_n:
            lnpk.append(pk(data[:-i], period))
            i += 1
        lnpk.reverse()
        return self.__SCD(lnpk[-1], mean(lnpk))

    def macd(self, *args):
        data, mf_period, ms_period, s_period = self.data, 12, 26, 9
        if args:
            if isinstance(args[0], list): data = args[0]
            if len(args) > 1:
                if isinstance(args[1], (int, float)): s_period = int(args[1])
        mfl, msl, ml = [], [], []
        i = ms_period
        while i < len(data):
            mfl.append(self.ema(data[:i], mf_period))
            msl.append(self.ema(data[:i], ms_period))
            i += 1
        i = 0
        while i < (len(data) - ms_period):
            ml.append(mfl[i] - msl[i])
            i += 1
        s = self.ema(ml, s_period)
        return ml[-1], s, ml[-1] - s

    def patr(self, *args):
        data = self.data
        if args:
            if isinstance(args[0], (list, tuple)): data = list(args[0])
        lc = data[-1].close
        lr = self.atr(data)
        res = gslice([lc + lr, lc])
        res.extend(gslice([lc, lc -lr]))
        res.sort()
        return res

    def pgap(self, *args):
        data = self.data
        if args:
            if isinstance(args[0], (int, float)): pivot = args[0]
            if len(args) > 1:
                if isinstance(args[1], (list, tuple)): data = list(args[1])
        gap = pivot - data[-1].close
        res = gslice([pivot + gap, pivot])
        res.extend(gslice([pivot, pivot - gap]))
        res.sort()
        return res

    def full_range(self, *args):
        data, res, hdr = self.data, [], []
        if args:
            if isinstance(args[0], (tuple, list)): data = list(args[0])
        [hdr.extend(self.pgap(_, data)) for _ in self.patr(data)]
        hdr.sort()
        while len(hdr):
            ph = hdr.pop()
            if ph not in hdr: res.append(ph)
        return res

    def SMA(self, *args):
        period, hdr = 20, {}
        if args:
            if isinstance(args[0], (int, float)): period = int(args[0])
        for __ in self.__trade_date[period:]: hdr[__] = self.sma([_ for _ in self.data if not _.date > __], period)
        res = pd.DataFrame.from_dict(hdr, orient='index')
        res.rename(columns={'index':'Date', 0:'SMA'}, inplace=True)
        return res

    def WMA(self, *args):
        period, hdr = 20, {}
        if args:
            if isinstance(args[0], (int, float)): period = int(args[0])
        for __ in self.__trade_date[period:]: hdr[__] = self.wma([_ for _ in self.data if not _.date > __], period)
        res = pd.DataFrame.from_dict(hdr, orient='index')
        res.rename(columns={'index':'Date', 0:'WMA'}, inplace=True)
        return res

    def EMA(self, *args):
        period, hdr = 20, {}
        if args:
            if isinstance(args[0], (int, float)): period = int(args[0])
        for __ in self.__trade_date[period:]: hdr[__] = self.ema([_ for _ in self.data if not _.date > __], period)
        res = pd.DataFrame.from_dict(hdr, orient='index')
        res.rename(columns={'index':'Date', 0:'EMA'}, inplace=True)
        return res

    def KAMA(self, *args):
        period, fast, slow, hdr = 10, 2, 30, {}
        if args:
            if isinstance(args[0], (int, float)): period = int(args[0])
            if len(args) > 1:
                if isinstance(args[1], (int, float)): fast = int(args[1])
            if len(args) > 1:
                if isinstance(args[2], (int, float)): slow = int(args[2])
        for __ in self.__trade_date[period:]: hdr[__] = self.kama([_ for _ in self.data if not _.date > __], period, fast, slow)
        res = pd.DataFrame.from_dict(hdr, orient='index')
        res.rename(columns={'index':'Date', 0:'KAMA'}, inplace=True)
        return res

    def RSI(self, *args):
        period, hdr = 14, {}
        if args:
            if isinstance(args[0], (int, float)): period = int(args[0])
        for __ in self.__trade_date[period:]: hdr[__] = self.rsi([_ for _ in self.data if not _.date > __], period)
        res = pd.DataFrame.from_dict(hdr, orient='index')
        res.rename(columns={'index':'Date', 0:'RSI'}, inplace=True)
        return res

    def ADX(self, *args):
        period, hdr = 14, {}
        if args:
            if isinstance(args[0], (int, float)): period = int(args[0])
        for __ in self.__trade_date[period:]: hdr[__] = self.adx([_ for _ in self.data if not _.date > __], period)
        res = pd.DataFrame.from_dict(hdr, orient='index')
        res.rename(columns={'index':'Date', 0:'ADX'}, inplace=True)
        return res

    def STC(self, *args):
        period, kd, dd = 14, {}, {}
        if args:
            if isinstance(args[0], (int, float)): period = int(args[0])
        for __ in self.__trade_date[period:]:
            hdr = self.stc([_ for _ in self.data if not _.date > __], period)
            kd[__], dd[__] = hdr.K, hdr.D
        pkd, pdd = pd.DataFrame.from_dict(kd, orient='index'), pd.DataFrame.from_dict(dd, orient='index')
        pkd.rename(columns={'index':'Date', 0:'%K'}, inplace=True)
        pdd.rename(columns={'index':'Date', 0:'%D'}, inplace=True)
        res = pd.concat([pkd, pdd], axis=1, join='inner', ignore_index=False)
        return res

    def KC(self, *args):
        period, ud, ld = (20, 10), {}, {}
        if args:
            if isinstance(args[0], (tuple, list)): period = tuple(args[0])
        for __ in self.__trade_date[period[0]:]:
            hdr = self.kc([_ for _ in self.data if not _.date > __], period)
            ud[__], ld[__] = hdr.Upper, hdr.Lower
        pud, pld = pd.DataFrame.from_dict(ud, orient='index'), pd.DataFrame.from_dict(ld, orient='index')
        pud.rename(columns={'index':'Date', 0:'Upper'}, inplace=True)
        pld.rename(columns={'index':'Date', 0:'Lower'}, inplace=True)
        res = pd.concat([pud, pld], axis=1, join='inner', ignore_index=False)
        return res

    def APZ(self, *args):
        period, ud, ld = 5, {}, {}
        if args:
            if isinstance(args[0], (int, float)): period = int(args[0])
        for __ in self.__trade_date[period:]:
            hdr = self.apz([_ for _ in self.data if not _.date > __], period)
            ud[__], ld[__] = hdr.Upper, hdr.Lower
        pud, pld = pd.DataFrame.from_dict(ud, orient='index'), pd.DataFrame.from_dict(ld, orient='index')
        pud.rename(columns={'index':'Date', 0:'Upper'}, inplace=True)
        pld.rename(columns={'index':'Date', 0:'Lower'}, inplace=True)
        res = pd.concat([pud, pld], axis=1, join='inner', ignore_index=False)
        return res
