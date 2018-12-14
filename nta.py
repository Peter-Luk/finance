import numpy as np
import pandas as pd
from datetime import datetime
from utilities import gslice, gr, lique

class ONA(object):
    k_period={'atr':14, 'er':10, 'fast':2, 'slow':30}
    def __init__(self, data, date=datetime.today().date()):
        self.data = data
        self.date = date
        if date not in self.data['Date']: self.date = self.data['Date'][-1]
        self.data = self.construct(self.data, self.date)

    def __del__(self):
        self.data = self.date = None
        del(self.data, self.date)

    def ma(self, raw=None, period=20, favour='s', req_field='close', programmatic=False):
        if not raw: raw = self.data
        mres = []
        def process(raw, period, favour, req_field):
            res, _ = [], 0
            while _ < len(raw):
                hdr = np.nan
                if not _ < period - 1:
                    if req_field.lower() in ['close', 'c']: rdata = raw[_ - period + 1: _ + 1, -2]
                    if req_field.lower() in ['full', 'f', 'ohlc', 'all']: rdata = raw[_ - period + 1: _ + 1, :-1].mean(axis=1)
                    if req_field.lower() in ['range', 'hl', 'lh']: rdata = raw[_ - period + 1: _ + 1, 1:3].mean(axis=1)
                    if favour[0].lower() == 's': hdr = rdata.mean()
                    if favour[0].lower() == 'w': hdr = (rdata * raw[_ - period + 1: _ + 1, -1]).sum() / raw[_ - period + 1: _ + 1, -1].sum()
                    if favour[0].lower() == 'e':
                        if _ == period: hdr = rdata.mean()
                        if _ > period: hdr = (res[-1] * (period - 1) + rdata[-1]) / period
                res.append(hdr)
                _ += 1
            return res
        rflag = np.isnan(raw['Data']).any(axis=1)
        if rflag.any():
            _ = 0
            while _ < len(raw['Data'][rflag]):
                mres.append(np.nan)
                _ += 1
            mres.extend(process(raw['Data'][~rflag], period, favour, req_field))
        else: mres.extend(process(raw['Data'], period, favour, req_field))
        if programmatic: return mres
        return pd.DataFrame({'{}ma'.format(favour).upper(): mres}, index=raw['Date'])

    def bbw(self, raw=None, period=20, req_field='close', programmatic=False):
        if not raw: raw = self.data
        mres = []
        def process(raw, period, req_field):
            res, _ = [], 0
            while _ < len(raw):
                hdr = np.nan
                if not _ < period - 1:
                    if req_field.lower() in ['close', 'c']: rdata = raw[_ - period + 1: _ + 1, -2]
                    if req_field.lower() in ['full', 'f', 'ohlc', 'all']: rdata = raw[_ - period + 1: _ + 1, :-1].mean(axis=1)
                    if req_field.lower() in ['range', 'hl', 'lh']: rdata = raw[_ - period + 1: _ + 1, 1:3].mean(axis=1)
                    hdr = 2 * rdata.std()
                res.append(hdr)
                _ += 1
            return res
        rflag = np.isnan(raw['Data']).any(axis=1)
        if rflag.any():
            _ = 0
            while _ < len(raw['Data'][rflag]):
                mres.append(np.nan)
                _ += 1
            mres.extend(process(raw['Data'][~rflag], period, req_field))
        else: mres.extend(process(raw['Data'], period, req_field))
        if programmatic: return mres
        return pd.DataFrame({'BBW': mres}, index=raw['Date'])

    def kama(self, raw=None, period=k_period, programmatic=False):
        if not raw: raw = self.data
        mres, sma = [], self.ma(raw, period['slow'], 'e', 'c', True)

        def er(raw, period):
            def __dc(raw, lapse=period['er']):
                res, _ = [], 0
                while _ < len(raw):
                    hdr = np.nan
                    if not _ < lapse: hdr = abs(raw[_,-2] - raw[_ - lapse,-2])
                    res.append(hdr)
                    _ += 1
                return res

            res, _, d_close = [], 0, __dc(raw)
            while _ < len(raw):
                hdr = np.nan
                if not _ < period['er']:
                    __, delta = 1, 0
                    while __ < period['er']:
                        delta += abs(raw[_ - period['er'] + 1 + __, -2] - raw[_ - period['er'] + __, -2])
                        __ += 1
                    hdr = d_close[_]
                    if delta: hdr = hdr / delta
                res.append(hdr)
                _ += 1
            return res

        def sc(raw, period):
            res, _, ver = [], 0, er(raw, period)
            while _ < len(raw):
                hdr = np.nan
                if not _ < period['slow']:
                    fsc = 2 / (period['fast'] + 1)
                    ssc = 2 / (period['slow'] + 1)
                    hdr = (ver[_] * (fsc - ssc) + ssc) ** 2
                res.append(hdr)
                _ += 1
            return res

        def process(raw, period):
            res, _, vsc = [], 0, sc(raw, period)
            while _ < len(raw):
                hdr = np.nan
                if _ == period['slow']: hdr = sma[_]
                if _ > period['slow']: hdr = res[-1] + vsc[_] * (raw[_, -2] - res[-1])
                res.append(hdr)
                _ += 1
            return res

        rflag = np.isnan(raw['Data']).any(axis=1)
        if rflag.any():
            _ = 0
            while _ < len(raw['Data'][rflag]):
                mres.append(np.nan)
                _ += 1
            mres.extend(process(raw['Data'][~rflag], period))
        else: mres.extend(process(raw['Data'], period))
        if programmatic: return mres
        return pd.DataFrame({'KAMA': mres}, index=raw['Date'])

    def atr(self, raw=None, period=14, programmatic=False):
        if not raw: raw = self.data
        mres = []
        def __tr(data):
            nr, res, _ = data[:,:-1].ptp(axis=1).tolist(), [], 0
            while _ < len(data):
                hdr = nr[_]
                if not _ == 0:
                    hmpc, lmpc = abs(data[_, 1] - data[_ - 1, -2]), abs(data[_, 2] - data[_ - 1, -2])
                    hdr = hmpc
                    if lmpc > nr[_]:
                        if lmpc > hmpc: hdr = lmpc
                    elif hmpc < nr[_]: hdr = nr[_]
                res.append(hdr)
                _ += 1
            return res

        def process(data, period):
            res, _, truerange = [], 0, __tr(data)
            while _ < len(data):
                if _ < period: hdr = np.nan
                else:
                    if _ == period: hdr = np.mean(truerange[:_])
                    else: hdr = (res[-1] * (period - 1) + truerange[_]) / period
                res.append(hdr)
                _ += 1
            return res

        rflag = np.isnan(raw['Data']).any(axis=1)
        if rflag.any():
            _ = 0
            while _ < len(raw['Data'][rflag]):
                mres.append(np.nan)
                _ += 1
            mres.extend(process(raw['Data'][~rflag], period))
        else: mres.extend(process(raw['Data'], period))
        if programmatic: return mres
        return pd.DataFrame({'ATR': mres}, index=raw['Date'])

    def adx(self, raw=None, period=14):
        if not raw: raw = self.data
        mres = []
        def __tr(data):
            nr, res, _ = data[:,:-1].ptp(axis=1).tolist(), [], 0
            while _ < len(data):
                hdr = nr[_]
                if not _ == 0:
                    hmpc, lmpc = abs(data[_, 1] - data[_ - 1, -2]), abs(data[_, 2] - data[_ - 1, -2])
                    hdr = hmpc
                    if lmpc > nr[_]:
                        if lmpc > hmpc: hdr = lmpc
                    elif hmpc < nr[_]: hdr = nr[_]
                res.append(hdr)
                _ += 1
            return res

        def __dm(data):
            i, res = 1, {'+':[0], '-':[0]}
            while i < len(data):
                dh, dl = data[i][1] - data[i - 1][1], data[i - 1][2] - data[i][2]
                if dh > dl and dh > 0: res['+'].append(dh)
                else: res['+'].append(0)
                if dl > dh and dl > 0: res['-'].append(dl)
                else: res['-'].append(0)
                i += 1
            return res

        def __di(data, period=period):
            res, _, dir_mov, true_range = {'+':[np.nan], '-':[np.nan]}, 0, __dm(data), __tr(data)
            while _ < len(dir_mov['+']):
                if _ < period:
                    phdr = np.nan
                    mhdr = np.nan
                else:
                    phdr = sum(dir_mov['+'][_ - period:_]) / sum(true_range[_ - period + 1:_ + 1])
                    mhdr = sum(dir_mov['-'][_ - period:_]) / sum(true_range[_ - period + 1:_ + 1])
                res['+'].append(phdr)
                res['-'].append(mhdr)
                _ += 1
            return res

        def __dx(raw, period):
            _, res, dip = 0, [], __di(raw, period)
            while _ < len(raw):
                if np.isnan(dip['+'][_]) or np.isnan(dip['-'][_]): hdr = np.nan
                else:
                    hdr = abs(dip['+'][_] - dip['-'][_]) / (dip['+'][_] + dip['-'][_]) * 100
                res.append(hdr)
                _ += 1
            return res

        def process(data, period):
            res, nc, _, vdx = [], 0, 0, __dx(data, period)
            while _ < len(data):
                if np.isnan(vdx[_]):
                    hdr = np.nan
                    nc += 1
                else:
                    if _ < period + nc: hdr = np.nan
                    else:
                        if _ == period + nc: hdr = np.mean(vdx[nc:_])
                        else: hdr = (res[-1] * (period - 1) + vdx[_]) / period
                res.append(hdr)
                _ += 1
            return res

        rflag = np.isnan(raw['Data']).any(axis=1)
        if rflag.any():
            _ = 0
            while _ < len(raw['Data'][rflag]):
                mres.append(np.nan)
                _ += 1
            mres.extend(process(raw['Data'][~rflag], period))
        else: mres.extend(process(raw['Data'], period))
        return pd.DataFrame({'ADX': mres}, index=raw['Date'])

    def rsi(self, raw=None, period=14):
        if not raw: raw = self.data
        mres = []
        def process(raw, period):
            _, gain, loss, res = 0, [], [], []
            while _ < len(raw):
                if _ == 0:
                    if raw[_, -2] > raw[_, 0]:
                        gain.append(raw[_, -2] - raw[_, 0])
                        loss.append(0)
                    elif raw[_, -2] == raw[_, 0]:
                        gain.append(0)
                        loss.append(0)
                    else:
                        gain.append(0)
                        loss.append(raw[_, 0] - raw[_, -2])
                else:
                    if raw[_, -2] > raw[_ - 1, -2]:
                        gain.append(raw[_, -2] - raw[_ - 1, -2])
                        loss.append(0)
                    elif raw[_, -2] == raw[_ - 1, -2]:
                        gain.append(0)
                        loss.append(0)
                    else:
                        gain.append(0)
                        loss.append(raw[_ -1, -2] - raw[_, -2])
                _ += 1
            _ = 0
            while _ < len(raw):
                hdr = np.nan
                if _ == period: hdr = 100 - 100 / ((1 + np.mean(gain[:_]) / np.mean(loss[:_])))
                if _ > period: hdr = 100 - 100 / (( 1 + np.mean(gain[_ - period:_]) / np.mean(loss[_ - period:_])))
                res.append(hdr)
                _ += 1
            return res

        rflag = np.isnan(raw['Data']).any(axis=1)
        if rflag.any():
            _ = 0
            while _ < len(raw['Data'][rflag]):
                mres.append(np.nan)
                _ += 1
            mres.extend(process(raw['Data'][~rflag], period))
        else: mres.extend(process(raw['Data'], period))
        return pd.DataFrame({'RSI': mres}, index=raw['Date'])

    def kc(self, raw=None, period=k_period, ratio=gr, programmatic=False):
        upper, lower = [], []
        if not raw: raw = self.data
        _, kma, ar = 0, self.kama(raw=raw, period={'er':period['er'], 'fast':period['fast'], 'slow':period['slow']}, programmatic=True), self.atr(raw=raw, period=period['atr'], programmatic=True)
        while _ < len(kma):
            uhdr, lhdr = np.nan, np.nan
            if not np.isnan([kma[_], ar[_]]).any():
                uhdr = kma[_] + ratio * ar[_]
                lhdr = kma[_] - ratio * ar[_]
            upper.append(uhdr)
            lower.append(lhdr)
            _ += 1
        if programmatic: return {'Upper':upper, 'Lower':lower}
        res = pd.DataFrame.from_dict({'Upper':upper, 'Lower':lower})
        res.index = raw['Date']
        return res

    def bb(self, raw=None, period=20, req_field='c', programmatic=False):
        upper, lower = [], []
        if not raw: raw = self.data
        sma, bw = self.ma(raw=raw, period=period, req_field=req_field, programmatic=True), self.bbw(raw=raw, period=period, req_field=req_field, programmatic=True)
        _ = 0
        while _ < len(sma):
            uhdr, lhdr = np.nan, np.nan
            if not np.isnan([sma[_], bw[_]]).any():
                uhdr = sma[_] + bw[_] / 2
                lhdr = sma[_] - bw[_] / 2
            upper.append(uhdr)
            lower.append(lhdr)
            _ += 1
        if programmatic: return {'Upper':upper, 'Lower':lower}
        res = pd.DataFrame.from_dict({'Upper':upper, 'Lower':lower})
        res.index = raw['Date']
        return res

    def apz(self, raw=None, period=5, df=.092, programmatic=False):
        upper, lower = [], []
        if not raw: raw = self.data
        def __volitality(raw, period=5):
            _, res, ehl = 0, [], self.ma(raw=raw, period=5, favour='e', req_field='hl', programmatic=True)
            while _ < len(ehl):
                if np.isnan(ehl[_]): hdr = np.nan
                else:
                    hdr = np.nan
                    if _ == 2 * period: hdr = np.mean(ehl[_ - period:_])
                    if _ > 2 * period: hdr = (res[-1] * (period - 1) + ehl[_]) / period
                res.append(hdr)
                _ += 1
            return res
        _, vol = 0, __volitality(raw, period)
        while _ < len(vol):
            uhdr, lhdr = np.nan, np.nan
            if not np.isnan(vol[_]):
                uhdr = vol[_] * (1 + df / 2)
                lhdr = vol[_] * (1 - df / 2)
            upper.append(uhdr)
            lower.append(lhdr)
            _ += 1
        if programmatic: return {'Upper':upper, 'Lower':lower}
        res = pd.DataFrame.from_dict({'Upper':upper, 'Lower':lower})
        res.index = raw['Date']
        return res

    def _patr(self, raw=None):
        if not raw: raw = self.data
        lc, lr = raw['Data'][-1, -2], self.atr(raw=raw).values[-1][0]
        _ = [lc + lr, lc, lc - lr]
        _.extend(gslice([lc + lr, lc]))
        _.extend(gslice([lc, lc - lr]))
        _.sort()
        return _

    def _pgap(self, pivot, raw=None):
        if not raw: raw = self.data
        gap = pivot - raw['Data'][-1, -2]
        _ = gslice([pivot + gap, pivot])
        _.extend(gslice([pivot, pivot - gap]))
        _.sort()
        return _

    def ratr(self, raw=None):
        hdr = []
        if not raw: raw = self.data
        [hdr.extend(self._pgap(__, raw)) for __ in self._patr(raw)]
        return pd.Series([hsirnd(_) for _ in lique(hdr)])

    def ovr(self, raw=None, date=datetime.today().date()):
        if not raw: raw = self.data
        if date not in raw['Date']: date = raw['Date'][-1]
        res = {}
        akc = self.kc(raw).transpose()[date]
        aapz = self.apz(raw).transpose()[date]
        abb = self.bb(raw).transpose()[date]
        ami, amx = np.min([akc['Lower'], aapz['Lower'], abb['Lower']]), np.max([akc['Upper'], aapz['Upper'], abb['Upper']])
        if akc['Lower'] == ami: res['min'] = {'KC': hsirnd(ami)}
        if aapz['Lower'] == ami: res['min'] = {'APZ': hsirnd(ami)}
        if abb['Lower'] == ami: res['min'] = {'BB': hsirnd(ami)}
        if akc['Upper'] == amx: res['max'] = {'KC': hsirnd(amx)}
        if aapz['Upper'] == amx: res['max'] = {'APZ': hsirnd(amx)}
        if abb['Upper'] == amx: res['max'] = {'BB': hsirnd(amx)}
        return pd.DataFrame(res)

    def construct(self, raw=None, date=None):
        if not raw: raw = self.data
        if isinstance(date, str): date = datetime.strptime(date, '%Y-%m-%d').date()
        if not date: date = raw['Date'][-1]
        if date not in raw['Date']: date = raw['Date'][-1]
        rdx = raw['Date'].index(date) + 1
        return {'Date': raw['Date'][:rdx], 'Data': raw['Data'][:rdx]}

class Viewer(ONA):
    k_period={'atr':14, 'er':10, 'fast':2, 'slow':30}
    def __init__(self, data):
        self.data = data

    def __del__(self):
        self.data = None
        del(self.data)

    def mas(self, data=None, period=k_period):
        if not data: data = self.data
        _o = ONA(data)
        return _o.kama(period=period).merge(_o.ma(favour='e'), left_index=True, right_index=True).merge(_o.ma(), left_index=True, right_index=True).merge(_o.ma(favour='w'), left_index=True, right_index=True)

    def mapc(self, data=None):
        if not data: data = self.data
        return self.mas(data).pct_change()

    def idrs(self, data=None):
        if not data: data = self.data
        _o = ONA(data)
        return _o.adx().merge(_o.rsi(), left_index=True, right_index=True).merge(_o.atr(), left_index=True, right_index=True)

def hsirnd(value):
    _ = int(np.floor(np.log10(value)))
    __ = np.divmod(value, 10 ** (_ - 1))[0]
    if _ < 0:
        if __ < 25: return np.round(value, 3)
        if __ < 50: return np.round(value * 2, 2) / 2
        return np.round(value, 2)
    if _ == 0: return np.round(value, 2)
    if _ > 3: return np.round(value, 0)
    if _ > 1:
        if __ < 20: return np.round(value, 1)
        return np.round(value * 5, 0) / 5
    if _ > 0:
        if __ < 10: return np.round(value, 2)
        if __ < 20: return np.round(value * 5, 1) / 5
        return np.round(value * 2, 1) / 2
