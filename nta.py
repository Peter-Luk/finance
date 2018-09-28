import numpy as np
import pandas as pd
from utilities import gr

class ONA(object):
    def __init__(self, data):
        self.data = data

    def __del__(self):
        self.data = None
        del(self.data)

    def ma(self, raw=None, period=20, favour='s', req_field='close', programmatic=False):
        if not raw: raw = self.data
        mres = []
        def process(raw, period, favour, req_field):
            res, i = [], 0
            while i < len(raw):
                if i < period - 1: res.append(np.nan)
                else:
                    if req_field.lower() in ['close', 'c']: rdata = raw[i - period + 1: i + 1, -2]
                    if req_field.lower() in ['full', 'f', 'ohlc', 'all']: rdata = raw[i - period + 1: i + 1, :-1].mean(axis=1)
                    if req_field.lower() in ['range', 'hl', 'lh']: rdata = raw[i - period + 1: i + 1, 1:3].mean(axis=1)
                    if favour[0].lower() == 's': res.append(rdata.sum() / period)
                    if favour[0].lower() == 'w': res.append((rdata * raw[i - period + 1: i + 1, -1]).sum() / raw[i - period + 1: i + 1, -1].sum())
                    if favour[0].lower() == 'e':
                        if i == period: hdr = rdata.sum() / period
                        else: hdr = (res[-1] * (period - 1) + rdata[-1]) / period
                        res.append(hdr)
                i += 1
            return res
        rflag = np.isnan(raw['Data']).any(axis=1)
        if rflag.any():
            i = 0
            while i < len(raw['Data'][rflag]):
                mres.append(np.nan)
                i += 1
            mres.extend(process(raw['Data'][~rflag], period, favour, req_field))
        else: mres.extend(process(raw['Data'], period, favour, req_field))
        if programmatic: return mres
        return pd.DataFrame({'{}ma'.format(favour).upper(): mres}, index=raw['Date'])

    def bbw(self, raw=None, period=20, req_field='close', programmatic=False):
        if not raw: raw = self.data
        mres = []
        def process(raw, period, req_field):
            res, i = [], 0
            while i < len(raw):
                if i < period - 1: res.append(np.nan)
                else:
                    if req_field.lower() in ['close', 'c']: rdata = raw[i - period + 1: i + 1, -2]
                    if req_field.lower() in ['full', 'f', 'ohlc', 'all']: rdata = raw[i - period + 1: i + 1, :-1].mean(axis=1)
                    if req_field.lower() in ['range', 'hl', 'lh']: rdata = raw[i - period + 1: i + 1, 1:3].mean(axis=1)
                    res.append(2 * rdata.std())
                i += 1
            return res
        rflag = np.isnan(raw['Data']).any(axis=1)
        if rflag.any():
            i = 0
            while i < len(raw['Data'][rflag]):
                mres.append(np.nan)
                i += 1
            mres.extend(process(raw['Data'][~rflag], period, req_field))
        else: mres.extend(process(raw['Data'], period, req_field))
        if programmatic: return mres
        return pd.DataFrame({'BBW': mres}, index=raw['Date'])

    def kama(self, raw=None, period={'er':10, 'fast':2, 'slow':30}, programmatic=False):
        if not raw: raw = self.data
        mres, sma = [], self.ma(raw, period['slow'], 'e', 'c', True)

        def er(raw, period):
            def dc(raw, lapse=period['er']):
                res, _ = [], 0
                while _ < len(raw):
                    hdr = np.nan
                    if not _ < lapse: hdr = abs(raw[_,-2] - raw[_ - lapse,-2])
                    res.append(hdr)
                    _ += 1
                return res

            res, _, d_close = [], 0, dc(raw)
            while _ < len(raw):
                if _ < period['er']: hdr = np.nan
                else:
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
                    fsc, ssc = 2 / (period['fast'] + 1), 2 / (period['slow'] + 1)
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
        def tr(data):
            nr, res, i = data[:,:-1].ptp(axis=1).tolist(), [], 0
            while i < len(data):
                if i == 0: res.append(nr[i])
                else:
                    hmpc, lmpc = abs(data[i, 1] - data[i - 1, -2]), abs(data[i, 2] - data[i - 1, -2])
                    hdr = hmpc
                    if lmpc > nr[i]:
                        if lmpc > hmpc: hdr = lmpc
                    elif hmpc < nr[i]: hdr = nr[i]
                    res.append(hdr)
                i += 1
            return res

        def process(data, period):
            res, i, truerange = [], 0, tr(data)
            while i < len(data):
                if i < period: hdr = np.nan
                else:
                    if i == period: hdr = np.mean(truerange[:i])
                    else: hdr = (res[-1] * (period - 1) + truerange[i]) / period
                res.append(hdr)
                i += 1
            return res

        rflag = np.isnan(raw['Data']).any(axis=1)
        if rflag.any():
            i = 0
            while i < len(raw['Data'][rflag]):
                mres.append(np.nan)
                i += 1
            mres.extend(process(raw['Data'][~rflag], period))
        else: mres.extend(process(raw['Data'], period))
        if programmatic: return mres
        return pd.DataFrame({'ATR': mres}, index=raw['Date'])

    def adx(self, raw=None, period=14):
        if not raw: raw = self.data
        mres = []
        def tr(data):
            nr, res, i = data[:,:-1].ptp(axis=1).tolist(), [], 0
            while i < len(data):
                if i == 0: res.append(nr[i])
                else:
                    hmpc, lmpc = abs(data[i, 1] - data[i - 1, -2]), abs(data[i, 2] - data[i - 1, -2])
                    hdr = hmpc
                    if lmpc > nr[i]:
                        if lmpc > hmpc: hdr = lmpc
                    elif hmpc < nr[i]: hdr = nr[i]
                    res.append(hdr)
                i += 1
            return res

        def dm(data):
            i, res = 1, {'+':[0], '-':[0]}
            while i < len(data):
                dh, dl = data[i][1] - data[i - 1][1], data[i - 1][2] - data[i][2]
                if dh > dl and dh > 0: res['+'].append(dh)
                else: res['+'].append(0)
                if dl > dh and dl > 0: res['-'].append(dl)
                else: res['-'].append(0)
                i += 1
            return res

        def di(data, period=period):
            res, i, dir_mov, true_range = {'+':[np.nan], '-':[np.nan]}, 0, dm(data), tr(data)
            while i < len(dir_mov['+']):
                if i < period:
                    phdr = np.nan
                    mhdr = np.nan
                else:
                    phdr = sum(dir_mov['+'][i - period:i]) / sum(true_range[i - period + 1:i + 1])
                    mhdr = sum(dir_mov['-'][i - period:i]) / sum(true_range[i - period + 1:i + 1])
                res['+'].append(phdr)
                res['-'].append(mhdr)
                i += 1
            return res

        def dx(raw, period):
            i, res, dip = 0, [], di(raw, period)
            while i < len(raw):
                if np.isnan(dip['+'][i]) or np.isnan(dip['-'][i]): hdr = np.nan
                else:
                    hdr = abs(dip['+'][i] - dip['-'][i]) / (dip['+'][i] + dip['-'][i]) * 100
                res.append(hdr)
                i += 1
            return res

        def process(data, period):
            res, nc, i, vdx = [], 0, 0, dx(data, period)
            while i < len(data):
                if np.isnan(vdx[i]):
                    hdr = np.nan
                    nc += 1
                else:
                    if i < period + nc: hdr = np.nan
                    else:
                        if i == period + nc: hdr = np.mean(vdx[nc:i])
                        else: hdr = (res[-1] * (period - 1) + vdx[i]) / period
                res.append(hdr)
                i += 1
            return res

        rflag = np.isnan(raw['Data']).any(axis=1)
        if rflag.any():
            i = 0
            while i < len(raw['Data'][rflag]):
                mres.append(np.nan)
                i += 1
            mres.extend(process(raw['Data'][~rflag], period))
        else: mres.extend(process(raw['Data'], period))
        return pd.DataFrame({'ADX': mres}, index=raw['Date'])

    def rsi(self, raw=None, period=14):
        if not raw: raw = self.data
        mres = []
        def process(raw, period):
            i, gain, loss, res = 0, [], [], []
            while i < len(raw):
                if i == 0:
                    if raw[i, -2] > raw[i, 0]:
                        gain.append(raw[i, -2] - raw[i, 0])
                        loss.append(0)
                    elif raw[i, -2] == raw[i, 0]:
                        gain.append(0)
                        loss.append(0)
                    else:
                        gain.append(0)
                        loss.append(raw[i, 0] - raw[i, -2])
                else:
                    if raw[i, -2] > raw[i - 1, -2]:
                        gain.append(raw[i, -2] - raw[i - 1, -2])
                        loss.append(0)
                    elif raw[i, -2] == raw[i - 1, -2]:
                        gain.append(0)
                        loss.append(0)
                    else:
                        gain.append(0)
                        loss.append(raw[i -1, -2] - raw[i, -2])
                i += 1
            i = 0
            while i < len(raw):
                if i < period: res.append(np.nan)
                elif i == period: res.append(100 - 100 / ((1 + np.mean(gain[:i]) / np.mean(loss[:i]))))
                else:
                    res.append(100 - 100 / (( 1 + np.mean(gain[i - period:i]) / np.mean(loss[i - period:i]))))
                i += 1
            return res

        rflag = np.isnan(raw['Data']).any(axis=1)
        if rflag.any():
            i = 0
            while i < len(raw['Data'][rflag]):
                mres.append(np.nan)
                i += 1
            mres.extend(process(raw['Data'][~rflag], period))
        else: mres.extend(process(raw['Data'], period))
        return pd.DataFrame({'RSI': mres}, index=raw['Date'])

    def kc(self, raw=None, period={'atr':14, 'er':10, 'fast':2, 'slow':30}, ratio=gr, programmatic=False):
        upper, lower = [], []
        if not raw: raw = self.data
        kma, ar = self.kama(raw=raw, period={'er':period['er'], 'fast':period['fast'], 'slow':period['slow']}, programmatic=True), self.atr(raw=raw, period=period['atr'], programmatic=True)
        i = 0
        while i < len(kma):
            uhdr, lhdr = np.nan, np.nan
            if not np.isnan([kma[i], ar[i]]).any():
                uhdr = kma[i] + ratio * ar[i]
                lhdr = kma[i] - ratio * ar[i]
            upper.append(uhdr)
            lower.append(lhdr)
            i += 1
        if programmatic: return {'Upper':upper, 'Lower':lower}
        res = pd.DataFrame.from_dict({'Upper':upper, 'Lower':lower})
        res.index = raw['Date']
        return res

    def bb(self, raw=None, period=20, req_field='c', programmatic=False):
        upper, lower = [], []
        if not raw: raw = self.data
        sma, bw = self.ma(raw=raw, period=period, req_field=req_field, programmatic=True), self.bbw(raw=raw, period=period, req_field=req_field, programmatic=True)
        i = 0
        while i < len(sma):
            uhdr, lhdr = np.nan, np.nan
            if not np.isnan([sma[i], bw[i]]).any():
                uhdr = sma[i] + bw[i] / 2
                lhdr = sma[i] - bw[i] / 2
            upper.append(uhdr)
            lower.append(lhdr)
            i += 1
        if programmatic: return {'Upper':upper, 'Lower':lower}
        res = pd.DataFrame.from_dict({'Upper':upper, 'Lower':lower})
        res.index = raw['Date']
        return res

    def apz(self, raw=None, period=5, df=.092, programmatic=False):
        upper, lower = [], []
        if not raw: raw = self.data
        def volitality(raw, period=5):
            res, ehl = [], self.ma(raw=raw, period=5, favour='e', req_field='hl', programmatic=True)
            i = 0
            while i < len(ehl):
                if np.isnan(ehl[i]): hdr = np.nan
                else:
                    if i < 2 * period: hdr = np.nan
                    elif i == 2 * period: hdr = np.mean(ehl[i - period:i])
                    else: hdr = (res[-1] * (period - 1) + ehl[i]) / period
                res.append(hdr)
                i += 1
            return res
        i, vol = 0, volitality(raw, period)
        while i < len(vol):
            uhdr, lhdr = np.nan, np.nan
            if not np.isnan(vol[i]):
                uhdr = vol[i] * (1 + df / 2)
                lhdr = vol[i] * (1 - df / 2)
            upper.append(uhdr)
            lower.append(lhdr)
            i += 1
        if programmatic: return {'Upper':upper, 'Lower':lower}
        res = pd.DataFrame.from_dict({'Upper':upper, 'Lower':lower})
        res.index = raw['Date']
        return res

class Viewer(ONA):
    def __init__(self, code):
        self.code = code

    def __del__(self):
        self.code = None
        del(self.code)

    def mas(self, code=None):
        if not code: code = self.code
        dcode = ONA(code)
        return dcode.kama().merge(dcode.ma(favour='e'), left_index=True, right_index=True).merge(dcode.ma(), left_index=True, right_index=True)

    def idrs(self, code=None):
        if not code: code = self.code
        dcode = ONA(code)
        return dcode.adx().merge(dcode.rsi(), left_index=True, right_index=True).merge(dcode.atr(), left_index=True, right_index=True)
