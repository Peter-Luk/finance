import numpy as np
import pandas as pd
from utilities import datetime, gslice, gr, lique


class ONA(object):
    def __init__(self, data, date=datetime.today().date()):
        self.data = data
        self.date = date
        if date not in self.data['Date']: self.date = self.data['Date'][-1]
        self.data = self.construct(self.data, self.date)

    def __del__(self):
        self.data = self.date = None
        del(self.data, self.date)

    def ma(self, raw, period, favour='s', req_field='close', programmatic=False):
        mres = []
        def process(raw, period, favour, req_field):
            res, _ = [], 0
            while _ < len(raw):
                hdr = np.nan
                if not _ < period - 1:
                    if req_field.lower() in ['close', 'c']:
                        rdata = raw[_ - period + 1: _ + 1, -2]
                        pdata = raw[_ - period + 1: _ + 1, -2:].prod(axis=1)
                        wdata = raw[_ - period + 1: _ + 1, -1]
                    if req_field.lower() in ['full', 'f', 'ohlc', 'all']: rdata = raw[_ - period + 1: _ + 1, :-1].mean(axis=1)
                    if req_field.lower() in ['range', 'hl', 'lh']: rdata = raw[_ - period + 1: _ + 1, 1:3].mean(axis=1)
                    if favour[0].lower() == 's': hdr = rdata.mean()
                    # if favour[0].lower() == 'w': hdr = (rdata * raw[_ - period + 1: _ + 1, -1]).sum() / raw[_ - period + 1: _ + 1, -1].sum()
                    if favour[0].lower() == 'w': hdr = pdata.sum() / wdata.sum()
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
        return pd.DataFrame({f'{favour}ma'.upper(): mres}, index=raw['Date'])

    def bbw(self, raw, period, req_field='close', programmatic=False):
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

    def kama(self, raw, period, programmatic=False):
        mres, sma = [], self.ma(raw, period['simple'], favour='e', req_field='close', programmatic=True)

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

    def atr(self, raw, period, programmatic=False):
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

    def adx(self, raw, period):
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

    def rsi(self, raw, period):
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

    def kc(self, raw, period, ratio=gr/2, programmatic=False):
        upper, lower = [], []
        _, kma, ar = 0, self.kama(raw, period, programmatic=True), self.atr(raw, period['atr'], programmatic=True)
        while _ < len(kma):
            uhdr, lhdr = np.nan, np.nan
            if not np.isnan([kma[_], ar[_]]).any():
                uhdr = kma[_] + ratio * ar[_]
                lhdr = kma[_] - ratio * ar[_]
            upper.append(uhdr)
            lower.append(lhdr)
            _ += 1
        if programmatic: return {'Upper':upper, 'Lower':lower}
        res = pd.DataFrame.from_dict({'Upper':[hsirnd(_) for _ in upper], 'Lower':[hsirnd(_) for _ in lower]})
        res.index = raw['Date']
        return res

    def bb(self, raw, period, req_field='c', programmatic=False):
        upper, lower = [], []
        sma, bw = self.ma(raw, period, req_field=req_field, programmatic=True), self.bbw(raw, period, req_field, programmatic=True)
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
        res = pd.DataFrame.from_dict({'Upper':[hsirnd(_) for _ in upper], 'Lower':[hsirnd(_) for _ in lower]})
        res.index = raw['Date']
        return res

    def apz(self, raw, period, df=None, programmatic=False):
        upper, lower = [], []
        if not df:
            date = raw['Date'][-1]
            df = self.atr(raw, period['atr'])['ATR'][date] / raw['Data'][raw['Date'].index(date), -2]

        def __pema(pd_data, period):
            data, res, c = [_[0] for _ in pd_data.values], [], 0
            for i in range(len(data)):
                hdr = np.nan
                if not np.isnan(data[i]):
                    if c == period:
                        hdr = np.array(data[i - period: i]).mean()
                    if c > period:
                        hdr = (res[-1] * (period - 1) + data[i]) / period
                    c += 1
                res.append(hdr)
            return pd.Series(res, index=pd_data.index)

        def __volitality(raw, period):
            _, res, ehl = 0, [], self.ma(raw, period, favour='e', req_field='hl', programmatic=True)
            while _ < len(ehl):
                if np.isnan(ehl[_]): hdr = np.nan
                else:
                    hdr = np.nan
                    if _ == 2 * period: hdr = np.mean(ehl[_ - period:_])
                    if _ > 2 * period: hdr = (res[-1] * (period - 1) + ehl[_]) / period
                res.append(hdr)
                _ += 1
            return res
        _, vol = 0, __volitality(raw, period['apz'])
        em5 = __pema(self.ma(raw, period['apz'], favour='e'), period['apz'])
        while _ < len(vol):
            ev = em5[self.data['Date'][_]]
            uhdr, lhdr = np.nan, np.nan
            if not np.isnan(vol[_]):
                uhdr = ev + vol[_] * df
                lhdr = ev - vol[_] * df
            upper.append(uhdr)
            lower.append(lhdr)
            _ += 1
        if programmatic: return {'Upper':upper, 'Lower':lower}
        res = pd.DataFrame.from_dict({'Upper':[hsirnd(_) for _ in upper], 'Lower':[hsirnd(_) for _ in lower]})
        res.index = raw['Date']
        return res

    def ratr(self, raw, period):
        def _patr(raw, period):
            lc, lr = raw['Data'][-1, -2], self.atr(raw, period).values[-1][0]
            _ = [lc + lr, lc, lc - lr]
            _.extend(gslice([lc + lr, lc]))
            _.extend(gslice([lc, lc - lr]))
            _.sort()
            return _

        def _pgap(pivot, raw):
            gap = pivot - raw['Data'][-1, -2]
            _ = gslice([pivot + gap, pivot])
            _.extend(gslice([pivot, pivot - gap]))
            _.sort()
            return _

        hdr = []
        if not raw: raw = self.data
        [hdr.extend(_pgap(__, raw)) for __ in _patr(raw, period)]
        return pd.Series(lique([hsirnd(_) for _ in hdr]))

    def ovr(self, raw, period, date=datetime.today().date()):
        if date not in raw['Date']: date = raw['Date'][-1]
        res = {}
        akc = self.kc(raw, period).transpose()[date]
        aapz = self.apz(raw, period).transpose()[date]
        abb = self.bb(raw, period['simple']).transpose()[date]
        ami, amx = np.min([akc['Lower'], aapz['Lower'], abb['Lower']]), np.max([akc['Upper'], aapz['Upper'], abb['Upper']])
        if akc['Lower'] == ami: res['min'] = {'KC': ami}
        if aapz['Lower'] == ami: res['min'] = {'APZ': ami}
        if abb['Lower'] == ami: res['min'] = {'BB': ami}
        if akc['Upper'] == amx: res['max'] = {'KC': amx}
        if aapz['Upper'] == amx: res['max'] = {'APZ': amx}
        if abb['Upper'] == amx: res['max'] = {'BB': amx}
        return pd.DataFrame(res)

    def construct(self, raw, date=None):
        if not raw: raw = self.data
        if isinstance(date, str): date = datetime.strptime(date, '%Y-%m-%d').date()
        if not date: date = raw['Date'][-1]
        if date not in raw['Date']: date = raw['Date'][-1]
        rdx = raw['Date'].index(date) + 1
        return {'Date': raw['Date'][:rdx], 'Data': raw['Data'][:rdx]}

    def trp(self, data, period):
        if not data: data = self.data
        _atr = self.atr(data, period)
        hdr = [_atr['ATR'][_] / self.data['Data'][self.data['Date'].index(_), -2] * 100 for _ in _atr.index]
        return pd.DataFrame({'TRP':hdr}, index=_atr.index)

class Viewer(ONA):
    def __init__(self, data):
        self.data = data

    def __del__(self):
        self.data = None
        del(self.data)

    def mas(self, data, period):
        _o = ONA(data)
        return pd.DataFrame([_o.kama(data, period)['KAMA'].map(hsirnd), _o.ma(data, period['simple'], favour='e')['EMA'].map(hsirnd), _o.ma(data, period['simple'])['SMA'].map(hsirnd), _o.ma(data, period['simple'], favour='w')['WMA'].map(hsirnd)]).T

    def idrs(self, data, period):
        _o = ONA(data)
        return _o.adx(data, period['adx']).merge(_o.rsi(data, period['simple']), left_index=True, right_index=True).merge(_o.atr(data, period['atr']), left_index=True, right_index=True).merge(_o.trp(data, period['atr']), left_index=True, right_index=True)

    def best_quote(self, action='buy', bound=True):
        er, eo = self.ratr(), self.ovr()
        _ = er[(er > eo['min'].min()) & (er < eo['max'].max())]
        if action == 'buy':
            if bound:
                if self.close > _.min(): return pd.Series([__ for __ in er if __ > _.min()]).min()
                return np.nan
            return er.min()
        if action == 'sell':
            if bound:
                if self.close < _.max(): return pd.Series([__ for __ in er if __ < _.max()]).max()
                return np.nan
            return er.max()

def hsirnd(value):
    if np.isnan(value): return np.nan
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
