import pandas as pd
import numpy as np
from scipy.constants import golden_ratio as gr
from sys import platform

class FOA(object):
    def __init__(self, data):
        self.__data = data

    def sma(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data.close
        _ = data.rolling(period).mean()
        _.name = 'sma'
        return _

    def wma(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        _ = (data.close * data.volume).rolling(period).sum() / data.volume.rolling(period).sum()
        _.name = 'wma'
        return _

    def ema(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data.close
        if platform == 'linux':
            _ = data.to_frame()
            y = stepper(period, data.to_numpy())
            _['ema'] = y
        else:
            sma = self.sma(period, data)
            _ = pd.concat([data, sma], axis=1)
            i, tmp = 0, []
            while i < len(_):
                v = _.iloc[i, 1]
                if i > period:
                    v = (tmp[-1] * (period - 1) + _.iloc[i, 0]) / period
                tmp.append(v)
                i += 1
            _['ema'] = tmp
        return _.ema

    def rsi(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        fd = data.close.diff()

        def avgstep(source, direction, period):
            i, res = 0, []

            while i < len(source):
                _, hdr = np.nan, source[:i]
                if i == period:
                    if direction == '+':
                        _ = hdr[hdr.gt(0)].sum() / period
                    if direction == '-':
                        _ = hdr[hdr.lt(0)].abs().sum() / period
                if i > period:
                    _, hdr = res[-1], 0
                    if direction == '+' and source[i] > 0:
                        hdr = source[i]
                    if direction == '-' and source[i] < 0:
                        hdr = abs(source[i])
                    _ = (_ * (period - 1) + hdr) / period
                res.append(_)
                i += 1
            return res

        ag = avgstep(fd, '+', period)
        al = avgstep(fd, '-', period)

        data['rsi'] = np.apply_along_axis(lambda a, b: 100 - 100 / (1 + a / b),0, ag, al)
        return data.rsi

    def macd(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        hl = (data.high + data.low) / 2
        e_slow = self.ema(period['slow'], hl)
        e_fast = self.ema(period['fast'], hl)
        m_line = e_fast - e_slow
        s_line = self.ema(period['signal'], m_line[period['slow']:])
        m_hist = m_line - s_line
        _ = pd.concat([m_line, s_line, m_hist], axis=1)
        _.columns = ['M Line', 'Signal', 'Histogram']
        return _

    def atr(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        pc = data.close.shift()
        _ = pd.concat([data.high - data.low, (data.high - pc).abs(), (data.low - pc).abs()], axis=1).max(axis=1)
        _['atr'] = self.ema(period, _)
        _.atr.name = 'atr'
        return _.atr

    def kama(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data.close
        sma = self.sma(period['er'], data)
        acp = data.diff(period['er']).abs()[period['er']:]
        vot = data.diff(1).abs()[1:].rolling(period['er']).sum()
        er = acp / vot
        sc = (er * (2 / (period['fast'] + 1) - 2 / (period['slow'] + 1)) + 2 / (period['slow'] + 1)) ** 2
        _ = pd.concat([data, sma, sc], axis=1)
        i, tmp = 0, []
        while i < len(_):
            v = _.iloc[i, 1]
            if i > 0 and pd.notna(tmp[-1]):
                v = tmp[-1] + _.iloc[i, 2] * (_.iloc[i, 0] - tmp[-1])
            tmp.append(v)
            i += 1
        _['kama'] = tmp
        return _.kama

    def soc(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        ml = data.low.rolling(period['K']).min()
        mh = data.high.rolling(period['K']).max()
        data['kseries'] = np.apply_along_axis(lambda a, b, c: (a - c) /(b - c) * 100, 0, data.close, mh, ml)
        k = data.kseries.rolling(period['D']).mean()
        k.name = '%K'
        d = k.rolling(period['D']).mean()
        d.name = '%D'
        _ = pd.concat([k, d], axis=1)
        _.name = 'SOC'.lower()
        return _

    def stc(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        hl = (data.high + data.low) / 2
        e_slow = self.ema(period['slow'], hl)
        e_fast = self.ema(period['fast'], hl)
        m_line = e_fast.sub(e_slow)
        mh = m_line.rolling(period['K']).max()
        ml = m_line.rolling(period['K']).min()
        kseries = (m_line - ml) / (mh - ml)
        k = kseries.rolling(period['D']).mean()
        k.name = '%K'
        d = k.rolling(period['D']).mean()
        d.name = '%D'
        _ = (m_line - k) / (d - k)
        _.name = 'STC'.lower()
        return _

    def adx(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        atr = self.atr(period)
        _ = data.high.diff(1)
        _[_<0] = 0
        dm_plus = _[1:]
        _ = data.low.diff(1)
        _[_<0] = 0
        dm_minus = _[1:]
        di_plus = self.ema(period, dm_plus) / atr * 100
        di_plus.name = f'+DI{period:02d}'

        di_minus = self.ema(period, dm_minus) / atr * 100
        di_minus.name = f'-DI{period:02d}'

        dx = (di_plus - di_minus).abs() / (di_plus + di_minus) * 100
        _ = self.ema(period, dx[period + 1:])
        _.name = f'ADX{period:02d}'
        __ = pd.concat([di_plus, di_minus, _], axis=1)
        return __

    def kc(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        hl = (data.high + data.low) / 2
        middle_line = self.kama(period['kama'], hl)
        atr = self.atr(period['atr'], data)
        upper = middle_line + (gr * atr)
        lower = middle_line - (gr * atr)
        _ = pd.concat([upper, lower], axis=1)
        _.columns = ['Upper', 'Lower']
        return _

    def apz(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        hl = (data.high + data.low) / 2
        ehl = self.ema(period, hl)
        volatility = self.ema(2 * period, ehl[period:])
        tr = pd.concat(
            [data.high - data.low,
                (data.high - data.close.shift(1)).abs(),
                (data.low - data.close.shift(1)).abs()], 1).max(axis=1)

        upper = volatility + tr * gr
        lower = volatility - tr * gr
        _ = pd.concat([upper, lower], axis=1)
        _.columns = ['Upper', 'Lower']
        return _

    def dc(self, period, data=None):
        # Donchian Channel
        if isinstance(data, type(None)):
            data = self.__data
        pax = data.high.rolling(period).max()
        pin = data.low.rolling(period).min()
        _ = pd.concat([pax, pin], axis=1)
        _.columns = ['Upper', 'Lower']
        return _

    def bb(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data.close
        ml = self.sma(period, data)
        wd = data.rolling(period).std()
        _ = pd.concat([ml + wd, ml - wd], 1)
        _.columns = ['Upper', 'Lower']
        return _

    def obv(self, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        dcp = data.close.diff()
        tmp, i = [], 0
        while i < len(data.volume):
            _ = data.volume.iloc[i]
            if i > 0:
                _ = tmp[i - 1]
                if dcp.iloc[i] > 0:
                    _ += data.volume.iloc[i]
                if dcp.iloc[i] < 0:
                    _ -= data.volume.iloc[i]
            tmp.append(_)
            i += 1
        data['obv'] = tmp
        return data['obv']

    def vwap(self, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        pv = data.drop(['open', 'volume'], 1).mean(axis=1) * data.volume
        data['vwap'] = pv.cumsum() / data.volume.cumsum()
        return data['vwap']

if platform == 'linux':
    from numba import jit
    @jit(nopython=True)
    def stepper(period, x):
        y = np.empty(x.size)
        y[:] = np.nan
        i = 0
        while i < x.size:
            if i > period:
                y[i] = (y[i - 1] * (period - 1) + x[i]) / period
            elif i == period:
                y[i] = x[:i].mean()
            i += 1
        return y
