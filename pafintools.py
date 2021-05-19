import pandas as pd
import numpy as np

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
        sma = self.sma(period, data)
        _ = pd.concat([data, sma], axis=1)
        i, tmp = 0, []
        while i < len(_):
            try:
                if pd.isna(tmp[-1]):
                    v = _.iloc[i, 1]
                else:
                    v = (tmp[-1] * (period - 1) + _.iloc[i, 0]) / period
            except Exception:
                v = _.iloc[i, 1]
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
                if i < period:
                    _ = np.nan
                elif i == period:
                    hdr = source[:i]
                    if direction == '+':
                        _ = hdr[hdr.gt(0)].sum() / period
                    if direction == '-':
                        _ = hdr[hdr.lt(0)].abs().sum() / period
                else:
                    _ = res[i - 1]
                    if direction == '+' and source[i] > 0:
                        _ = (_ * (period - 1) + source[i]) / period
                    if direction == '-' and source[i] < 0:
                        _ = (_ * (period - 1) + abs(source[i])) / period
                res.append(_)
                i += 1
            return res

        ag = avgstep(fd, '+', period)
        al = avgstep(fd, '-', period)

        data['rsi'] = np.apply_along_axis(lambda a, b: 100 - 100 / (1 + a / b),0, ag, al)
        return data.rsi

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
        acp = data.diff(period['er']).fillna(0).abs()
        vot = data.diff().fillna(0).abs().rolling(period['er']).sum()
        er = acp / vot
        sc = (er * (2 / (period['fast'] + 1) - 2 / (period['slow'] + 1)) + 2 / (period['slow'] + 1)) ** 2
        _ = pd.concat([data, sma, sc], axis=1)
        i, tmp = 0, []
        while i < len(_):
            try:
                if pd.isna(tmp[-1]):
                    v = _.iloc[i, 1]
                else:
                    v = tmp[-1] + _.iloc[i, 2] * (_.iloc[i, 0] - tmp[-1])
            except Exception:
                v = _.iloc[i, 1]
            tmp.append(v)
            i += 1
        _['kama'] = tmp
        return _.kama
