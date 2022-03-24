import pandas as pd
from finaux import np, stepper
try:
    from scipy.constants import golden_ratio as gr
except ImportError:
    gr = 1.618


def hsirnd(value):
    if np.isnan(value) or not value > 0:
        return np.nan
    _ = int(np.floor(np.log10(value)))
    __ = np.divmod(value, 10 ** (_ - 1))[0]
    if _ < 0:
        if __ < 25:
            return np.round(value, 3)
        if __ < 50:
            return np.round(value * 2, 2) / 2
        return np.round(value, 2)
    if _ == 0:
        return np.round(value, 2)
    if _ > 3:
        return np.round(value, 0)
    if _ > 1:
        if __ < 20:
            return np.round(value, 1)
        if __ < 50:
            return np.round(value * 5, 0) / 5
        return np.round(value * 2, 0) / 2
    if _ > 0:
        if __ < 10:
            return np.round(value, 2)
        if __ < 20:
            return np.round(value * 5, 1) / 5
        return np.round(value * 2, 1) / 2


class Inspect(object):
    def __init__(self, code=None, xhg='HKEx'):
        if code is None:
            code = list(prefer_stock(xhg).keys())
        self.__data = latest(code, exchange=xhg)

    def close(self):
        return (hsirnd(_['close']) for _ in self.__data)

    def delta(self):
        return (round(_['change'], 3) for _ in self.__data)


def latest(code, period='5d', exchange='HKEx'):
    import yfinance
    keys = ['close', 'change']
    if isinstance(code, (int, str)):
        code = [code]
    if exchange == 'NYSE':
        code_ = [_.upper() for _ in code]
    if exchange == 'HKEx':
        code_ = [f'{_:04d}.HK' for _ in code]
    _ = yfinance.download(code_, period=period, group_by='ticker')
    if exchange == 'NYSE':
        res = [zip(keys, [round(_[__].iloc[-1].Close, 1), round(_[__].Close.diff().iloc[-1], 2)]) for __ in code_]
    if exchange == 'HKEx':
        res = [zip(keys, [hsirnd(_[__].iloc[-1].Close), round(_[__].Close.diff().iloc[-1], 3)]) for __ in code_]
    return [dict(_) for _ in res]


def gap(boundary, ratio=gr):
    rb = boundary
    rb.reverse()
    high, low = rb if boundary[-1] > boundary[0] else boundary
    if ratio > 1:
        ratio = 1 / ratio
    value = (high - low) * ratio
    _ = [low - value, low, low + value, high - value, high, high + value]
    _.sort()
    return _


def prefer_stock(exchange='TSE', file='pref.yaml'):
    if file.split('.')[-1] == 'yaml':
        import yaml
        with open(file, encoding='utf-8') as f:
            _ = yaml.load(f, Loader=yaml.FullLoader)
        return _.get('prefer_stock').get(exchange)


def get_periods(entity='Equities', file='pref.yaml'):
    if file.split('.')[-1] == 'yaml':
        import yaml
        with open(file, encoding='utf-8') as f:
            _ = yaml.load(f, Loader=yaml.FullLoader)
        return _.get('periods').get(entity)


class FOA(object):
    def __init__(self, data, dtype):
        self.__data = data.copy()
        self.dtype = dtype
        self._ta = False
        try:
            import talib
            self._ta = talib
        except ImportError:
            pass

    def sma(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data.close
        _ = data.rolling(period).mean()
        _.name = 'sma'
        return _

    def sar(self, acceleration, maximum, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        _ = data
        _['res'] = np.nan
        return self._ta.SAR(data.high, data.low, acceleration=acceleration, maximum=maximum) if self._ta else _['res']

    def wma(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        _ = (data.close * data.volume).rolling(period).sum() / data.volume.rolling(period).sum()
        _.name = 'wma'
        return _

    def ema(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data.close
        _ = data.to_frame()
        y = stepper(period, data.to_numpy().astype(self.dtype))
        _['ema'] = y
        return _.ema

    def rsi(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        if self._ta:
            _ = self._ta.RSI(data.close, period)
            _.name = 'RSI'
            return _
        else:
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
        if self._ta:
            _ = self._ta.MACD(data.close, period['fast'], period['slow'], period['signal'])
            return pd.DataFrame({'M Line':_[0], 'Signal':_[1], 'Histogram':_[-1]})
        else:
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
        if self._ta:
            _ = self._ta.ATR(data.high, data.low, data.close, period)
            _.name = 'ATR'
            return _
        else:
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
