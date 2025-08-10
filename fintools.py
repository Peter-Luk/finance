import pandas as pd
import numpy as np
from typing import Iterable
# from rich.console import Console
from rich.table import Table
from benedict import benedict
from finaux import stepper
from utilities import YAML, PORTFOLIO_PATH

try:
    from scipy.constants import golden_ratio as gr
except ImportError:
    gr = 1.618


def hsirnd(value: float) -> float:
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


class Portfolio:
    def __init__(self, client: str, boarse: str = 'HKEx'):
        self.client = client.upper()
        self.boarse = boarse
        self.__filepath = PORTFOLIO_PATH
        self.__porfolio = benedict.from_yaml(self.__filepath)

    def add(self, code: int|str|Iterable[str|int]):
        holder = self.__porfolio[self.client][self.boarse]
        match code:
            case str()|int():
                if code not in holder:
                    holder.append(code)
            case list():
                if (c_ := [_ for _ in code if _ not in holder]):
                    holder.extend(c_)
        holder.sort()
        self.__porfolio[self.client][self.boarse] = holder
        self.__porfolio.to_yaml(filepath=self.__filepath)

    def remove(self, code: int|str|Iterable[int|str]):
        match code:
            case str()|int():
                holder = list(filter(lambda _: _ != code, self.__porfolio[self.client][self.boarse]))
            case list():
                holder = [_ for _ in self.__porfolio[self.client][self.boarse] if _ not in code]
        self.__porfolio[self.client][self.boarse] = holder
        self.__porfolio.to_yaml(filepath=self.__filepath)

    def __call__(self) -> list:
        return self.__porfolio[self.client][self.boarse]


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


def gap(boundary: Iterable, ratio: float=gr) -> Iterable:
    rb = boundary
    rb.reverse()
    high, low = rb if boundary[-1] > boundary[0] else boundary
    if ratio > 1:
        ratio = 1 / ratio
    value = (high - low) * ratio
    _ = [low - value, low, low + value, high - value, high, high + value]
    _.sort()
    return _


def mplot(df, last=200, sar=True):
    import mplfinance as mpf
    adps = [mpf.make_addplot(df.kc()[-last:], color='blue'), \
        mpf.make_addplot(df.kama()[-last:], color='silver'), \
        mpf.make_addplot(df.rsi()[-last:], panel=1, color='green')]
    if sar:
        adps.append(mpf.make_addplot(df.sar()[-last:], type='scatter', marker='o', markersize=1, color='black'))

    tt = df.code
    try:
        tt = df.yahoo_code
    except Exception:
        pass
    colors = mpf.make_marketcolors(up='#00ff00', down='#ff0000')
    mpf_style = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=colors)
    mpf.plot(df()[-last:], type='candle', style=mpf_style, addplot=adps, title=tt, volume=True, ylabel='Price')


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
            try:
                data = self.__data.close
            except Exception:
                data = self.__data.Close
        _ = data.rolling(period).mean()
        _.name = 'sma'
        return _

    def sar(self, acceleration, maximum, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        _ = data
        _['res'] = np.nan
        try:
            result = self._ta.SAR(data.High, data.Low, acceleration=acceleration, maximum=maximum) if self._ta else _['res']
        except Exception:
            result = self._ta.SAR(data.high, data.low, acceleration=acceleration, maximum=maximum) if self._ta else _['res']
        result.name = 'SAR'
        return result

    def wma(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        try:
            _ = (data.close * data.volume).rolling(period).sum() / data.volume.rolling(period).sum()
        except Exception:
            _ = (data.Close * data.Volume).rolling(period).sum() / data.Volume.rolling(period).sum()
        _.name = 'wma'
        return _

    def ema(self, period, data=None):
        if isinstance(data, type(None)):
            try:
                data = self.__data.close
            except Exception:
                data = self.__data.Close
        _ = data.to_frame()
        y = stepper(period, data.to_numpy().astype(self.dtype))
        _['ema'] = y
        return _.ema

    def rsi(self, period, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        if self._ta:
            try:
                _ = self._ta.RSI(data.close, period)
            except Exception:
                _ = self._ta.RSI(data.Close, period)
            _.name = 'RSI'
            return _
        else:
            try:
                fd = data.close.diff()
            except Exception:
                fd = data.Close.diff()

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
                        if direction == '+' and source.iloc[i] > 0:
                            hdr = source.iloc[i]
                        if direction == '-' and source.iloc[i] < 0:
                            hdr = abs(source.iloc[i])
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
            try:
                _ = self._ta.MACD(data.close, period['fast'], period['slow'], period['signal'])
                return pd.DataFrame({'M Line':_[0], 'Signal':_[1], 'Histogram':_[-1]})
            except Exception:
                _ = self._ta.MACD(data.Close, period['fast'], period['slow'], period['signal'])
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
            try:
                _ = self._ta.ATR(data.high, data.low, data.close, period)
            except Exception:
                _ = self._ta.ATR(data.High, data.Low, data.Close, period)
            _.name = 'ATR'
            return _
        else:
            try:
                pc = data.close.shift()
                _ = pd.concat([data.high - data.low, (data.high - pc).abs(), (data.low - pc).abs()], axis=1).max(axis=1)
            except Exception:
                pc = data.Close.shift()
                _ = pd.concat([data.High - data.Low, (data.High - pc).abs(), (data.Low - pc).abs()], axis=1).max(axis=1)
            _['atr'] = self.ema(period, _)
            _.atr.name = 'atr'
            return _.atr

    def kama(self, period, data=None):
        if isinstance(data, type(None)):
            try:
                data = self.__data.close
            except Exception:
                data = self.__data.Close
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
        try:
            ml = data.low.rolling(period['K']).min()
            mh = data.high.rolling(period['K']).max()
            data['kseries'] = np.apply_along_axis(lambda a, b, c: (a - c) /(b - c) * 100, 0, data.close, mh, ml)
        except Exception:
            ml = data.Low.rolling(period['K']).min()
            mh = data.High.rolling(period['K']).max()
            data['kseries'] = np.apply_along_axis(lambda a, b, c: (a - c) /(b - c) * 100, 0, data.Close, mh, ml)
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
        try:
            hl = (data.high + data.low) / 2
        except Exception:
            hl = (data.High + data.Low) / 2
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
        try:
            _ = data.high.diff(1)
        except Exception:
            _ = data.High.diff(1)
        _[_<0] = 0
        dm_plus = _[1:]
        try:
            _ = data.low.diff(1)
        except Exception:
            _ = data.Low.diff(1)
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
        try:
            hl = (data.high + data.low) / 2
        except Exception:
            hl = (data.High + data.Low) / 2
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
        try:
            hl = (data.high + data.low) / 2
        except Exception:
            hl = (data.High + data.Low) / 2
        ehl = self.ema(period, hl)
        volatility = self.ema(2 * period, ehl[period:])
        try:
            tr = pd.concat(
                [data.high - data.low,
                    (data.high - data.close.shift(1)).abs(),
                    (data.low - data.close.shift(1)).abs()], axis=1).max(axis=1)
        except Exception:
            tr = pd.concat(
                [data.High - data.Low,
                    (data.High - data.Close.shift(1)).abs(),
                    (data.Low - data.Close.shift(1)).abs()], axis=1).max(axis=1)

        upper = volatility + tr * gr
        lower = volatility - tr * gr
        _ = pd.concat([upper, lower], axis=1)
        _.columns = ['Upper', 'Lower']
        return _

    def dc(self, period, data=None):
        # Donchian Channel
        if isinstance(data, type(None)):
            data = self.__data
        try:
            pax = data.high.rolling(period).max()
            pin = data.low.rolling(period).min()
        except Exception:
            pax = data.High.rolling(period).max()
            pin = data.Low.rolling(period).min()
        _ = pd.concat([pax, pin], axis=1)
        _.columns = ['Upper', 'Lower']
        return _

    def bb(self, period, data=None):
        if isinstance(data, type(None)):
            try:
                data = self.__data.close
            except Exception:
                data = self.__data.Close
        ml = self.sma(period, data)
        wd = data.rolling(period).std()
        _ = pd.concat([ml + wd, ml - wd], 1)
        _.columns = ['Upper', 'Lower']
        return _

    def obv(self, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        try:
            dcp = data.close.diff()
        except Exception:
            dcp = data.Close.diff()
        tmp, i = [], 0
        try:
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
        except Exception:
            while i < len(data.Volume):
                _ = data.Volume.iloc[i]
                if i > 0:
                    _ = tmp[i - 1]
                    if dcp.iloc[i] > 0:
                        _ += data.Volume.iloc[i]
                    if dcp.iloc[i] < 0:
                        _ -= data.Volume.iloc[i]
                tmp.append(_)
                i += 1
        data['obv'] = tmp
        return data['obv']

    def vwap(self, data=None):
        if isinstance(data, type(None)):
            data = self.__data
        try:
            pv = data.drop(['open', 'volume'], 1).mean(axis=1) * data.volume
            data['vwap'] = pv.cumsum() / data.volume.cumsum()
        except Exception:
            pv = data.drop(['Open', 'Volume'], 1).mean(axis=1) * data.Volume
            data['vwap'] = pv.cumsum() / data.Volume.cumsum()
        return data['vwap']


def parabolic_sar(high: pd.DataFrame,
        low: pd.DataFrame,
        af_start: float=0.02,
        af_step: float=0.02,
        af_max: float=0.2) -> pd.DataFrame:
    sar = [low[0]]  # Initial SAR
    trend = 1  # 1 for uptrend, -1 for downtrend
    ep = high[0] if trend == 1 else low[0]  # Extreme point
    af = af_start
                        
    for i in range(1, len(high)):
        sar_current = sar[-1] + af * (ep - sar[-1])
                                                
        # Check reversal
        if (trend == 1 and low[i] < sar_current) or (trend == -1 and high[i] > sar_current):
            trend *= -1
            sar_current = ep
            af = af_start
            ep = high[i] if trend == 1 else low[i]
        else:
            # Update EP and AF in trend
            if trend == 1:
                if high[i] > ep:
                    ep = high[i]
                    af = min(af + af_step, af_max)
                else:
                    if low[i] < ep:
                        ep = low[i]
                        af = min(af + af_step, af_max)
        sar.append(sar_current)
    return sar


def construct_report(df: pd.DataFrame,
        code: str) -> str:
    def get_currency(code: str) -> str:
        currency = 'USD'
        i_split = code.split('.')
        if len(i_split) == 2:
            match i_split[-1]:
                case 'HK':
                    currency = 'HKD'
                case 'T':
                    currency = 'JPY'
                case 'DE':
                    currency = 'EUR'
                case 'L':
                    currency = 'GBP'
        return currency

    params = YAML.periods.Equities
    currency = get_currency(code)
    data = df.xs(code, axis=1, level='Ticker')
    last_trade = data.index[-1].to_pydatetime()
    close = data.Close.loc[last_trade]
    change = data.Close.pct_change(fill_method=None).loc[last_trade]
    _ = FOA(data, dtype='float64')
    sar = _.sar(params.sar.acceleration, params.sar.maximum).loc[last_trade]
    kama = _.kama(params.kama).loc[last_trade]
    rsi = _.rsi(params.rsi).loc[last_trade]
    # """
    # console = Console()
    table = Table(title=f"{code} {last_trade:%Y-%m-%d} ({currency})")
    table.add_column("Close", style="cyan")
    table.add_column("RSI", justify="right", style="magenta")
    table.add_column("SAR", justify="right", style="magenta")
    table.add_column("KAMA", justify="right", style="magenta")
    table.add_row(f"{close:,.2f} ({change:+0.3%})",
        f"{rsi:0.3f}",
        f"{sar:0.2f}",
        f"{kama:0.2f}")
    return table
    # console.print(table)
    # """
    # return f"{code} {last_trade:%d-%m-%Y}: close @ {currency} {close:,.2f} ({change:0.3%}), rsi: {rsi:0.3f}, sar: {currency} {sar:,.2f} and KAMA: {currency} {kama:,.2f}"
