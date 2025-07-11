import pandas as pd
import numpy as np

from datetime import datetime
from utilities import gslice
from fintools import hsirnd

try:
    from scipy.constants import golden_ratio as gr
except ImportError:
    gr = 1.618


class ONA(object):
    def __init__(self, data, date=datetime.today().date()):
        self.data = data
        self.date = date
        if date not in self.data.index:
            self.date = self.data.index[-1]

    def __del__(self):
        self.data = self.date = None
        del(self.data, self.date)

    def ma(self, raw, period, favour='s', field_initial='close', date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        _data = grabber(raw, field_initial)
        if favour.upper() in ['SIMPLE', 'S']:
            __ = _data.rolling(period).mean()
        if favour.upper() in ['W', 'WEIGHTED']:
            __ = (_data * raw.Volume).rolling(period).sum() \
                / raw.Volume.rolling(period).sum()
        if favour.upper() in ['E', 'EXPONENTIAL']:
            __ = stepper(_data, period)
        __.name = f'{favour}ma{period:02d}'.upper()
        return __

    def macd(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        e_slow = self.ma(raw, period['slow'], 'e', 'hl')
        e_fast = self.ma(raw, period['fast'], 'e', 'hl')
        m_line = e_fast - e_slow
        s_line = stepper(m_line, period['signal'])
        m_hist = m_line - s_line
        _ = pd.concat([m_line, s_line, m_hist], axis=1)
        _.columns = ['M Line', 'Signal', 'Histogram']
        return _

    def soc(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        ml = raw.Low.rolling(period['K']).min()
        mh = raw.High.rolling(period['K']).max()
        kseries = pd.Series(
            (raw.Close - ml) / (mh - ml) * 100, index=raw.index)
        k = kseries.rolling(period['D']).mean()
        k.name = '%K'
        d = k.rolling(period['D']).mean()
        d.name = '%D'
        _ = pd.concat([k, d], axis=1)
        _.name = 'SOC'
        return _

    def stc(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        e_slow = self.ma(raw, period['slow'], 'e', 'hl')
        e_fast = self.ma(raw, period['fast'], 'e', 'hl')
        m_line = e_fast.sub(e_slow)
        mh = m_line.rolling(period['K']).max()
        ml = m_line.rolling(period['K']).min()
        kseries = (m_line - ml) / (mh - ml)
        k = kseries.rolling(period['D']).mean()
        k.name = '%K'
        d = k.rolling(period['D']).mean()
        d.name = '%D'
        _ = (m_line - k) / (d - k)
        _.name = 'STC'
        return _

    def atr(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        tr = pd.DataFrame(
            [raw.High - raw.Low,
                (raw.High - raw.Close.shift(1)).abs(),
                (raw.Low - raw.Close.shift(1)).abs()]).max()
        _ = stepper(tr, period)
        _.name = f'ATR{period:02d}'
        return _

    def rsi(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, datetime):
                raw = raw.loc[:date]

        def _gz(_):
            if _ > 0:
                return _
            return 0

        def _lz(_):
            if _ < 0:
                return abs(_)
            return 0
        delta = raw.Close.diff(1)
        gain = delta.apply(_gz)
        loss = delta.apply(_lz)
        ag = stepper(gain, period)
        al = stepper(loss, period)
        rs = ag / al
        _ = 100 - 100 / (1 + rs)
        _.name = f'RSI{period:02d}'
        return _

    def adx(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        atr = self.atr(raw, period, date)
        hcp, lpc = raw.High.diff(1), -(raw.Low.diff(1))

        def _hgl(_):
            if _[0] > _[-1] and _[0] > 0:
                return _[0]
            return 0
        dm_plus = pd.concat([hcp, lpc], axis=1).apply(_hgl, axis=1)
        dm_minus = pd.concat([lpc, hcp], axis=1).apply(_hgl, axis=1)
        di_plus = stepper(dm_plus, period) / atr * 100
        di_plus.name = f'+DI{period:02d}'

        di_minus = stepper(dm_minus, period) / atr * 100
        di_minus.name = f'-DI{period:02d}'

        dx = (di_plus - di_minus).abs() / (di_plus + di_minus) * 100
        _ = stepper(dx, period)
        _.name = f'ADX{period:02d}'
        __ = pd.concat([di_plus, di_minus, _], axis=1)
        return __

    def kama(self, raw, period, field_initial='c', date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        _data = grabber(raw, field_initial)
        change = (_data - _data.shift(period['er'])).abs()
        volatility = _data.diff(1).abs().rolling(period['er']).sum()
        er = change / volatility
        sc = (
            er * (2 / (period['fast'] + 1) - 2 / (period['slow'] + 1)) + 2
            / (period['slow'] + 1)) ** 2
        _ = stepper(_data, period['slow'], sc)
        _.name = f"KAMA{period['er']:02d}"
        return _

    def apz(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        ehl = self.ma(raw, period, 'e', 'hl')
        volatility = stepper(ehl, period)
        tr = pd.DataFrame(
            [raw.High - raw.Low,
                (raw.High - raw.Close.shift(1)).abs(),
                (raw.Low - raw.Close.shift(1)).abs()]).max()

        upper = volatility + tr * gr
        lower = volatility - tr * gr
        # _ = pd.concat([upper.apply(hsirnd), lower.apply(hsirnd, axis=1)], axis=1)
        _ = pd.concat([upper.apply(hsirnd), lower.apply(hsirnd)], axis=1)
        _.columns = ['Upper', 'Lower']
        return _

    def dc(self, raw, period, date=None):
        # Donchian Channel
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        pax = raw.High.rolling(period).max()
        pin = raw.Low.rolling(period).min()
        # ehl = self.ma(raw, period, 'e', 'hl')
        # volatility = stepper(ehl, period)
        # tr = pd.DataFrame(
        #     [raw.High - raw.Low,
        #         (raw.High - raw.Close.shift(1)).abs(),
        #         (raw.Low - raw.Close.shift(1)).abs()]).max()
        #
        # upper = volatility + tr * gr
        # lower = volatility - tr * gr
        # _ = pd.concat(
        #     [upper.apply(hsirnd, 1), lower.apply(hsirnd, 1)], axis=1)
        _ = pd.concat([pax.apply(hsirnd), pin.apply(hsirnd)], axis=1)
        _.columns = ['Upper', 'Lower']
        return _

    def kc(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        middle_line = self.kama(raw, period['kama'], 'hl')
        atr = self.atr(raw, period['atr'])
        upper = middle_line + (gr * atr)
        lower = middle_line - (gr * atr)
        _ = pd.concat([upper.apply(hsirnd), lower.apply(hsirnd)], axis=1)
        _.columns = ['Upper', 'Lower']
        return _

    def bb(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        middle_line = self.ma(raw, period, 's', 'c')
        width = raw.Close.rolling(period).std()
        upper = middle_line + width
        lower = middle_line - width
        _ = pd.concat([upper.apply(hsirnd), lower.apply(hsirnd)], axis=1)
        _.columns = ['Upper', 'Lower']
        return _

    def obv(self, raw, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        hdr, _ = [raw.Volume.iloc[0]], 1
        dcp = raw.Close.diff(1)
        while _ < dcp.size:
            val = 0
            if dcp[_] > 0:
                val = raw.Volume.iloc[_]
            if dcp[_] < 0:
                val = -raw.Volume.iloc[_]
            hdr.append(hdr[-1] + val)
            _ += 1
        _ = pd.Series(hdr, index=dcp.index)
        _.name = 'OBV'
        return _

    def vwap(self, raw, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        pv = raw.drop(['Open', 'Volume'], 1).mean(axis=1) * raw.Volume
        _ = pd.Series(
                pv.cumsum() / raw.Volume.cumsum(),
                index=raw.index).apply(hsirnd, axis=1)
        _.name = 'VWAP'
        return _

    def gat(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass

        def _patr(period, raw, date):
            lc, lr = raw.Close.loc[date], self.atr(raw, period, date).loc[date]
            _ = [lc + lr, lc, lc - lr]
            _.extend(gslice([lc + lr, lc]))
            _.extend(gslice([lc, lc - lr]))
            return _

        def _pgap(pivot, raw):
            gap = pivot - raw.Close.iloc[-1]
            _ = gslice([pivot + gap, pivot])
            _.extend(gslice([pivot, pivot - gap]))
            return _
        hdr = []
        [hdr.extend(_pgap(_, raw)) for _ in _patr(period, raw, date)]
        hdr.sort()
        # _ = pd.Series(hdr).apply(hsirnd, 1).unique()
        _ = pd.Series(hdr)
        return _

    def ovr(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        if date not in raw.index:
            date = raw.index[-1]
        ols = ['APZ', 'BB', 'KC']
        ups = pd.DataFrame(
            [self.apz(raw, period['apz'])['Upper'],
                self.bb(raw, period['simple'])['Upper'],
                self.kc(raw, period['kc'])['Upper']], index=ols)[date]
        los = pd.DataFrame(
            [self.apz(raw, period['apz'])['Lower'],
                self.bb(raw, period['simple'])['Lower'],
                self.kc(raw, period['kc'])['Lower']], index=ols)[date]
        hdr, val = {'Max': [], 'Min': []}, np.nan
        for _ in ols:
            val = np.nan
            if ups[_] == ups.max():
                val = hsirnd(ups[_])
            hdr['Max'].append(val)
            val = np.nan
            if los[_] == los.min():
                val = hsirnd(los[_])
            hdr['Min'].append(val)
        _ = pd.DataFrame(hdr, index=ols).T
        return _
        return hdr



class Viewer(ONA):
    def __init__(self, data):
        self.data = data

    def __del__(self):
        self.data = None
        del(self.data)

    def mas(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        _ = pd.concat(
                [self.kama(raw, period['kama'], 'c', date).map(hsirnd),
                    self.ma(raw, period['simple'], 'e', 'c', date).map(hsirnd),
                    self.ma(raw, period['simple'], 's', 'c', date).map(hsirnd),
                    self.ma(
                        raw,
                        period['simple'],
                        'w',
                        'c',
                        date).map(hsirnd)],
                axis=1)
        return _

    def idrs(self, raw, period, date=None):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d')
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        _ = pd.concat(
            [self.adx(raw, period['adx'], date)[f"ADX{period['adx']:02d}"],
                self.rsi(raw, period['simple'], date),
                self.atr(raw, period['atr'], date)], axis=1)
        return _

    def maverick(self, raw, period, date, unbound=False, exclusive=True):
        if date is not None:
            if isinstance(date, str):
                try:
                    date = datetime.strptime(date, '%Y%m%d').date()
                except Exception:
                    pass
            if isinstance(date, datetime):
                try:
                    raw = raw.loc[:date]
                except Exception:
                    pass
        # bare = self.gat(raw, period['atr'], date).apply(hsirnd, axis=1).unique()
        bare = self.gat(raw, period['atr'], date).apply(hsirnd).unique()
        boundary = self.ovr(raw, period, date).T
        close = raw.Close.loc[date]
        inside = [
            _ for _ in bare.tolist() if _ > boundary['Min'].min() and
            _ < boundary['Max'].max()]
        outside = [_ for _ in bare.tolist() if _ not in inside]
        hdr = {'buy': np.nan, 'sell': np.nan}
        # hdr['buy'] = close
        if inside != []:
            if close > min(inside):
                hdr['buy'] = min(inside)
            if close < max(inside):
                hdr['sell'] = max(inside)
        if unbound:
            if exclusive:
                hdr = {'buy': np.nan, 'sell': np.nan}
            if outside != []:
                if close > min(outside):
                    hdr['buy'] = min(outside)
                if close < max(outside):
                    hdr['sell'] = max(outside)
        _ = pd.DataFrame({date: hdr})
        return _


def stepper(x, period, addition=None):
    data, an, hdr, _, __ = x.values, addition, [], 0, 0
    if isinstance(addition, pd.Series):
        an = addition.values
    while _ < data.size:
        val = np.nan
        if not np.isnan(data[_]):
            if __ > period:
                val = (hdr[-1] * (period - 1) + data[_]) / period
                if isinstance(addition, pd.Series):
                    val = hdr[-1] + an[_] * (data[_] - hdr[-1])
            elif __ == period:
                val = np.array(data[_ - period: _]).mean()
            __ += 1
        hdr.append(val)
        _ += 1
    return pd.Series(hdr, index=x.index)


def grabber(x, initial='c'):
    fdict = {'c': 'Close', 'h': 'High', 'l': 'Low', 'o': 'Open'}
    if initial.lower() in fdict.keys():
        return eval(f'x.{fdict[initial.lower()]}')
    if initial.capitalize() in fdict.values():
        return eval(f'x.{initial.capitalize()}')
    if initial.lower() in ['hl', 'lh', 'range']:
        return x[['High', 'Low']].mean(axis=1)
    if initial.lower() in ['ohlc', 'full', 'all']:
        return x.drop('Volume', 1).mean(axis=1)
#
#
# def hsirnd(value):
#     if np.isnan(value) or not value > 0:
#         return np.nan
#     _ = int(np.floor(np.log10(value)))
#     __ = np.divmod(value, 10 ** (_ - 1))[0]
#     if _ < 0:
#         if __ < 25:
#             return np.round(value, 3)
#         if __ < 50:
#             return np.round(value * 2, 2) / 2
#         return np.round(value, 2)
#     if _ == 0:
#         return np.round(value, 2)
#     if _ > 3:
#         return np.round(value, 0)
#     if _ > 1:
#         if __ < 20:
#             return np.round(value, 1)
#         if __ < 50:
#             return np.round(value * 5, 0) / 5
#         return np.round(value * 2, 0) / 2
#     if _ > 0:
#         if __ < 10:
#             return np.round(value, 2)
#         if __ < 20:
#             return np.round(value * 5, 1) / 5
#         return np.round(value * 2, 1) / 2
