import pref
pd, np, datetime, gr = pref.nta
from utilities import gslice

class ONA(object):
    def __init__(self, data, date=datetime.today().date()):
        self.data = data
        self.date = date
        if date not in self.data.index: self.date = self.data.index[-1]

    def __del__(self):
        self.data = self.date = None
        del(self.data, self.date)

    def ma(self, raw, period, favour='s', req_field='close', dataframe=True):
        if req_field.upper() in ['C', 'CLOSE']: _data = raw['Close']
        if req_field.upper() in ['HL', 'LH', 'RANGE']: _data = raw[['High', 'Low']].mean(axis=1)
        if req_field.upper() in ['OHLC', 'FULL', 'ALL']: _data = raw.drop('Volume', 1).mean(axis=1)
        if favour.upper() in ['SIMPLE', 'S']: __ = _data.rolling(period).mean()
        if favour.upper() in ['W', 'WEIGHTED']:
            _ = _data * raw['Volume']
            __ = _.rolling(period).sum() / raw['Volume'].rolling(period).sum()
        if favour.upper() in ['E', 'EXPONENTIAL']:
            _, hdr, val = 0, [], np.nan
            while _ < len(_data):
                if _ == period: val = _data[:period].mean()
                if _ > period: val = (hdr[-1] * (period - 1) + _data[_]) / period
                hdr.append(val)
                _ += 1
            __ = pd.Series(hdr, index=_data.index)
        __.name = f'{favour}ma{period:02d}'.upper()
        if dataframe: return __
        return __.to_dict()

    def macd(self, raw, period, dataframe=True):
        def __pema(pd_data, period):
            data, hdr, __ = pd_data.values, [], 0
            for _ in range(len(data)):
                val = np.nan
                if not np.isnan(data[_]):
                    if __ == period:
                        val = np.array(data[_ - period: _]).mean()
                    if __ > period:
                        val = (hdr[-1] * (period - 1) + data[_]) / period
                    __ += 1
                hdr.append(val)
            return pd.Series(hdr, index=pd_data.index)

        e_slow = self.ma(raw, period['slow'], 'e', 'hl', True)
        e_fast = self.ma(raw, period['fast'], 'e', 'hl', True)
        m_line = e_fast - e_slow
        s_line = __pema(m_line, period['signal'])
        m_hist = m_line - s_line
        _ = pd.DataFrame([m_line, s_line, m_hist]).T
        _.columns = ['M Line', 'Signal Line', 'M Histogram']
        if dataframe: return _
        return _.to_dict()

    def soc(self, raw, period, dataframe=True):
        ml = raw['Low'].rolling(period['K']).min()
        mh = raw['High'].rolling(period['K']).max()
        kseries = pd.Series((raw['Close'] - ml) / (mh - ml) * 100, index=raw.index)
        k = kseries.rolling(period['D']).mean()
        k.name = '%K'
        d = k.rolling(period['D']).mean()
        d.name = '%D'
        _ = pd.DataFrame([k, d]).T
        if dataframe: return _
        return _.to_dict()

    def stc(self, raw, period, dataframe=True):
        e_slow = self.ma(raw, period['slow'], 'e', 'hl', True)
        e_fast = self.ma(raw, period['fast'], 'e', 'hl', True)
        m_line = e_fast - e_slow
        mh = m_line.rolling(period['K']).max()
        ml = m_line.rolling(period['K']).min()
        kseries = (m_line - ml) / (mh - ml)
        k = kseries.rolling(period['D']).mean()
        k.name = '%K'
        d = k.rolling(period['D']).mean()
        d.name = '%D'
        _ = (m_line - k) / (d - k)
        _.name = 'STC'
        if dataframe: return _
        return _.to_dict()

    def atr(self, raw, period, dataframe=True):
        tr = pd.DataFrame([raw['High'] - raw['Low'], (raw['High'] - raw['Close'].shift(1)).abs(), (raw['Low'] - raw['Close'].shift(1)).abs()]).max()
        _, hdr, __ = 0, [], np.nan
        while _ < len(raw):
            if _ == period: __ = tr[:_].mean()
            if _ > period: __ = (hdr[-1] * (period - 1) + tr[_]) / period
            hdr.append(__)
            _ += 1
        _ = pd.Series(hdr, index=raw.index)
        _.name = f'ATR{period:02d}'
        if dataframe: return _
        return _.to_dict()

    def rsi(self, raw, period, dataframe=True):
        def _gz(_):
            if _ > 0: return _
            return 0
        def _lz(_):
            if _ < 0: return abs(_)
            return 0
        delta = raw['Close'].diff(1)
        gain = delta.apply(_gz)
        loss = delta.apply(_lz)
        _, hdr, __ = 0, [], np.nan
        while _ < len(gain):
            if _ == period: __ = gain[:_].mean()
            if _ > period: __ = (hdr[-1] * (period - 1) + gain[_]) / period
            hdr.append(__)
            _ += 1
        ag = pd.Series(hdr, index=gain.index)
        _, hdr, __ = 0, [], np.nan
        while _ < len(loss):
            if _ == period: __ = loss[:_].mean()
            if _ > period: __ = (hdr[-1] * (period - 1) + loss[_]) / period
            hdr.append(__)
            _ += 1
        al = pd.Series(hdr, index=loss.index)
        rs = ag / al
        _ = 100 - 100 / (1 + rs)
        _.name = f'RSI{period:02d}'
        if dataframe: return _
        return _.to_dict()

    def adx(self, raw, period, dataframe=True):
        atr = self.atr(raw, period, True)
        hcp, lpc = raw['High'].diff(1), -(raw['Low'].diff(1))
        def _hgl(_):
            if _[0] > _[-1] and _[0] > 0: return _[0]
            return 0
        dm_plus = pd.DataFrame([hcp, lpc]).T.apply(_hgl, axis=1)
        dm_minus = pd.DataFrame([lpc, hcp]).T.apply(_hgl, axis=1)
        _, iph, __ = 0, [], np.nan
        while _ < len(dm_plus):
            if _ == period: __ = dm_plus[:_].mean()
            if _ > period: __ = (iph[-1] * (period - 1) + dm_plus[_]) / period
            iph.append(__)
            _ += 1
        di_plus = pd.Series(iph, index=dm_plus.index) / atr * 100
        di_plus.name = f'+DI{period:02d}'

        _, imh, __ = 0, [], np.nan
        while _ < len(dm_minus):
            if _ == period: __ = dm_minus[:_].mean()
            if _ > period: __ = (imh[-1] * (period - 1) + dm_minus[_]) / period
            imh.append(__)
            _ += 1
        di_minus = pd.Series(imh, index=dm_minus.index) / atr * 100
        di_minus.name = f'-DI{period:02d}'

        dx = (di_plus - di_minus).abs() / (di_plus + di_minus) * 100
        _, hdr, __, val = 0, [], 0, np.nan
        while _ < len(dx):
            if not np.isnan(dx[_]):
                if __ == period: val = dx[_ - __:_].mean()
                if __ > period: val = (hdr[-1] * (period - 1) + dx[__]) / period
                __ += 1
            hdr.append(val)
            _ += 1
        _ = pd.Series(hdr, index=dx.index)
        _.name = f'ADX{period:02d}'
        __ = pd.DataFrame([di_plus, di_minus, _]).T
        if dataframe: return __
        return __.to_dict()

    def kama(self, raw, period, req_field='c', dataframe=True):
        if req_field.upper() in ['C', 'CLOSE']: _data = raw['Close']
        if req_field.upper() in ['HL', 'LH', 'RANGE']: _data = raw[['High', 'Low']].mean(axis=1)
        if req_field.upper() in ['OHLC', 'FULL', 'ALL']: _data = raw.drop('Volume', 1).mean(axis=1)
        change = (_data - _data.shift(period['er'])).abs()
        volatility = _data.diff(1).abs().rolling(period['er']).sum()
        er = change / volatility
        sc = (er * (2 / (period['fast'] + 1) - 2 / (period['slow'] + 1)) + 2 / (period['slow'] + 1)) ** 2
        _, hdr, __ = 0, [], np.nan
        while _ < len(raw):
            if _ == period['slow']: __ = _data[:_].mean()
            if _ > period['slow']: __ = hdr[-1] + sc[_] * (_data[_] - hdr[-1])
            hdr.append(__)
            _ += 1
        _ = pd.Series(hdr, index=raw.index)
        _.name = f"KAMA{period['er']:02d}"
        if dataframe: return _
        return _.to_dict()

    def apz(self, raw, period, dataframe=True):
        ehl = self.ma(raw, period, 'e', 'hl', True)
        _, hdr, __, val = 0, [], 0, np.nan
        while _ < len(ehl):
            if not np.isnan(ehl[_]):
                if __ == period: val = ehl[_ - __:_].mean()
                if __ > period: val = (hdr[-1] * (period - 1) + ehl[_]) / period
                __ += 1
            hdr.append(val)
            _ += 1
        volatility = pd.Series(hdr, index=ehl.index)
        tr = pd.DataFrame([raw['High'] - raw['Low'], (raw['High'] - raw['Close'].shift(1)).abs(), (raw['Low'] - raw['Close'].shift(1)).abs()]).max()

        upper = volatility + tr * gr
        lower = volatility - tr * gr
        _ = pd.DataFrame([upper, lower]).T
        _.columns = ['Upper', 'Lower']
        if dataframe: return _
        return _.to_dict()

    def kc(self, raw, period, dataframe=True):
        middle_line = self.kama(raw, period['kama'], 'hl', True)
        atr = self.atr(raw, period['atr'], True)
        upper = middle_line + (gr * atr)
        lower = middle_line - (gr * atr)
        _ = pd.DataFrame([upper, lower]).T
        _.columns = ['Upper', 'Lower']
        if dataframe: return _
        return _.to_dict()

    def bb(self, raw, period, dataframe=True):
        middle_line = self.ma(raw, period, 's', 'c', True)
        width = raw['Close'].rolling(period).std()
        upper = middle_line + width
        lower = middle_line - width
        _ = pd.DataFrame([upper, lower]).T
        _.columns = ['Upper', 'Lower']
        if dataframe: return _
        return _.to_dict()

    def obv(self, raw, dataframe=True):
        hdr, _ = [raw['Volume'][0]], 1
        dcp = raw['Close'].diff(1)
        while _ < dcp.size:
            val = 0
            if dcp[_] > 0: val = raw['Volume'][_]
            if dcp[_] < 0: val = -raw['Volume'][_]
            hdr.append(hdr[-1] + val)
            _ += 1
        _ = pd.Series(hdr, index=dcp.index)
        _.name = 'OBV'
        if dataframe: return _
        return _.to_dict()

    def vwap(self, raw, dataframe=True):
        pv = raw.drop(['Open', 'Volume'], 1).mean(axis=1) * raw['Volume']
        _ = pd.Series(pv.cumsum() / raw['Volume'].cumsum(), index=raw.index)
        _.name = 'VWAP'
        if dataframe: return _
        return _.to_dict()

    def ratr(self, raw, period, date=None, dataframe=True):
        if date == None or date not in raw.index: date = raw.index[-1]
        def _patr(period, raw):
            lc, lr = raw['Close'][date], self.atr(raw, period, True)[date]
            _ = [lc + lr, lc, lc - lr]
            _.extend(gslice([lc + lr, lc]))
            _.extend(gslice([lc, lc - lr]))
            return _

        def _pgap(pivot, raw):
            gap = pivot - raw['Close'][date]
            _ = gslice([pivot + gap, pivot])
            _.extend(gslice([pivot, pivot - gap]))
            return _
        hdr = []
        [hdr.extend(_pgap(_, raw)) for _ in _patr(period, raw)]
        hdr.sort()
        _ = pd.Series([hsirnd(__) for __ in hdr]).unique()
        if dataframe: return _
        return hdr

    def ovr(self, raw, period, date=None, dataframe=True):
        if date not in raw.index: date = raw.index[-1]
        ols = ['APZ', 'BB', 'KC']
        ups = pd.DataFrame([self.apz(raw, period['apz'], True)['Upper'], self.bb(raw, period['simple'], True)['Upper'], self.kc(raw, period['kc'], True)['Upper']], index=ols)[date]
        los = pd.DataFrame([self.apz(raw, period['apz'], True)['Lower'], self.bb(raw, period['simple'], True)['Lower'], self.kc(raw, period['kc'], True)['Lower']], index=ols)[date]
        hdr, val = {'Max':[], 'Min':[]}, np.nan
        for _ in ols:
            val = np.nan
            if ups[_] == ups.max(): val = hsirnd(ups[_])
            hdr['Max'].append(val)
            val = np.nan
            if los[_] == los.min(): val = hsirnd(los[_])
            hdr['Min'].append(val)
        _ = pd.DataFrame(hdr, index=ols).T
        if dataframe: return _
        return hdr

class Viewer(ONA):
    def __init__(self, data):
        self.data = data

    def __del__(self):
        self.data = None
        del(self.data)

    def mas(self, raw, period, dataframe=True):
        _ = pd.DataFrame([self.kama(raw, period['kama'], 'c', True).map(hsirnd), self.ma(raw, period['simple'], 'e', 'c', True).map(hsirnd), self.ma(raw, period['simple'], 's', 'c', True).map(hsirnd), self.ma(raw, period['simple'], 'w', 'c', True).map(hsirnd)]).T
        if dataframe: return _
        return _.to_dict()

    def idrs(self, raw, period, dataframe=True):
        _ = pd.DataFrame([self.adx(raw, period['adx'], True)[f"ADX{period['adx']:02d}"], self.rsi(raw, period['simple'], True), self.atr(raw, period['atr'], True)]).T
        if dataframe: return _
        return _.to_dict()

    def maverick(self, raw, period, date, unbound=False, exclusive=True, dataframe=True):
        bare = self.ratr(raw, period['atr'], date, True)
        boundary = self.ovr(raw, period, date, True).T
        close = raw['Close'][date]
        inside = [_ for _ in bare.tolist() if _ > boundary['Min'].min() and _ < boundary['Max'].max()]
        outside = [_ for _ in bare.tolist() if _ not in inside]
        hdr = {'buy':np.nan, 'sell':np.nan}
        if close > min(inside): hdr['buy'] = min(inside)
        if close < max(inside): hdr['sell'] = max(inside)
        if unbound:
            if exclusive: hdr = {'buy':np.nan, 'sell':np.nan}
            if outside != []:
                if close > min(outside): hdr['buy'] = min(outside)
                if close < max(outside): hdr['sell'] = max(outside)
        _ = pd.DataFrame({date:hdr})
        if dataframe: return _
        return _.to_dict()

def hsirnd(value):
    if np.isnan(value) or not value > 0: return np.nan
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
