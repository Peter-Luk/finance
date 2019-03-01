using PyCall
py"""
import pandas as pd
import sqlalchemy as sqa
import pathlib
from scipy.constants import golden_ratio
from numpy import nan, isnan, array
from datetime import datetime
start = datetime(datetime.today().year - 4, 12, 31).date()
dir_ = ''
platform = pathlib.sys.platform
if platform in ['linux']:
    import fix_yahoo_finance as yf
    dir_ = 'storage/shared'
    if 'EXTERNAL_STORAGE' in pathlib.os.environ.keys():
        dir_ = 'storage/external-1'
d_loc = f'sqlite:///{pathlib.Path.home()}'
if dir_: d_loc += f'/{dir_}'
d_loc += '/data/sqlite3/Securities'
engine = sqa.create_engine(d_loc)
def ema(raw, period, req_field='c'):
    if req_field.lower() in ['c', 'close']: _data = raw['Close']
    if req_field.lower() in ['hl', 'lh', 'range']: _data = raw.drop(['Open', 'Close', 'Volume'], 1).mean(axis=1)
    if req_field.lower() in ['ohlc', 'all', 'full']: _data = raw.drop('Volume', 1).mean(axis=1)
    tl, _ = [], 0
    while _ < _data.size:
        hdr = nan
        if _ == period: hdr = _data[:period].mean()
        if _ > period: hdr = (tl[-1] * (period - 1) + _data[_]) / period
        tl.append(hdr)
        _ += 1
    _ = pd.Series(tl, index=raw.index)
    _.name = f'EMA{period:02d}'
    return _
def kama(raw, period, req_field='c'):
    if req_field.upper() in ['C', 'CLOSE']: _data = raw['Close']
    if req_field.upper() in ['HL', 'LH', 'RANGE']: _data = raw.drop(['Open', 'Close', 'Volume'], 1).mean(axis=1)
    if req_field.upper() in ['OHLC', 'FULL', 'ALL']: _data = raw.drop('Volume', 1).mean(axis=1)
    change = (_data - _data.shift(period['er'])).abs()
    volatility = (_data - _data.shift(1)).abs().rolling(period['er']).sum()
    er = change / volatility
    sc = (er * (2 / (period['fast'] + 1) - 2 / (period['slow'] + 1)) + 2 / (period['slow'] + 1)) ** 2
    _, hdr, __ = 0, [], nan
    while _ < len(raw):
        if _ == period['slow']: __ = _data[:_].mean()
        if _ > period['slow']: __ = hdr[-1] + sc[_] * (_data[_] - hdr[-1])
        hdr.append(__)
        _ += 1
    _ = pd.Series(hdr, index=raw.index)
    _.name = f'KAMA{period["er"]:02d}'
    return _
"""
sma(x, period=20) = py"$x['Close'].rolling($period).mean()"

function wma(x, period::Int32=20, req_field="c")
py"""
raw = $x
period = $period
rf = $req_field
if rf.lower() in ['c', 'close']: _data = raw['Close']
if rf.lower() in ['hl', 'lh', 'range']: _data = raw.drop(['Open', 'Close', 'Volume'], 1).mean(axis=1)
if rf.lower() in ['ohlc', 'all', 'full']: _data = raw.drop('Volume', 1).mean(axis=1)
_ = (_data * raw['Volume']).rolling(period).sum() / raw['Volume'].rolling(period).sum()
_.name = f'WMA{period:02d}'
"""
py"_"
end

function ema(x, period::Int32=20, req_field="c")
py"""
_ = ema($x, $period, $req_field)
"""
py"_"
end

function kama(x, period=Dict("er" => 10, "fast" => 2, "slow" => 30), req_field="c")
py"""
_ = kama($x, $period, $req_field)
"""
py"_"
end

function apz(x, period::Int32=5)
py"""
raw = $x
period = $period
ehl = ema(raw, period, 'hl')
_, hdr, __, val = 0, [], 0, nan
while _ < len(ehl):
    if not isnan(ehl[_]):
        if __ == period: val = ehl[_ - __:_].mean()
        if __ > period: val = (hdr[-1] * (period - 1) + ehl[_]) / period
        __ += 1
    hdr.append(val)
    _ += 1
volatility = pd.Series(hdr, index=ehl.index)
tr = pd.DataFrame([raw['High'] - raw['Low'], (raw['High'] - raw['Close'].shift(1)).abs(), (raw['Low'] - raw['Close'].shift(1)).abs()]).max()

upper = volatility + tr * golden_ratio
lower = volatility - tr * golden_ratio
_ = pd.DataFrame([upper, lower]).T
_.columns = ['Upper', 'Lower']
"""
py"_"
end

function kc(x, period=Dict("kama" => Dict("er" => 5, "fast" => 2, "slow" => 20), "atr" => 10))
py"""
period = $period
middle_line = kama($x, period['kama'], 'hl')
atr_ = $atr($x, period['atr'])
upper = middle_line + (golden_ratio * atr_)
lower = middle_line - (golden_ratio * atr_)
_ = pd.DataFrame([upper, lower]).T
_.columns = ['Upper', 'Lower']
"""
py"_"
end

function bb(x, period::Int32=20)
py"""
raw = $x
period = $period
middle_line = $sma(raw, period)
width = raw['Close'].rolling(period).std()
upper = middle_line + width
lower = middle_line - width
_ = pd.DataFrame([upper, lower]).T
_.columns = ['Upper', 'Lower']
"""
py"_"
end

function macd(x, period=Dict("fast" => 12, "slow" => 26, "signal" => 9))
py"""
raw = $x
period = $period
def __pema(pd_data, period):
    data, hdr, __ = pd_data.values, [], 0
    for _ in range(len(data)):
        val = nan
        if not isnan(data[_]):
            if __ == period:
                val = array(data[_ - period: _]).mean()
            if __ > period:
                val = (hdr[-1] * (period - 1) + data[_]) / period
            __ += 1
        hdr.append(val)
    return pd.Series(hdr, index=pd_data.index)

e_slow = ema(raw, period['slow'], 'hl')
e_fast = ema(raw, period['fast'], 'hl')
m_line = e_fast - e_slow
s_line = __pema(m_line, period['signal'])
m_hist = m_line - s_line
_ = pd.DataFrame([m_line, s_line, m_hist]).T
_.columns = ['M Line', 'Signal Line', 'M Histogram']
"""
py"_"
end

function soc(x, period=Dict("K" => 14, "D" => 3))
py"""
raw = $x
period = $period
ml = raw['Low'].rolling(period['K']).min()
mh = raw['High'].rolling(period['K']).max()
kseries = pd.Series((raw['Close'] - ml) / (mh - ml) * 100, index=raw.index)
k = kseries.rolling(period['D']).mean()
k.name = '%K'
d = k.rolling(period['D']).mean()
d.name = '%D'
_ = pd.DataFrame([k, d]).T
"""
py"_"
end

function stc(x, period=Dict("fast" => 23, "slow" => 50, "K" => 10, "D" => 10))
py"""
raw = $x
period = $period
slow_ = ema(raw, period['slow'], 'hl')
fast_ = ema(raw, period['fast'], 'hl')
m_line = fast_ - slow_
mh = m_line.rolling(period['K']).max()
ml = m_line.rolling(period['K']).min()
kseries = (m_line - ml) / (mh - ml)
k = kseries.rolling(period['D']).mean()
k.name = '%K'
d = k.rolling(period['D']).mean()
d.name = '%D'
_ = (m_line - k) / (d - k)
_.name = 'STC'
"""
py"_"
end

function atr(x, period::Int32=14)
py"""
raw = $x
period = $period
tr = pd.DataFrame([raw['High'] - raw['Low'], (raw['High'] - raw['Close'].shift(1)).abs(), (raw['Low'] - raw['Close'].shift(1)).abs()]).max()
_, hdr, __ = 0, [], nan
while _ < len(raw):
    if _ == period: __ = tr[:_].mean()
    if _ > period: __ = (hdr[-1] * (period - 1) + tr[_]) / period
    hdr.append(__)
    _ += 1
_ = pd.Series(hdr, index=raw.index)
_.name = f'ATR{period:d}'
"""
py"_"
end

function rsi(x, period::Int32=14)
py"""
raw = $x
period = $period
def _gz(_):
    if _ > 0: return _
    return 0
def _lz(_):
    if _ < 0: return abs(_)
    return 0
delta = raw['Close'] - raw['Close'].shift(1)
gain = delta.apply(_gz)
loss = delta.apply(_lz)
_, hdr, __ = 0, [], nan
while _ < len(gain):
    if _ == period: __ = gain[:_].mean()
    if _ > period: __ = (hdr[-1] * (period - 1) + gain[_]) / period
    hdr.append(__)
    _ += 1
ag = pd.Series(hdr, index=gain.index)
_, hdr, __ = 0, [], nan
while _ < len(loss):
    if _ == period: __ = loss[:_].mean()
    if _ > period: __ = (hdr[-1] * (period - 1) + loss[_]) / period
    hdr.append(__)
    _ += 1
al = pd.Series(hdr, index=loss.index)
rs = ag / al
_ = 100 - 100 / (1 + rs)
_.name = f'RSI{period:d}'
"""
py"_"
end

function adx(x, period::Int32=14)
py"""
raw = $x
period = $period
atr_ = $atr($x , period)
hcp, lpc = raw['High'] - raw['High'].shift(1), raw['Low'].shift(1) - raw['Low']
def _hgl(_):
    if _[0] > _[-1] and _[0] > 0: return _[0]
    return 0
dm_plus = pd.DataFrame([hcp, lpc]).T.apply(_hgl, axis=1)
dm_minus = pd.DataFrame([lpc, hcp]).T.apply(_hgl, axis=1)
_, iph, __ = 0, [], nan
while _ < len(dm_plus):
    if _ == period: __ = dm_plus[:_].mean()
    if _ > period: __ = (iph[-1] * (period - 1) + dm_plus[_]) / period
    iph.append(__)
    _ += 1
di_plus = pd.Series(iph, index=dm_plus.index) / atr_ * 100
di_plus.name = f'+DI{period:d}'

_, imh, __ = 0, [], nan
while _ < len(dm_minus):
    if _ == period: __ = dm_minus[:_].mean()
    if _ > period: __ = (imh[-1] * (period - 1) + dm_minus[_]) / period
    imh.append(__)
    _ += 1
di_minus = pd.Series(imh, index=dm_minus.index) / atr_ * 100
di_minus.name = f'-DI{period:d}'

dx = (di_plus - di_minus).abs() / (di_plus + di_minus) * 100
_, hdr, __, val = 0, [], 0, nan
while _ < len(dx):
    if not isnan(dx[_]):
        if __ == period: val = dx[_ - __:_].mean()
        if __ > period: val = (hdr[-1] * (period - 1) + dx[__]) / period
        __ += 1
    hdr.append(val)
    _ += 1
__ = pd.Series(hdr, index=dx.index)
__.name = f'ADX{period:d}'
_ = pd.DataFrame([di_plus, di_minus, __]).T
"""
py"_"
end

function obv(x)
py"""
raw =$x
hdr, _ = [raw['Volume'][0]], 0
dcp = raw['Close'] - raw['Close'].shift(1)
while _ < len(dcp):
    if _ > 0:
        val = 0
        if dcp[_] > 0: val = raw['Volume'][_]
        if dcp[_] < 0: val = -raw['Volume'][_]
        hdr.append(hdr[-1] + val)
    _ += 1
_ = pd.Series(hdr, index=dcp.index)
_.name = 'OBV'
"""
py"_"
end

function vwap(x)
py"""
raw = $x
pv = raw.drop(['Open', 'Volume'], 1).mean(axis=1) * raw['Volume']
_ = pd.Series(pv.cumsum() / raw['Volume'].cumsum(), index=raw.index)
_.name = 'VWAP'
"""
py"_"
end

function fetch(c::Int32, adhoc=false)
py"""
code = $c
adhoc =$adhoc
if adhoc:
    d = pd.DataFrame()
    if platform in ['linux']:
        d = yf.download(f'{code:04d}.HK', start, group_by='ticker')
        d.drop('Adj Close', 1, inplace=True)
else:
    exist = False
    t_str = "SELECT DISTINCT eid FROM records ORDER BY eid ASC"
    uid = pd.read_sql(t_str, engine)
    if code in uid.values: exist = True
    if exist:
        q_str = f"SELECT date, open, high, low, close, volume FROM records WHERE eid={code:d} AND date>{start:'%Y-%m-%d'}"
        d = pd.read_sql(q_str, engine, index_col='date', parse_dates=['date'])
        d.columns = [_.capitalize() for _ in d.columns]
        d.index.name = d.index.name.capitalize()
    else:
        if platform in ['linux']:
            d = yf.download(f'{code:04d}.HK', start, group_by='ticker')
            d.drop('Adj Close', 1, inplace=True)
"""
if ~py"d.empty"
py"d"
end

end

function ratr(x::Int32, adhoc=true,  ratio::Float64=py"golden_ratio")
function delta(b::Float64, d::Float64, r::Float64)
[b - d, b - d / r, b - (1 - 1 / r) * d, b, b + (1 - 1 / r) * d, b + d / r, b + d]
end
data = py"platform" == "linux" ? fetch(x, adhoc) : fetch(x, false)
delta(py"$data['Close'][-1]", py"$(atr(data))[-1]", ratio)
end

function exist(c::Int32)
py"""
bool = False
q_str = "SELECT DISTINCT eid FROM records ORDER BY eid ASC"
uid = pd.read_sql(q_str, engine)
if $c in uid.values: bool = True
"""
py"bool"
end
