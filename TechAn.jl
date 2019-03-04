using PyCall, Pandas
py"""
import pandas as pd
import sqlalchemy as sqa
import pathlib
from scipy.constants import golden_ratio
from numpy import nan, isnan, array
from datetime import datetime
start = datetime(datetime.today().year - 4, 12, 31).date()
dir_ = '~'
platform = pathlib.sys.platform
if platform in ['linux']:
    import fix_yahoo_finance as yf
    dir_ = '~/storage/shared'
    if 'EXTERNAL_STORAGE' in pathlib.os.environ.keys():
        dir_ = '~/storage/external-1'
dir_ += '/data/sqlite3/Securities'
path = pathlib.Path(dir_)
engine = sqa.create_engine(f'sqlite:///{path.expanduser()}')
"""
sma(x, period=20) = x."Close".rolling(period).mean()

function wma(x, period=20, req_field="c")
_data = x."Close"
if lowercase(req_field) in ["ohlc", "all", "full"]
    _data = x.drop("Volume", 1).mean(axis=1)
end
if lowercase(req_field) in ["hl", "lh", "range"]
    _data = x.drop(["Open", "Close", "Volume"], 1).mean(axis=1)
end
(_data * x."Volume").rolling(period).sum() / x."Volume".rolling(period).sum()
end

function ema(x, period=20, req_field="c")
_data = x."Close"
if lowercase(req_field) in ["ohlc", "all", "full"]
    _data = x.drop("Volume", 1).mean(axis=1)
end
if lowercase(req_field) in ["hl", "lh", "range"]
    _data = x.drop(["Open", "Close", "Volume"], 1).mean(axis=1)
end
py"""
data, period = $_data, $period
tl, _ = [], 0
while _ < data.size:
    hdr = nan
    if _ == period: hdr = data[:period].mean()
    if _ > period:
        hdr = (tl[-1] * (period - 1) + data[_]) / period
    tl.append(hdr)
    _ += 1
_ = pd.Series(tl, index=data.index)
_.name = f'EMA{period:02d}'
"""
py"_"
end

function kama(x, period=Dict("er" => 10, "fast" => 2, "slow" => 30), req_field="c")
_data = x."Close"
if uppercase(req_field) in ["HL", "LH", "RANGE"]
    _data = x.drop(["Open", "Close", "Volume"], 1).mean(axis=1)
end
if uppercase(req_field) in ["OHLC", "FULL", "ALL"]
    _data = x.drop("Volume", 1).mean(axis=1)
end
change = (_data - _data.shift(period["er"])).abs()
volatility = (_data - _data.shift(1)).abs().rolling(period["er"]).sum()
er = change / volatility
sc = (er * (2 / (period["fast"] + 1) - 2 / (period["slow"] + 1)) + 2 / (period["slow"] + 1)) ^ 2
py"""
data, period = $_data, $period
_, hdr, __ = 0, [], nan
while _ < data.size:
    if _ == period['slow']: __ = data[:_].mean()
    if _ > period['slow']: __ = hdr[-1] + $sc[_] * (data[_] - hdr[-1])
    hdr.append(__)
    _ += 1
_ = pd.Series(hdr, index=data.index)
_.name = f'KAMA{period["er"]:02d}'
"""
py"_"
end

function apz(x, period=5)
ehl = ema(x, period, "hl")
py"""
raw = $x
period = $period
_, hdr, __, val = 0, [], 0, nan
while _ < len($ehl):
    if not isnan($ehl[_]):
        if __ == period: val = $ehl[_ - __:_].mean()
        if __ > period: val = (hdr[-1] * (period - 1) + $ehl[_]) / period
        __ += 1
    hdr.append(val)
    _ += 1
volatility = pd.Series(hdr, index=$ehl.index)
tr = pd.DataFrame([raw['High'] - raw['Low'], (raw['High'] - raw['Close'].shift(1)).abs(), (raw['Low'] - raw['Close'].shift(1)).abs()]).max()
upper = volatility + tr * golden_ratio
lower = volatility - tr * golden_ratio
_ = pd.DataFrame([upper, lower]).T
_.columns = ['Upper', 'Lower']
"""
py"_"
end

function kc(x, period=Dict("kama" => Dict("er" => 5, "fast" => 2, "slow" => 20), "atr" => 10))
middle_line = kama(x, period["kama"], "hl")
atr_ = atr(x, period["atr"])
gr = py"golden_ratio"
upper = middle_line + (gr * atr_)
lower = middle_line - (gr * atr_)
py"""
_ = pd.DataFrame([$upper, $lower]).T
_.columns = ['Upper', 'Lower']
"""
py"_"
end

function bb(x, period=20)
middle_line = sma(x, period)
width = x."Close".rolling(period).std()
upper = middle_line + width
lower = middle_line - width
py"""
_ = pd.DataFrame([upper, lower]).T
_.columns = ['Upper', 'Lower']
"""
py"_"
end

function macd(x, period=Dict("fast" => 12, "slow" => 26, "signal" => 9))
py"""
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
"""
e_slow = ema(x, period["slow"], "hl")
e_fast = ema(x, period["fast"], "hl")
m_line = e_fast - e_slow
s_line = py"__pema($m_line, $period['signal'])"
m_hist = m_line - s_line
setproperty!(m_line, "name", "M Line")
setproperty!(s_line, "name", "Signal")
setproperty!(m_hist, "name", "Histogram")
py"pd.DataFrame([$m_line, $s_line, $m_hist]).T"
end

function soc(x, period=Dict("K" => 14, "D" => 3))
ml = x."Low".rolling(period["K"]).min()
mh = x."High".rolling(period["K"]).max()
kseries = py"pd.Series(($x['Close'] - $ml) / ($mh - $ml) * 100, index=$x.index)"
k = kseries.rolling(period["D"]).mean()
d = k.rolling(period["D"]).mean()
setproperty!(k, "name", "%K")
setproperty!(d, "name", "%D")
py"pd.DataFrame([$k, $d]).T"
end

function stc(x, period=Dict("fast" => 23, "slow" => 50, "K" => 10, "D" => 10))
slow_ = ema(x, period["slow"], "hl")
fast_ = ema(x, period["fast"], "hl")
m_line = fast_ - slow_
mh = m_line.rolling(period["K"]).max()
ml = m_line.rolling(period["K"]).min()
kseries = (m_line - ml) / (mh - ml)
k = kseries.rolling(period["D"]).mean()
d = k.rolling(period["D"]).mean()
_ = (m_line - k) / (d - k)
end

function atr(x, period=14)
py"""
raw, period = $x, $period
tr = pd.DataFrame([raw['High'] - raw['Low'], (raw['High'] - raw['Close'].shift(1)).abs(), (raw['Low'] - raw['Close'].shift(1)).abs()]).max()
_, hdr, __ = 0, [], nan
while _ < len(raw):
    if _ == period: __ = tr[:_].mean()
    if _ > period: __ = (hdr[-1] * (period - 1) + tr[_]) / period
    hdr.append(__)
    _ += 1
_ = pd.Series(hdr, index=raw.index)
_.name = f'ATR{period:02d}'
"""
py"_"
end

function rsi(x, period=14)
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

function adx(x, period=14)
atr_ = atr(x , period)
hcp = x."High" - x."High".shift(1)
lpc = x."Low".shift(1) - x."Low"
py"""
period = $period
def _hgl(_):
    if _[0] > _[-1] and _[0] > 0: return _[0]
    return 0
dm_plus = pd.DataFrame([$hcp, $lpc]).T.apply(_hgl, axis=1)
dm_minus = pd.DataFrame([$lpc, $hcp]).T.apply(_hgl, axis=1)
_, iph, __ = 0, [], nan
while _ < len(dm_plus):
    if _ == period: __ = dm_plus[:_].mean()
    if _ > period: __ = (iph[-1] * (period - 1) + dm_plus[_]) / period
    iph.append(__)
    _ += 1
"""
di_plus = py"pd.Series(iph, index=dm_plus.index) / $atr_ * 100"
py"""
_, imh, __ = 0, [], nan
while _ < len(dm_minus):
    if _ == period: __ = dm_minus[:_].mean()
    if _ > period: __ = (imh[-1] * (period - 1) + dm_minus[_]) / period
    imh.append(__)
    _ += 1
"""
di_minus = py"pd.Series(imh, index=dm_minus.index) / $atr_ * 100"
py"""
dx = ($di_plus - $di_minus).abs() / ($di_plus + $di_minus) * 100
_, hdr, __, val = 0, [], 0, nan
while _ < len(dx):
    if not isnan(dx[_]):
        if __ == period: val = dx[_ - __:_].mean()
        if __ > period: val = (hdr[-1] * (period - 1) + dx[__]) / period
        __ += 1
    hdr.append(val)
    _ += 1
"""
g = py"pd.Series(hdr, index=dx.index)"
setproperty!(di_plus, "name", "+DI" * string(period))
setproperty!(di_minus, "name", "-DI" * string(period))
setproperty!(g, "name", "ADX" * string(period))
h = py"pd.DataFrame([$di_plus, $di_minus, $g]).T"
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
"""
h = py"pd.Series(hdr, index=dcp.index)"
setproperty!(h, "name", "OBV")
end

function vwap(x)
pv = x.drop(["Open", "Volume"], 1).mean(axis=1) * x."Volume"
h = py"pd.Series($pv.cumsum() / $x['Volume'].cumsum(), index=$x.index)"
setproperty!(h, "name", "VWAP")
end

function fetch(c, adhoc=false)
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

function ratr(x, adhoc=true,  ratio=py"golden_ratio")
function delta(b, d, r)
[b - d, b - d / r, b - (1 - 1 / r) * d, b, b + (1 - 1 / r) * d, b + d / r, b + d]
end
data = py"platform" == "linux" ? fetch(x, adhoc) : fetch(x, false)
delta(py"$data['Close'][-1]", py"$(atr(data))[-1]", ratio)
end

function exist(c)
py"""
bool = False
q_str = "SELECT DISTINCT eid FROM records ORDER BY eid ASC"
uid = pd.read_sql(q_str, engine)
if $c in uid.values: bool = True
"""
py"bool"
end
