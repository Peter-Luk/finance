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
d = (_data * x."Volume").rolling(period).sum() / x."Volume".rolling(period).sum()
setproperty!(d, "name", "WMA" * string(period))
end

function ema(x, period=20, req_field="c")
_data = x."Close"
if lowercase(req_field) in ["ohlc", "all", "full"]
_data = x.drop("Volume", 1).mean(axis=1)
end
if lowercase(req_field) in ["hl", "lh", "range"]
_data = x.drop(["Open", "Close", "Volume"], 1).mean(axis=1)
end
tl = []
let i = 1
val = _data.values
while i <= length(val)
hdr = NaN
if i == period
hdr = mean(val[1:i])
end
if i > period
hdr = (tl[end] * (period - 1) + val[i]) / period
end
push!(tl, hdr)
i += 1
end
end
d = py"pd.Series($tl, index=$_data.index)"
setproperty!(d, "name", "EMA"* string(period))
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
hdr = []
let i = 1
val = _data.values
while i <= length(val)
j = NaN
if i == period["slow"]
j = mean(val[1:i])
end
if i > period["slow"]
j = hdr[end] + sc[i] * (val[i] - hdr[end])
end
push!(hdr, j)
i += 1
end
end
d = py"pd.Series($hdr, index=$_data.index)"
setproperty!(d, "name", "KAMA" * string(period["er"]))
end

function apz(x, period=5)
ehl = ema(x, period, "hl")
#=
hdr = []
let i = 1, j = 1
    val = ehl.values
    while i <= length(val)
        if isnan(val[i])
            push!(hdr, NaN)
        else
            tmp = NaN
            if j == period
                tmp = mean(val[i - j:i])
            end
            if j > period
                tmp = (hdr[end] * (period - 1) + val[i]) / period
            end
            push!(hdr, tmp)
            j += 1
        end
        i += 1
    end
end
volatility = py"pd.Series($hdr, index=$ehl.index)"
=#
py"""
_, hdr, __, val = 0, [], 0, nan
while _ < len($ehl):
    if not isnan($ehl[_]):
        if __ == $period: val = $ehl[_ - __:_].mean()
        if __ > $period: val = (hdr[-1] * ($period - 1) + $ehl[_]) / $period
        __ += 1
    hdr.append(val)
    _ += 1
"""
volatility = py"pd.Series(hdr, index=$ehl.index)"
tr = py"pd.DataFrame([$x['High'] - $x['Low'], ($x['High'] - $x['Close'].shift(1)).abs(), ($x['Low'] - $x['Close'].shift(1)).abs()]).max()"
gr = py"golden_ratio"
upper = volatility + tr * gr
lower = volatility - tr * gr
d = py"pd.DataFrame([$upper, $lower]).T"
setproperty!(d, "columns", ["Upper", "Lower"])
setproperty!(d, "name", "APZ" * string(period))
end

function kc(x, period=Dict("kama" => Dict("er" => 5, "fast" => 2, "slow" => 20), "atr" => 10))
middle_line = kama(x, period["kama"], "hl")
atr_ = atr(x, period["atr"])
gr = py"golden_ratio"
upper = middle_line + (gr * atr_)
lower = middle_line - (gr * atr_)
d = py"pd.DataFrame([$upper, $lower]).T"
setproperty!(d, "columns", ["Upper", "Lower"])
end

function bb(x, period=20)
middle_line = sma(x, period)
width = x."Close".rolling(period).std()
upper = middle_line + width
lower = middle_line - width
d = py"pd.DataFrame([$upper, $lower]).T"
setproperty!(d, "columns", ["Upper", "Lower"])
setproperty!(d, "name", "BB" * string(period))
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
h = py"pd.DataFrame([$m_line, $s_line, $m_hist]).T"
setproperty!(h, "name", "MACD")
end

function soc(x, period=Dict("K" => 14, "D" => 3))
ml = x."Low".rolling(period["K"]).min()
mh = x."High".rolling(period["K"]).max()
kseries = py"pd.Series(($x['Close'] - $ml) / ($mh - $ml) * 100, index=$x.index)"
k = kseries.rolling(period["D"]).mean()
d = k.rolling(period["D"]).mean()
setproperty!(k, "name", "%K")
setproperty!(d, "name", "%D")
hdr = py"pd.DataFrame([$k, $d]).T"
setproperty!(hdr, "name", "SOC")
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
setproperty!(k, "name", "%K")
setproperty!(d, "name", "%D")
hdr = (m_line - k) / (d - k)
setproperty!(hdr, "name", "STC")
end

function atr(x, period=14)
tr = py"pd.DataFrame([$x['High'] - $x['Low'], ($x['High'] - $x['Close'].shift(1)).abs(), ($x['Low'] - $x['Close'].shift(1)).abs()]).max()"
hdr = []
let i = 1
val = tr.values
while i <= length(val)
tmp = NaN
if i == period
tmp = mean(val[1:i])
end
if i > period
tmp = (hdr[end] * (period - 1) + val[i]) / period 
end
push!(hdr, tmp)
i += 1
end
end
d = py"pd.Series($hdr, index=$x.index)"
setproperty!(d, "name", "ATR"* string(period))
end

function rsi(x, period=14)
function _gz(x)
x > 0 ? x : 0
end
function _lz(x)
x < 0 ? abs(x) : 0
end
delta = x."Close" - x."Close".shift(1)
gain = delta.apply(_gz)
loss = delta.apply(_lz)
hdr = []
let i = 1
val = gain.values
while i <= length(val)
tmp = NaN
if i == period
tmp = mean(val[1:i])
end
if i > period
tmp = (hdr[end] * (period - 1) + val[i]) / period
end
push!(hdr, tmp)
i += 1
end
end
ag = py"pd.Series($hdr, index=$gain.index)"
hdr = []
let i = 1
val = loss.values
while i <= length(val)
tmp = NaN
if i == period
tmp = mean(val[1:i])
end
if i > period
tmp = (hdr[end] * (period - 1) + val[i]) / period
end
push!(hdr, tmp)
i += 1
end
end
al = py"pd.Series($hdr, index=$loss.index)"
rs = ag / al
h = 100 - 100 / (1 + rs)
setproperty!(h, "name", "RSI" * string(period))
end

function adx(x, period=14)
atr_ = atr(x , period)
hcp = x."High" - x."High".shift(1)
lpc = x."Low".shift(1) - x."Low"
py"""
def _hgl(_):
    if _[0] > _[-1] and _[0] > 0: return _[0]
    return 0
"""
dm_plus = py"pd.DataFrame([$hcp, $lpc]).T.apply(_hgl, axis=1)"
dm_minus = py"pd.DataFrame([$lpc, $hcp]).T.apply(_hgl, axis=1)"
iph = []
let i = 1
val = dm_plus.values
while i <= length(val)
tmp = NaN
if i == period
tmp = mean(val[1:i])
end
if i > period
tmp = (iph[end] * (period - 1) + val[i]) / period
end
push!(iph, tmp)
i += 1
end
end
di_plus = py"pd.Series($iph, index=$dm_plus.index) / $atr_ * 100"
imh = []
let i = 1
val = dm_minus.values
while i <= length(val)
tmp = NaN
if i == period
tmp = mean(val[1:i])
end
if i > period
tmp = (imh[end] * (period - 1) + val[i]) / period
end
push!(imh, tmp)
i += 1
end
end
di_minus = py"pd.Series($imh, index=$dm_minus.index) / $atr_ * 100"
dx = (di_plus - di_minus).abs() / (di_plus + di_minus) * 100
py"""
_, hdr, __, val = 0, [], 0, nan
while _ < len($dx):
    if not isnan($dx[_]):
        if __ == $period: val = $dx[_ - __:_].mean()
        if __ > $period: val = (hdr[-1] * ($period - 1) + $dx[__]) / $period
        __ += 1
    hdr.append(val)
    _ += 1
"""
g = py"pd.Series(hdr, index=$dx.index)"
setproperty!(di_plus, "name", "+DI" * string(period))
setproperty!(di_minus, "name", "-DI" * string(period))
setproperty!(g, "name", "ADX" * string(period))
h = py"pd.DataFrame([$di_plus, $di_minus, $g]).T"
end

function obv(x)
dcp = x."Close" - x."Close".shift(1)
hdr = [x."Volume".values[1]]
let i = 2
val = x."Volume".values
while i <= length(val)
tmp = 0
if dcp[i] > 0
tmp = val[i]
end
if dcp[i] < 0
tmp = -val[i]
end
push!(hdr, hdr[end] + tmp)
i += 1
end
end
h = py"pd.Series($hdr, index=$dcp.index)"
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
    t_str = 'SELECT DISTINCT eid FROM records ORDER BY eid ASC'
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
q_str = "SELECT DISTINCT eid FROM records ORDER BY eid ASC"
c in py"pd.read_sql($q_str, engine)".values
end
