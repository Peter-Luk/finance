include("pref.jl")
using Pandas
py"""
import pandas as pd
import sqlalchemy as sqa
import pathlib
from scipy.constants import golden_ratio
from datetime import datetime
start = datetime(datetime.today().year - 4, 12, 31).date()
dir_, db_name, platform = '~', db['Equities']['name'], pathlib.sys.platform
if platform in ['linux']:
    import fix_yahoo_finance as yf
    dir_ = '~/storage/shared'
    if 'EXTERNAL_STORAGE' in pathlib.os.environ.keys():
        dir_ = '~/storage/external-1'
dir_ += f'/data/sqlite3/{db_name}'
path = pathlib.Path(dir_)
engine = sqa.create_engine(f'sqlite:///{path.expanduser()}')
"""
Eperiod = periods["Equities"]

function rdf(o::PyObject, s::Signed, e::Signed=0)
if e == 0; return py"$o.iloc[$s:]"; end
if e != 0; return py"$o.iloc[$s:$e]"; end
end

function idf(o::PyObject, s::Any)
if typeof(s) <: String; return py"$o.loc[$s]"; end
if typeof(s) <: Signed; return py"$o.iloc[$s]"; end
end

function grabber(x::PyObject, initial::String="c")
li = lowercase(initial)
if li in ["c", "close"]; return x.Close; end
if li in ["h", "high"]; return x.High; end
if li in ["l", "low"]; return x.Low; end
if li in ["o", "open"]; return x.Open; end
if li in ["hl", "lh", "range"]
return x.drop(["Open", "Close", "Volume"], 1).mean(axis=1)
end
if li in ["ohlc", "full", "all"]
return x.drop("Volume", 1).mean(axis=1)
end
end

function sma(x::Any, period::Signed=Eperiod["simple"], rf::String="c"; field_initial::String=rf)
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
grabber(y, field_initial).rolling(period).mean()
end

function wma(x::Any, period::Signed=Eperiod["simple"], rf::String="c"; field_initial::String=rf)
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
_data = grabber(y, field_initial)
d = (_data * y.Volume).rolling(period).sum() / y.Volume.rolling(period).sum()
setproperty!(d, "name", "WMA" * string(period))
end

function stepper(x::PyObject, period::Signed)
hdr = []
global j = 0
for i in 1:length(x.values)
if isnan(x.values[i])
push!(hdr, NaN)
else
if j < period
push!(hdr, NaN)
end
if j == period
push!(hdr, mean(x.values[i - j: i]))
end
if j > period
push!(hdr, (hdr[end] * (period - 1) + x.values[i]) / period)
end
j += 1
end
end
return hdr
end 

function ema(x::Any, period::Signed=Eperiod["simple"], rf::String="c"; field_initial::String=rf)
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
_data = grabber(y, field_initial)
tmp = stepper(_data, period)
d = py"pd.Series($tmp, index=$_data.index)"
setproperty!(d, "name", "EMA"* string(period))
end

function kama(x::Any, period::Dict=Eperiod["kama"], rf::String="c"; field_initial::String=rf)
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
_data = grabber(y, field_initial)
change = (_data - _data.shift(period["er"])).abs()
volatility = _data.diff(1).abs().rolling(period["er"]).sum()
er = change / volatility
sc = (er * (2 / (period["fast"] + 1) - 2 / (period["slow"] + 1)) + 2 / (period["slow"] + 1)) ^ 2
hdr = []
val = _data.values
let i = 1
while i <= length(val)
j = NaN
if i == period["slow"]
j = mean(val[1:i])
end
if i > period["slow"]
j = hdr[end] + get(sc, i - 1) * (val[i] - hdr[end])
end
push!(hdr, j)
i += 1
end
end
d = py"pd.Series($hdr, index=$_data.index)"
setproperty!(d, "name", "KAMA" * string(period["er"]))
end

function apz(x::Any, period::Signed=Eperiod["apz"])
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
ehl = ema(y, period, "hl")
tmp = stepper(ehl, period)
volatility = py"pd.Series($tmp, index=$ehl.index)"
tr = py"pd.DataFrame([$y['High'] - $y['Low'], ($y['High'] - $y['Close'].shift(1)).abs(), ($y['Low'] - $y['Close'].shift(1)).abs()]).max()"
gr = py"golden_ratio"
upper = volatility + tr * gr
lower = volatility - tr * gr
d = py"pd.DataFrame([$upper, $lower]).T"
setproperty!(d, "columns", ["Upper", "Lower"])
setproperty!(d, "name", "APZ" * string(period))
end

function kc(x::Any, period::Dict=Eperiod["kc"])
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
middle_line = kama(y, period["kama"], "hl")
atr_ = atr(y, period["atr"])
gr = py"golden_ratio"
upper = middle_line + (gr * atr_)
lower = middle_line - (gr * atr_)
d = py"pd.DataFrame([$upper, $lower]).T"
setproperty!(d, "columns", ["Upper", "Lower"])
end

function bb(x::Any, period::Signed=Eperiod["simple"])
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
middle_line = sma(y, period)
width = y.Close.rolling(period).std()
upper = middle_line + width
lower = middle_line - width
d = py"pd.DataFrame([$upper, $lower]).T"
setproperty!(d, "columns", ["Upper", "Lower"])
setproperty!(d, "name", "BB" * string(period))
end

function macd(x::Any, period::Dict=Eperiod["macd"])
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
e_slow = ema(y, period["slow"], "hl")
e_fast = ema(y, period["fast"], "hl")
m_line = e_fast - e_slow
tmp = stepper(m_line, period["signal"])
s_line = py"pd.Series($tmp, index=$m_line.index)"
m_hist = m_line - s_line
setproperty!(m_line, "name", "M Line")
setproperty!(s_line, "name", "Signal")
setproperty!(m_hist, "name", "Histogram")
h = py"pd.DataFrame([$m_line, $s_line, $m_hist]).T"
setproperty!(h, "name", "MACD")
end

function soc(x::Any, period::Dict=Eperiod["soc"])
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
ml = y.Low.rolling(period["K"]).min()
mh = y.High.rolling(period["K"]).max()
kseries = py"pd.Series(($y['Close'] - $ml) / ($mh - $ml) * 100, index=$y.index)"
k = kseries.rolling(period["D"]).mean()
d = k.rolling(period["D"]).mean()
setproperty!(k, "name", "%K")
setproperty!(d, "name", "%D")
hdr = py"pd.DataFrame([$k, $d]).T"
setproperty!(hdr, "name", "SOC")
end

function stc(x::Any, period::Dict=Eperiod["stc"])
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
slow_ = ema(y, period["slow"], "hl")
fast_ = ema(y, period["fast"], "hl")
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

function atr(x::Any, period::Signed=Eperiod["atr"])
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
tr = py"pd.DataFrame([$y['High'] - $y['Low'], ($y['High'] - $y['Close'].shift(1)).abs(), ($y['Low'] - $y['Close'].shift(1)).abs()]).max()"
tmp = stepper(tr, period)
d = py"pd.Series($tmp, index=$y.index)"
setproperty!(d, "name", "ATR"* string(period))
end

function rsi(x::Any, period::Signed=Eperiod["rsi"])
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
function _gz(x::Number);x > 0 ? x : 0; end
function _lz(x::Number);x < 0 ? abs(x) : 0; end
delta = y.Close.diff(1)
gain = delta.apply(_gz)
loss = delta.apply(_lz)
tmp = stepper(gain, period)
ag = py"pd.Series($tmp, index=$gain.index)"
tmp = stepper(loss, period)
al = py"pd.Series($tmp, index=$loss.index)"
rs = ag / al
h = 100 - 100 / (1 + rs)
setproperty!(h, "name", "RSI" * string(period))
end

function adx(x::Any, period::Signed=Eperiod["adx"])
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
atr_ = atr(y , period)
hcp = y.High.diff(1)
lpc = abs(y.Low.diff(1))
py"""
def _hgl(_):
    if _[0] > _[-1] and _[0] > 0: return _[0]
    return 0
"""
#=
dm_plus = py"pd.DataFrame([$hcp, $lpc]).T.apply(_hgl, axis=1)"
dm_minus = py"pd.DataFrame([$lpc, $hcp]).T.apply(_hgl, axis=1)"
=#
dm_plus = py"pd.concat([$hcp, $lpc], axis=1).apply(_hgl, axis=1)"
dm_minus = py"pd.concat([$lpc, $hcp], axis=1).apply(_hgl, axis=1)"
tmp = stepper(dm_plus, period)
di_plus = py"pd.Series($tmp, index=$dm_plus.index) / $atr_ * 100"
tmp = stepper(dm_minus, period)
di_minus = py"pd.Series($tmp, index=$dm_minus.index) / $atr_ * 100"
dx = (di_plus - di_minus).abs() / (di_plus + di_minus) * 100
tmp = stepper(dx, period)
g = py"pd.Series($tmp, index=$dx.index)"
setproperty!(di_plus, "name", "+DI" * string(period))
setproperty!(di_minus, "name", "-DI" * string(period))
setproperty!(g, "name", "ADX" * string(period))
py"pd.DataFrame([$di_plus, $di_minus, $g]).T"
end

function obv(x::Any)
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
dcp = y.Close.diff(1)
hdr = [get(y.Volume, 0)]
let i = 1
val = y.Volume
while i < length(val)
tmp = 0
if get(dcp, i) > 0; tmp = get(val, i); end
if get(dcp, i) < 0; tmp = abs(get(val, i)); end
push!(hdr, hdr[end] + tmp)
i += 1
end
end
h = py"pd.Series($hdr, index=$dcp.index)"
setproperty!(h, "name", "OBV")
end

function vwap(x::Any)
if typeof(x) <: Signed; y = exist(x) ? dFetch(x, false) : dFetch(x, true); end
if typeof(x) <: PyObject; y = x; end
pv = y.drop(["Open", "Volume"], 1).mean(axis=1) * y."Volume"
h = py"pd.Series($pv.cumsum() / $y['Volume'].cumsum(), index=$y.index)"
setproperty!(h, "name", "VWAP")
end

function dFetch(c::Signed, adhoc::Bool=false)
function internal(code::Signed, start_from=py"start")
q_str = "SELECT date, open, high, low, close, volume FROM records WHERE eid=" * string(code) * " AND date>'" * string(start_from) * "'"
pp2f(py"pd.read_sql($q_str, engine, index_col='date', parse_dates=['date'])", "capitalize")
end

function yahoo(code::Signed, start_from=py"start")
c = lpad(code, 4, '0') * ".HK"
d = py"yf.download($c, start, group_by='ticker')"
d.drop("Adj Close", 1, inplace=true)
return d
end

if adhoc
d = py"pd.DataFrame()"
if py"platform" in ["linux"]; d = yahoo(c); end
else
if c in entities()
d = internal(c, py"start")
else
if py"platform" in ["linux"]; d = yahoo(c); end
end
end
if ~d.empty; d; end
end

function ratr(x::Any, adhoc::Bool=false, dt::Any=nothing; date::Any=dt)
dta(x::Array) = [x[1] - x[end], x[1], x[1] + x[end]]
if typeof(x) <: Signed; data = py"platform" == "linux" ? dFetch(x, adhoc) : dFetch(x, false); end
if typeof(x) <: PyObject; data = x; end
if typeof(date) <: Nothing; date = get(data.index, length(data) -1); end
tmp = dta(py"pd.concat([$data.Close, $(atr(data))],1).loc[$date].values")
sort!(unique!([gslice(tmp[1:end-1]);gslice(tmp[end-1:end])]))
end

function compose(code::Any=entities())
cl = []
if typeof(code) <: Array
for c in code
if typeof(c) <: Signed; push!(cl, c); end
end
end
if typeof(code) <: Signed; push!(cl, code); end
pl = []
for c in cl
e = dFetch(c)
r = rsi(e); a = atr(e); x = adx(e).ADX14.diff()
ph = py"pd.concat([$r, ($e.High - $e.Low), $e.Close.diff(), $a, $x], axis=1)"
setproperty!(ph, "columns", ["RSI", "dHL", "dpC", "ATR", "dADX"])
push!(pl, ph)
end
py"pd.concat($pl, keys=$cl, names=['Code', 'Data'], axis=1)"
end

function entities()
q_str = "SELECT DISTINCT eid FROM records ORDER BY eid ASC"
hdr = []
for i in py"pd.read_sql($q_str, engine)".values
if ~(i in py"db['Equities']['exclude']"); push!(hdr, i); end
end
return hdr
end

function gslice(x::Array, ratio::Float64=py"golden_ratio")
hdr = map(y -> y .+ diff(x) .* [-1 + 1 / ratio, -1 / ratio, 1, 1 / ratio, 1 - 1 / ratio], x)
sort!(unique!([hdr[1]; hdr[end]]))
end

exist(c::Signed) = c in entities()
