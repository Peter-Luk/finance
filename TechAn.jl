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

function sma(x::Any, period::Signed=Eperiod["simple"], dt::Any=nothing, rf::String="c"; field_initial::String=rf, date::Any=dt)
d = grabber(data_factory(x, date=date), field_initial).rolling(period).mean()
setproperty!(d, "name", "SMA" * string(period))
end

function wma(x::Any, period::Signed=Eperiod["simple"], rf::String="c", dt::Any=nothing; field_initial::String=rf, date::Any=dt)
y = data_factory(x, date=date)
data = grabber(y, field_initial)
d = (data * y.Volume).rolling(period).sum() / y.Volume.rolling(period).sum()
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

function ema(x::Any, period::Signed=Eperiod["simple"], rf::String="c", dt::Any=nothing; field_initial::String=rf, date::Any=dt)
y = data_factory(x, date=date)
data = stepper(grabber(y, field_initial), period)
d = py"pd.Series($data, index=$y.index)"
setproperty!(d, "name", "EMA"* string(period))
end

function kama(x::Any, period::Dict=Eperiod["kama"], rf::String="c", dt::Any=nothing; field_initial::String=rf, date::Any=dt)
data = grabber(data_factory(x, date=date), field_initial)
change = (data - data.shift(period["er"])).abs()
volatility = data.diff(1).abs().rolling(period["er"]).sum()
er = change / volatility
sc = (er * (2 / (period["fast"] + 1) - 2 / (period["slow"] + 1)) + 2 / (period["slow"] + 1)) ^ 2
hdr = []
val = data.values
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
d = py"pd.Series($hdr, index=$data.index)"
setproperty!(d, "name", "KAMA" * string(period["er"]))
end

function apz(x::Any, period::Signed=Eperiod["apz"], dt::Any=nothing; date::Any=dt)
y = data_factory(x, date=date)
ehl = ema(y, period, "hl")
tmp = stepper(ehl, period)
volatility = py"pd.Series($tmp, index=$ehl.index)"
tr = py"pd.DataFrame([$y['High'] - $y['Low'], ($y['High'] - $y['Close'].shift(1)).abs(), ($y['Low'] - $y['Close'].shift(1)).abs()]).max()"
gr = py"golden_ratio"
upper = volatility + tr * gr
lower = volatility - tr * gr
d = py"pd.concat([$upper, $lower], axis=1)"
setproperty!(d, "columns", ["Upper", "Lower"])
setproperty!(d, "name", "APZ" * string(period))
end

function kc(x::Any, period::Dict=Eperiod["kc"], dt::Any=nothing; date::Any=dt)
y = data_factory(x, date=date)
middle_line = kama(y, period["kama"], "hl")
atr_ = atr(y, period["atr"])
gr = py"golden_ratio"
upper = middle_line + (gr * atr_)
lower = middle_line - (gr * atr_)
d = py"pd.concat([$upper, $lower], axis=1)"
setproperty!(d, "columns", ["Upper", "Lower"])
end

function bb(x::Any, period::Signed=Eperiod["simple"], dt::Any=nothing; date::Any=dt)
y = data_factory(x, date=date)
middle_line = sma(y, period)
width = y.Close.rolling(period).std()
upper = middle_line + width
lower = middle_line - width
d = py"pd.concat([$upper, $lower], axis=1)"
setproperty!(d, "columns", ["Upper", "Lower"])
setproperty!(d, "name", "BB" * string(period))
end

function macd(x::Any, period::Dict=Eperiod["macd"], dt::Any=nothing; date::Any=dt)
y = data_factory(x, date=date)
e_slow = ema(y, period["slow"], "hl")
e_fast = ema(y, period["fast"], "hl")
m_line = e_fast - e_slow
tmp = stepper(m_line, period["signal"])
s_line = py"pd.Series($tmp, index=$m_line.index)"
m_hist = m_line - s_line
setproperty!(m_line, "name", "M Line")
setproperty!(s_line, "name", "Signal")
setproperty!(m_hist, "name", "Histogram")
h = py"pd.concat([$m_line, $s_line, $m_hist], axis=1)"
setproperty!(h, "name", "MACD")
end

function soc(x::Any, dt::Any=nothing, pe::Dict=Eperiod["soc"]; date::Any=dt, period::Dict=pe)
y = data_factory(x, date=date)
ml = y.Low.rolling(period["K"]).min()
mh = y.High.rolling(period["K"]).max()
kseries = py"pd.Series(($y.Close - $ml) / ($mh - $ml) * 100, index=$y.index)"
k = kseries.rolling(period["D"]).mean()
d = k.rolling(period["D"]).mean()
setproperty!(k, "name", "%K")
setproperty!(d, "name", "%D")
hdr = py"pd.concat([$k, $d], axis=1)"
setproperty!(hdr, "name", "SOC@" * string(get(y.index, length(y) - 1)))
end

function stc(x::Any, period::Dict=Eperiod["stc"], dt::Any=nothing; date::Any=dt)
y = data_factory(x, date=date)
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

function atr(x::Any, pe::Signed=Eperiod["atr"], dt::Any=nothing; date::Any=dt, period::Signed=pe)
y = data_factory(x, date=date)
tr = py"pd.DataFrame([$y.High - $y.Low, ($y.High - $y.Close.shift(1)).abs(), ($y.Low - $y.Close.shift(1)).abs()]).max()"
tmp = stepper(tr, period)
d = py"pd.Series($tmp, index=$y.index)"
setproperty!(d, "name", "ATR"* string(period))
end

function rsi(x::Any, period::Signed=Eperiod["rsi"], dt::Any=nothing; date::Any=dt)
y = data_factory(x, date=date)
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

function adx(x::Any, period::Signed=Eperiod["adx"], dt::Any=nothing; date::Any=dt)
y = data_factory(x, date=date)
atr_ = atr(y, period)
hcp = y.High.diff(1)
lpc = abs(y.Low.diff(1))
py"""
def _hgl(_):
    if _[0] > _[-1] and _[0] > 0: return _[0]
    return 0
"""
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

function obv(x::Any, dt::Any=nothing; date::Any=dt)
y = data_factory(x, date=date)
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

function vwap(x::Any, dt::Any=nothing; date::Any=dt)
y = data_factory(x, date=date)
pv = y.drop(["Open", "Volume"], 1).mean(axis=1) * y."Volume"
h = py"pd.Series($pv.cumsum() / $y['Volume'].cumsum(), index=$y.index)"
setproperty!(h, "name", "VWAP")
end

import Base.fetch
function fetch(c::Signed, adhoc::Bool=false)
function internal(code::Signed, start_from=py"start")
q_str = "SELECT date as Date, open as Open, high as High, low as Low, close as Close, volume as Volume FROM records WHERE eid=" * string(code) * " AND date>'" * string(start_from) * "'"
py"pd.read_sql($q_str, engine, index_col='Date', parse_dates=['Date'])"
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

function ratr(x::Any, dt::Any=nothing, ac::Bool=false, pe::Signed=Eperiod["atr"]; date::Any=dt, period::Signed=pe, adhoc::Bool=ac)
dta(x::Array, gr::Float64=py"golden_ratio") = [x[1] - gr * x[end], x[1], x[1] + gr * x[end]]
y = data_factory(x, adhoc, date=date)
tmp = dta(py"pd.concat([$y.Close, $(atr(y, period))], 1).iloc[-1].values")
re = sort!(unique!([gslice(tmp[1:end-1]); gslice(tmp[end-1:end])]))
hdr = []
for i in 1:length(re)-1
for j in gslice(re[i:i+1])
push!(hdr, j)
end
end
sort!(unique!(hdr))
end

function compose(code::Any=entities())
function grab(c)
#=
function grab()
while true
c = take!(c1)
=#
e = fetch(c)
r = rsi(e); a = atr(e); x = adx(e).ADX14.diff()
ph = py"pd.concat([$r, $(e.High.sub(e.Low)), $e.Close.diff(), $a, $x], axis=1)"
setproperty!(ph, "columns", ["RSI", "dHL", "dpC", "ATR", "dADX"])
#=
put!(c2, ph)
end
=#
end
cl = []
if typeof(code) <: Array
for c in code
if typeof(c) <: Signed; push!(cl, c); end
end
end
if typeof(code) <: Signed; push!(cl, code); end
#=
c1 = Channel{Signed}(length(cl))
c2 = Channel{PyObject}(length(cl))
pl = []
foreach(c->put!(c1,c),cl)
for c in cl; @async grab(); end
for c in cl; push!(pl, take!(c2)); end
close(c1); close(c2)
=#
pl = pmap(grab, cl, batch_size=ceil(Int, length(cl)/nworkers()))
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
#=
hdr = map(y -> y .+ diff(x) .* [-1 + 1 / ratio, -1 / ratio, 1, 1 / ratio, 1 - 1 / ratio], x)
=#
dx = abs(diff(x)[1])
hdr =[x[1], x[1] + dx * (1 - 1 / ratio), x[1] + dx / ratio, x[1] + dx]
sort!(unique!(hdr))
end

function data_factory(x::Any, adhoc::Bool=false, dt::Any=nothing; date::Any=dt)
if ~(typeof(x) <: PyObject)
if typeof(x) <: Signed; x = py"platform" == "linux" ? fetch(x, adhoc) : fetch(x, false); end
end
if typeof(date) <: Nothing; date = get(x.index, length(x) -1); end
y = py"$x.loc[:$date]"
end

exist(c::Signed) = c in entities()
