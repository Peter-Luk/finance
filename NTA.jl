module NTA
include("pandas_helper.jl")
Eperiod = periods["Equities"]

export kama

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

function data_factory(x::Any, adhoc::Bool=false, dt::Any=nothing; date::Any=dt)
if ~(typeof(x) <: PyObject)
if typeof(x) <: Signed; x = py"platform" == "linux" ? fetch(x, adhoc) : fetch(x, false); end
end
if typeof(date) <: Nothing; date = get(x.index, length(x) -1); end
y = py"$x.loc[:$date]"
end

end
