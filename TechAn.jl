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

function grab(x, involved="c")
li = lowercase(involved)
if li == "c"
return x.Close
end
if li  in ["hl", "lh", "range"]
return x.drop(["Open", "Close", "Volume"], 1).mean(axis=1)
end
if li in ["ohlc", "full", "all"]
return x.drop("Volume", 1).mean(axis=1)
end
end

sma(x, period=Eperiod["simple"]) = x."Close".rolling(period).mean()

function wma(x, period=Eperiod["simple"], rf="c"; field_initial=rf)
_data = grab(x, field_initial)
d = (_data * x."Volume").rolling(period).sum() / x."Volume".rolling(period).sum()
setproperty!(d, "name", "WMA" * string(period))
end

function ema(x, period=Eperiod["simple"], rf="c"; field_initial=rf)
_data = grab(x, field_initial)
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

function kama(x, period=Eperiod["kama"], rf="c"; field_initial=rf)
_data = grab(x, field_initial)
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

function apz(x, period=Eperiod["apz"])
ehl = ema(x, period, "hl")

hdr = []
global j = 0
for i in 1:length(ehl.values)
if isnan(ehl.values[i])
push!(hdr, NaN)
else
if j < period
push!(hdr, NaN)
end
if j == period
push!(hdr, mean(ehl.values[i - j: i]))
end
if j > period
push!(hdr, (hdr[end] * (period - 1) + ehl.values[i]) / period)
end
j += 1
end
end
volatility = py"pd.Series($hdr, index=$ehl.index)"
tr = py"pd.DataFrame([$x['High'] - $x['Low'], ($x['High'] - $x['Close'].shift(1)).abs(), ($x['Low'] - $x['Close'].shift(1)).abs()]).max()"
gr = py"golden_ratio"
upper = volatility + tr * gr
lower = volatility - tr * gr
d = py"pd.DataFrame([$upper, $lower]).T"
setproperty!(d, "columns", ["Upper", "Lower"])
setproperty!(d, "name", "APZ" * string(period))
end

function kc(x, period=Eperiod["kc"])
middle_line = kama(x, period["kama"], "hl")
atr_ = atr(x, period["atr"])
gr = py"golden_ratio"
upper = middle_line + (gr * atr_)
lower = middle_line - (gr * atr_)
d = py"pd.DataFrame([$upper, $lower]).T"
setproperty!(d, "columns", ["Upper", "Lower"])
end

function bb(x, period=Eperiod["simple"])
middle_line = sma(x, period)
width = x."Close".rolling(period).std()
upper = middle_line + width
lower = middle_line - width
d = py"pd.DataFrame([$upper, $lower]).T"
setproperty!(d, "columns", ["Upper", "Lower"])
setproperty!(d, "name", "BB" * string(period))
end

function macd(x, period=Eperiod["macd"])
function pema(pd_data, period)
hdr = []
global j = 0
for i in 1:length(pd_data.values)
if isnan(pd_data.values[i])
push!(hdr, NaN)
else
if j < period
push!(hdr, NaN)
end
if j == period
push!(hdr, mean(pd_data.values[i - j: i]))
end
if j > period
push!(hdr, (hdr[end] * (period - 1) + pd_data.values[i]) / period)
end
j += 1
end
end
py"pd.Series($hdr, index=$pd_data.index)"
end
e_slow = ema(x, period["slow"], "hl")
e_fast = ema(x, period["fast"], "hl")
m_line = e_fast - e_slow
s_line = pema(m_line, period["signal"])
m_hist = m_line - s_line
setproperty!(m_line, "name", "M Line")
setproperty!(s_line, "name", "Signal")
setproperty!(m_hist, "name", "Histogram")
h = py"pd.DataFrame([$m_line, $s_line, $m_hist]).T"
setproperty!(h, "name", "MACD")
end

function soc(x, period=Eperiod["soc"])
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

function stc(x, period=Eperiod["stc"])
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

function atr(x, period=Eperiod["atr"])
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

function rsi(x, period=Eperiod["rsi"])
function _gz(x)
x > 0 ? x : 0
end
function _lz(x)
x < 0 ? abs(x) : 0
end
delta = x."Close".diff(1)
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

function adx(x, period=Eperiod["adx"])
atr_ = atr(x , period)
hcp = x."High".diff(1)
lpc = -(x."Low".diff(1))
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

hdr = []
global j = 0
for i in 1:length(dx.values)
if isnan(dx.values[i])
push!(hdr, NaN)
else
if j < period
push!(hdr, NaN)
end
if j == period
push!(hdr, mean(dx.values[i - j: i]))
end
if j > period
push!(hdr, (hdr[end] * (period - 1) + dx.values[i]) / period)
end
j += 1
end
end
g = py"pd.Series($hdr, index=$dx.index)"
setproperty!(di_plus, "name", "+DI" * string(period))
setproperty!(di_minus, "name", "-DI" * string(period))
setproperty!(g, "name", "ADX" * string(period))
py"pd.DataFrame([$di_plus, $di_minus, $g]).T"
end

function obv(x)
dcp = x."Close".diff(1)
hdr = [get(x."Volume", 0)]
let i = 1
val = x."Volume"
while i < length(val)
tmp = 0
if get(dcp, i) > 0
tmp = get(val, i)
end
if get(dcp, i) < 0
tmp = -(get(val, i))
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
function internal(code, start_from=py"start")
q_str = "SELECT date, open, high, low, close, volume FROM records WHERE eid=" * string(code) * " AND date>'" * string(start_from) * "'"
pp2f(py"pd.read_sql($q_str, engine, index_col='date', parse_dates=['date'])", "capitalize")
end

function yahoo(code, start_from=py"start")
c = lpad(code, 4, '0') * ".HK"
d = py"yf.download($c, start, group_by='ticker')"
d.drop("Adj Close", 1, inplace=true)
return d
end

if adhoc
d = py"pd.DataFrame()"
if py"platform" in ["linux"]
d = yahoo(c)
end
else
let exist = false
t_str = "SELECT DISTINCT eid FROM records ORDER BY eid ASC"
uid = py"pd.read_sql($t_str, engine)"
if c in uid.values
exist = true
end
if exist
d = internal(c, py"start")
else
if py"platform" in ["linux"]
d = yahoo(c)
end
end
end
end
if ~d.empty
d
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
