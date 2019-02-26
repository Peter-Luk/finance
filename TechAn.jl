using PyCall
py"""
import pandas as pd
import sqlalchemy as sqa
import pathlib
from numpy import nan, isnan
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
"""
sma(x, period=20) = py"$x['Close'].rolling($period).mean()"

function wma(x, period=20)
py"""
raw = $x
period = $period
_ = (raw['Close'] * raw['Volume']).rolling(period).sum() / raw['Volume'].rolling(period).sum()
_.name = f'WMA{period:02d}'
"""
py"_"
end

function ema(x, period=20)
py"""
raw = $x
period = $period
tl, _ = [], 0
while _ < raw['Close'].size:
    hdr = nan
    if _ == period: hdr = raw['Close'][:period].mean()
    if _ > period: hdr = (tl[-1] * (period - 1) + raw['Close'][_]) / period
    tl.append(hdr)
    _ += 1
_ = pd.Series(tl, index=raw.index)
_.name = f'EMA{period:02d}'
"""
py"_"
end

function atr(x, period=14)
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

function fetch(c)
py"""
code = $c
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
py"d"
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
