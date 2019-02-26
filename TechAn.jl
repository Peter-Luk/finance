using PyCall
py"""
import pandas as pd
import sqlalchemy as sqa
import pathlib
from numpy import nan
from datetime import datetime
start = datetime(datetime.today().year - 4, 12, 31).date()
dir_ = ''
platform = pathlib.sys.platform
if platform in ['linux']:
    import fix_yahoo_finance as yf
    dir_ = 'storage/shared'
    if 'EXTERNAL_STORAGE' in pathlib.os.environ.keys():
        dir_ = 'storage/external-1'
d_loc = f"sqlite:///{pathlib.Path.home()}"
if dir_: d_loc += f"/{dir_}"
d_loc += "/data/sqlite3/Securities"
engine = sqa.create_engine(d_loc)
"""
sma(x, period=20) = py"$x['Close'].rolling($period).mean()"
function wma(x, period=20)
py"""
iS = $x
period = $period
d = (iS['Close'] * iS['Volume']).rolling(period).sum() / iS['Volume'].rolling(period).sum()
d.name = f'WMA{period:02d}'
"""
py"d"
end
function ema(x, period=20)
py"""
iS = $x['Close']
period = $period
tl = []
i = 0
while i < iS.size:
    hdr = nan
    if i == period: hdr = iS[:period].mean()
    if i > period: hdr = (tl[-1] * (period - 1) + iS[i]) / period
    tl.append(hdr)
    i += 1
s = pd.Series(tl, index=$x.index)
s.name = f"EMA{period:02d}"
"""
py"s"
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
        d = yf.download(f"{code:04d}.HK", start, group_by='ticker')
        d.drop("Adj Close", 1, inplace=True)
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
