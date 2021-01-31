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
    import yfinance as yf
    dir_ = '~/storage/shared'
    if 'EXTERNAL_STORAGE' in pathlib.os.environ.keys():
        dir_ = '~/storage/external-1'
dir_ += f'/data/sqlite3/{db_name}'
path = pathlib.Path(dir_)
engine = sqa.create_engine(f'sqlite:///{path.expanduser()}')
"""
