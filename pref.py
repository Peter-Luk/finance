import pandas, numpy, sqlalchemy, fix_yahoo_finance
from utilities import filepath, datetime, mtf, gslice, gr, lique
from time import sleep

y2n = [pandas, numpy, sqlalchemy, fix_yahoo_finance, filepath, datetime, mtf, sleep]
nta = [numpy, pandas, datetime, gslice, gr, lique]
periods = dict(
    Futures = dict(
        atr = 12,
        er = 7,
        fast = 2,
        slow = 12,
        adx = 7,
        simple = 12,
        apz = 5),
    Equities = dict(
        atr = 14,
        er = 10,
        fast = 2,
        slow = 30,
        adx = 14,
        simple = 20,
        apz = 5)
)
db = dict(
    Equities = dict(
        name = 'Securities',
        table = 'records',
        index = 'date',
        freq = 'daily',
        exclude = [805]),
    Futures = dict(
        name = 'Futures',
        table = 'records',
        index = 'date',
        freq = 'bi-daily')
)
