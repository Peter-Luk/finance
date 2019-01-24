import pandas, numpy, sqlalchemy, fix_yahoo_finance
from scipy.constants import golden_ratio
from datetime import datetime
from functools import reduce
from time import sleep

y2n = [pandas, numpy, sqlalchemy, fix_yahoo_finance, datetime, sleep]
nta = [pandas, numpy, datetime, golden_ratio]
utils = [sqlalchemy, fix_yahoo_finance, golden_ratio, sleep, datetime, reduce]
public_holiday = {2015:{1:(1,), 2:(19, 20), 4:(3, 6, 7), 5:(25,), 7:(1,), 9:(28,), 10:(1, 21), 12:(25,)}}
periods = dict(
    Futures = dict(
        macd = dict(
            fast = 5,
            slow = 10,
            signal = 3),
        soc = dict(
            K = 14,
            D = 3),
        atr = 12,
        er = 7,
        fast = 2,
        slow = 12,
        adx = 7,
        simple = 12,
        apz = 5),
    Equities = dict(
        macd = dict(
            fast = 12,
            slow = 26,
            signal = 9),
        soc = dict(
            K = 14,
            D = 3),
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
        exclude = [368, 805]),
    Futures = dict(
        name = 'Futures',
        table = 'records',
        index = 'date',
        freq = 'bi-daily')
)
public_holiday[2016] = {1:(1,), 2:(8, 9, 10), 3:(25, 28), 4:(4,), 5:(2,), 6:(9,), 7:(1,), 9:(16,), 10:(10,), 12:(26, 27)}
public_holiday[2017] = {1:(2, 30, 31), 4:(4, 14, 17), 5:(1, 3, 30), 10:(2, 5), 12:(25, 26)}
public_holiday[2018] = {1:(1,), 2:(16, 19), 3:(30,), 4:(2, 5), 5:(1, 22), 6:(18,), 7:(2,), 9:(25,), 10:(1, 17), 12:(25, 26)}
public_holiday[2019] = {1:(1,), 2:(5, 6, 7), 4:(5, 19, 20, 22), 5:(1, 12, 13), 6:(7,), 7:(1,), 9:(14,), 10:(1, 7), 12:(25, 26)}
public_holiday[2020] = {1:(1, 25, 27, 28), 2:(14,), 3:(20,), 4:(4, 10, 11, 12, 13, 30), 5:(1, 10), 6:(20, 21, 25), 7:(1,), 9:(22,), 10:(1, 2, 26), 12:(21, 25, 26)}
