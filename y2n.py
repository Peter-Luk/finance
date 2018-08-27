import sqlite3 as lite
import numpy as np
import pandas as pd
import fix_yahoo_finance as yf
from utilities import filepath
from time import sleep

def fetch(code=None, start=None, table='Securities'):
    def stored_eid(table):
        conn = lite.connect(filepath(table))
        cur = conn.cursor()
        return [_ for _ in [__[0] for __ in cur.execute("SELECT DISTINCT eid FROM records ORDER BY eid ASC").fetchall()] if _ not in [805]]

    if not start: start = '2016-01-01'
    if code:
        if isinstance(code, [int, float]): aid = [int(code)]
    else: aid = stored_eid(table=table)
    res, process = {}, True
    while process:
        ar = yf.download(['{:04d}.HK'.format(_) for _ in aid], start, group_by='ticker')
        if len(ar): process = not process
        else:
            print('Retry in 30 seconds')
            sleep(30)
    for _ in aid:
        hdr = {}
        rd = ar['{:04d}.HK'.format(_)]
        d = rd.T
        hdr['Date'] = [__.to_pydatetime() for __ in rd.index]
        hdr['Data'] = np.asarray(pd.concat([rd[__] for __ in [___ for ___ in d.index if ___ not in ['Adj Close']]], axis=1, join='inner', ignore_index=False))
        res[_] = hdr
    return pd.Series(res)

def ma(raw, period=20, favour='s', req_field='close'):
    mres = []
    def process(raw, period, favour, req_field):
        res, i = [], 0
        while i < len(raw):
            if i < period - 1: res.append(np.nan)
            else:
                if req_field.lower() in ['close', 'c']: rdata = raw[i - period + 1: i + 1, -2]
                if req_field.lower() in ['full', 'f', 'ohlc', 'all']: rdata = raw[i - period + 1: i + 1, :-1].mean(axis=1)
                if req_field.lower() in ['range', 'hl', 'lh']: rdata = raw[i - period + 1: i + 1, 1:3].mean(axis=1)
                if favour[0].lower() == 's': res.append(rdata.sum() / period)
                if favour[0].lower() == 'w': res.append((rdata * raw[i - period + 1: i + 1, -1]).sum() / raw[i - period + 1: i + 1, -1].sum())
                if favour[0].lower() == 'e':
                    if i == period - 1: hdr = rdata.sum() / period
                    else: hdr = (res[-1] * (period - 1) + rdata[-1]) / period
                    res.append(hdr)
            i += 1
        return res
    rflag = np.isnan(raw['Data']).any(axis=1)
    if rflag.any():
        i = 0
        while i < len(raw['Data'][rflag]):
            mres.append(np.nan)
            i += 1
        mres.extend(process(raw['Data'][~rflag], period, favour, req_field))
    else: mres.extend(process(raw['Data'], period, favour, req_field))
    return pd.DataFrame({'{}ma'.format(favour).upper(): mres}, index=raw['Date'])
