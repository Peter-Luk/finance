#!/usr/bin/env python3
import uvicorn
import pretty_errors
from fastapi import FastAPI
from asa import Equity
from utilities import IP, YAML

app = FastAPI()


def ind_status(b: float, i: float) -> str:
    return '+' if b > i else '-' if b < i else '='


def get_name(c: int, b: str = '') -> str:
    if b == 'TSE':
        # for k, v in eval(f"YAML.prefer_stock.{b}.items()"):
        for k, v in YAML.prefer_stock[b].items():
            if c == v:
                return k


def _processor(data, boarse: str) -> dict:
    ind_list: list = ['kama', 'sar', 'rsi', 'obv', 'kc']
    data_close = data._Equity__data.Close
    d0 = data.date
    pct_change_spec = '+.2%'
    # pct_change_spec = lambda d=2: f'+.{d}%'
    # change = round(data.change * 100, 2)
    result = {f'{d0:%d-%m-%Y}': f'{data.change:{pct_change_spec()}}'}
    result['close'] = round(data.close, 2)
    for _ in ind_list:
        val = eval(f'data.{_}()')
        res = {'value': round(val.loc[d0], 3)}
        if _ == 'rsi':
            res['change'] = round(data.rsi().diff().loc[d0], 3)
        elif _ == 'kc':
            # val = eval(f'data.{_}()')
            res = {}
            res['value'] = [round(val.Upper.loc[d0], 3),
                    round(val.Lower.loc[d0], 3)]
            res['change'] = [round(val.Upper.diff().loc[d0], 3),
                    round(val.Lower.diff().loc[d0], 3)]
            tsh = (data_close - val.Upper).diff()
            tsl = (data_close - val.Lower).diff()
            res['delta'] = [ind_status(abs(tsh.iloc[-2]), abs(tsh.iloc[-1])),
                    ind_status(abs(tsl.iloc[-2]), abs(tsl.iloc[-1]))]
        elif _ == 'obv':
            res['value'] = round(val.loc[d0], 1)
            res['change'] = f'{val.pct_change(fill_method=None).loc[d0]:{pct_change_spec()}}'
        else:
            ts = (data_close - val).diff()
            res['delta'] = ind_status(abs(ts.iloc[-2]), abs(ts.iloc[-1]))
        result[_] = res
    code = data.yahoo_code
    if boarse == 'TSE':
        c, b = code.split('.')
        code = get_name(int(c), boarse)
    return {code: result}


@app.get("/hkex/{code}")
async def quote_hk(code: int):
    _ = await Equity(code, static=False)
    # return _processor(_)
    return {_.yahoo_code: f'{_}'}


@app.get("/hkex/{code}/optinum")
async def optinum_hk(code: int):
    _ = await Equity(code, static=False)
    return _.optinum()


@app.get("/tse/{code}")
async def quote_tse(code: str):
    boarse: str = 'TSE'
    _ = await Equity(code, boarse=boarse)
    return _processor(_, boarse)
    # return {_.yahoo_code: f'{_}'}
    # _ = Equity(tse_stock_code(code), exchange='TSE')
    # return {_.yahoo_code: f'{_}'}


@app.get("/tse/{code}/optinum")
async def optinum_tse(code: str):
    _ = await Equity(code, boarse='TSE')
    return _.optinum()
    # __  = prefer_stock('TSE')
    # if code.lower() in __.keys():
    #     _ = Equity(__[code.lower()], exchange='TSE')
    #     return _.optinum()


@app.get("/nyse/{code}")
async def quote_nyse(code: str) -> dict:
    boarse: str = 'NYSE'
    data = await Equity(code, boarse=boarse)
    return _processor(data, boarse)
    # ind_list: list = ['kama', 'sar', 'rsi', 'obv', 'kc']
    # data_close = data._Equity__data.Close
    # d0 = data.date
    # result = {f'{d0:%d-%m-%Y}': f'{data.change:+.2%}'}
    # result['close'] = round(data.close, 2)
    # for _ in ind_list:
    #     val = eval(f'data.{_}()')
    #     res = {'value': round(val.loc[d0], 3)}
    #     if _ == 'rsi':
    #         res['change'] = round(data.rsi().diff().loc[d0], 3)
    #     elif _ == 'kc':
    #         # val = eval(f'data.{_}()')
    #         res = {}
    #         res['value'] = [round(val.Upper.loc[d0], 3),
    #                 round(val.Lower.loc[d0], 3)]
    #         res['change'] = [round(val.Upper.diff().loc[d0], 3),
    #                 round(val.Lower.diff().loc[d0], 3)]
    #         tsh = (data_close - val.Upper).diff()
    #         tsl = (data_close - val.Lower).diff()
    #         res['delta'] = [ind_status(abs(tsh[-2]), abs(tsh[-1])),
    #                 ind_status(abs(tsl[-2]), abs(tsl[-1]))]
    #     elif _ == 'obv':
    #         res['value'] = round(val.loc[d0], 1)
    #         res['change'] = f'{val.pct_change().loc[d0]:+.2%}'
    #     else:
    #         ts = (data_close - val).diff()
    #         res['delta'] = ind_status(abs(ts[-2]), abs(ts[-1]))
    #     result[_] = res
    # return {data.yahoo_code: result}


@app.get("/nyse/{code}/optinum")
async def optinum_nyse(code: str):
    _ = await Equity(code, boarse="NYSE")
    return _.optinum()


if __name__ == "__main__":
    uvicorn.run("fapi:app", host=f'{IP()}', port=5000, reload=True, access_log=False)
