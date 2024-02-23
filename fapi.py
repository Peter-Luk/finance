#!/usr/bin/env python3
import uvicorn
import pretty_errors
from fastapi import FastAPI
from asa import Equity
# from finance import Equity
# from finance.fat import prefer_stock
# from finance.utilities import IP, tse_stock_code
from finance.utilities import IP

app = FastAPI()


def ind_status(b:float, i:float) -> str:
    return  '+' if b > i else '-' if b < i else '='


@app.get("/hkex/{code}")
async def quote_hk(code: int):
    _ = await Equity(code, static=False)
    return {_.yahoo_code: f'{_}'}


@app.get("/hkex/{code}/optinum")
async def optinum_hk(code: int):
    _ = await Equity(code, static=False)
    return _.optinum()


@app.get("/tse/{code}")
async def quote_tse(code: str):
    _ = await Equity(code, boarse='TSE')
    return {_.yahoo_code: f'{_}'}
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
    ind_list: list = ['kama', 'sar', 'rsi']
    data = await Equity(code, boarse="NYSE")
    data_close = data._Equity__data.Close
    d0 = data.date
    result = {f'{d0:%d-%m-%Y}': f'{data.change:+.2%}'}
    result['close'] = round(data.close, 2)
    for _ in ind_list:
        res = {'value': eval(f'round(data.{_}().loc[d0], 3)')}
        if _ == 'rsi':
            res['change'] = round(data.rsi().diff().loc[d0], 3)
        else:
            ts = eval(f'(data_close - data.{_}()).diff()')
            res['delta'] = ind_status(abs(ts[-2]), abs(ts[-1]))
        result[_] = res
    return {data.yahoo_code: result}


@app.get("/nyse/{code}/optinum")
async def optinum_nyse(code: str):
    _ = await Equity(code, boarse="NYSE")
    return _.optinum()


if __name__ == "__main__":
    uvicorn.run("fapi:app", host=f'{IP()}', port=5000, reload=True, access_log=False)
