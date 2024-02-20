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
async def quote_nyse(code: str):
    _ = await Equity(code, boarse="NYSE")
    result = {f'{_.date:%d-%m-%Y}': f'{_.change:+.2%}'}
    result['close'] = _.close
    result['kama'] = float(f'{_.kama().loc[_.date]:.3f}')
    result['sar'] = float(f'{_.sar().loc[_.date]:.3f}')
    result['rsi'] = float(f'{_.rsi().loc[_.date]:.3f}')

    return {_.yahoo_code: result}


@app.get("/nyse/{code}/optinum")
async def optinum_nyse(code: str):
    _ = await Equity(code, boarse="NYSE")
    return _.optinum()


if __name__ == "__main__":
    uvicorn.run("fapi:app", host=f'{IP()}', port=5000, reload=True, access_log=False)
