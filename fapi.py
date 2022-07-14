import uvicorn
from fastapi import FastAPI
from finance.fat import Equity, prefer_stock
from finance.utilities import IP

app = FastAPI()


@app.get("/hkex/{code}")
async def quote_hk(code: int):
    _ = Equity(code, False)
    return {_.yahoo_code: f'{_}'}


@app.get("/hkex/{code}/optinum")
async def optinum_hk(code: int):
    _ = Equity(code, False)
    return _.optinum()


@app.get("/tse/{code}")
async def quote_tse(code: str):
    __  = prefer_stock('TSE')
    if code.lower() in __.keys():
        _ = Equity(__[code.lower()], exchange='TSE')
        return {_.yahoo_code: f'{_}'}


@app.get("/tse/{code}/optinum")
async def optinum_tse(code: str):
    __  = prefer_stock('TSE')
    if code.lower() in __.keys():
        _ = Equity(__[code.lower()], exchange='TSE')
        return _.optinum()


@app.get("/nyse/{code}")
async def quote_nyse(code: str):
    _ = Equity(code, exchange="NYSE")
    return {_.yahoo_code: f'{_}'}


@app.get("/nyse/{code}/optinum")
async def optinum_nyse(code: str):
    _ = Equity(code, exchange="NYSE")
    return _.optinum()


if __name__ == "__main__":
    uvicorn.run("fapi:app", host=f'{IP()}', port=5000, reload=True, access_log=False)
