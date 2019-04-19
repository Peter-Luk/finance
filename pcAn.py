import multiprocessing
from y2n import Equities, entities, pref, datetime, gr, pd
from tqdm import tqdm

def grab(c):
    e = Equities(c)
    rd = e.data
    pdhr = pd.concat([e.rsi(), rd.High.sub(rd.Low), rd.Close.diff(), e.atr(), e.adx()[f"ADX{pref.periods['Equities']['adx']}"].diff()], axis=1)
    pdhr.columns = ['RSI', 'dHL', 'dpC', 'ATR', 'dADX']
    return pdhr

def mav(c):
    e = Equities(c).maverick(unbound=False)
    return e

def compose(code=entities(pref.db['Equities']['name'])):
    with multiprocessing.Pool() as pool:
        # r = pool.map(grab, tqdm(code))
        r = list(tqdm.tqdm(p.imap(grab, code), total=len(code)))
    return pd.concat(r, keys=code, names=['Code', 'Data'], axis=1)

def strayed(df, date, buy=True):
    if isinstance(date, str):
        try: date = datetime.strptime(date, "%Y%m%d")
        except: pass
    if isinstance(date, datetime):
        txr = df.reorder_levels(['Data','Code'], 1)
        rtr = txr.loc[date, 'RSI']
        hdr = []
        if buy:
            rl = rtr[(rtr < (1 - 1 / gr) * 100) & (txr.loc[date, 'dpC'].abs() > txr.loc[date, 'ATR'])].index.tolist()
            if rl:
                with multiprocessing.Pool() as pool:
                    r = pool.map(mav, tqdm(rl))
                    hdr.extend([_.loc["buy", date] for _ in r])
                return pd.Series(hdr, index=rl, name='buy')
        else:
            rl = rtr[(rtr > 1 / gr * 100) & (txr.loc[date, 'dpC'] > txr.loc[date, 'ATR'])].index.tolist()
            if rl:
                with multiprocessing.Pool() as pool:
                    r = pool.map(mav, tqdm(rl))
                    hdr.extend([_.loc["sell", date] for _ in r])
                return pd.Series(hdr, index=rl, name='sell')
