import multiprocessing
# from concurrent.futures import ThreadPoolExecutor
from y2n import Equities, entities, pref, datetime, tqdm, gr, pd


def grab(c):
    e = Equities(c)
    rd = e.data
    pdhr = pd.concat(
        [e.rsi(), rd.High.sub(rd.Low),
            rd.Close.diff(), e.atr(),
            e.adx()[f"ADX{pref.periods['Equities']['adx']}"].diff()],
        axis=1)
    pdhr.columns = ['RSI', 'dHL', 'dpC', 'ATR', 'dADX']
    return pdhr


def mav(c, date):
    e = Equities(c).maverick(date, unbound=False)
    return e


def compose(code=entities(pref.db['Equities']['name'])):
    # with ThreadPoolExecutor(max_workers=4) as pool:
    #     r = list(tqdm(pool.map(grab, code), total=len(code)))
    with multiprocessing.Pool() as pool:
        # r = pool.map(grab, tqdm(code))
        r = list(tqdm(pool.imap(grab, code), total=len(code)))
    return pd.concat(r, keys=code, names=['Code', 'Data'], axis=1)


def strayed(df, date, buy=True):
    if isinstance(date, str):
        try:
            date = datetime.strptime(date, "%Y%m%d")
        except Exception:
            pass
    if isinstance(date, datetime):
        txr = df.reorder_levels(['Data', 'Code'], 1)
        rtr = txr.loc[date, 'RSI']
        hdr = []
        if buy:
            rl = rtr[
                (rtr < (1 - 1 / gr) * 100) & (txr.loc[
                    date, 'dpC'].abs() > txr.loc[date, 'ATR'])].index.tolist()
            if rl:
                al = [(_, date) for _ in rl]
                with multiprocessing.Pool() as pool:
                    r = pool.starmap(mav, al)
                    hdr.extend([_.loc["buy", date] for _ in r])
                return pd.Series(hdr, index=rl, name='buy')
        else:
            rl = rtr[
                (rtr > 1 / gr * 100) & (txr.loc[date, 'dpC'] > txr.loc[
                    date, 'ATR'])].index.tolist()
            if rl:
                al = [(_, date) for _ in rl]
                with multiprocessing.Pool() as pool:
                    r = pool.starmap(mav, al)
                    hdr.extend([_.loc["sell", date] for _ in r])
                return pd.Series(hdr, index=rl, name='sell')


def press(__):
    _ = Equities(__)
    return f'{_}\n{_()}\n{_.gat()}\n'


def summary(__):
    # ae = entities()
    if not isinstance(__, (tuple, list)):
        __ = [__]
    try:
        with multiprocessing.Pool() as pool:
            _ = pool.map(press, __)
            # _ = pool.map(press, [_ for _ in __ if _ in ae])
    except Exception:
        _ = [press(___) for ___ in __]
    print('\n'.join(_))
