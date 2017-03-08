from trial01 import I2
import pandas as pd

def fdc(**args):
    mf, option, dd = I2(code=args['code']), 'B', {}

    if 'option' in args.keys():option = args['option']

    if option == 'B':
        data, hdr = [], {}
        for i in mf.trade_day:
            hdr = mf._I2__rangefinder(field='date', value=i)['D']
            hdr['date'] = i
            data.append(hdr)
        for dk in ('date', 'open', 'high', 'low', 'close', 'range', 'volume'): dd[dk.capitalize()] = [data[i][dk] for i in range(len(data))]
    elif option == 'I':
        r_date = mf.trade_day[mf._I2__period + 1:]
        dd['Date'] = [pd.Timestamp(d) for d in r_date]
        dd['SMA'] = [mf.SMA(date=i) for i in r_date]
        dd['WMA'] = [mf.WMA(date=i) for i in r_date]
        dd['EMA'] = [mf.EMA(date=i) for i in r_date]
        dd['KAMA'] = [mf.KAMA(date=i) for i in r_date]
    return pd.DataFrame(dd)
