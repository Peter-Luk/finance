from trial01 import I2
import pandas as pd
import numpy

def fdc(**args):
    mf, option, dd = I2(code=args['code']), 'basic', {}
    r_date = mf.trade_day

    if 'option' in args.keys():option = args['option']

    if option == 'basic':
        data, hdr = [], {}
        for i in r_date:
            hdr = mf._I2__rangefinder(field='date', value=i)['D']
            hdr['date'] = pd.Timestamp(i)
            data.append(hdr)
        for dk in ('date', 'open', 'high', 'low', 'close'): dd[dk.capitalize()] = [data[i][dk] for i in range(len(data))]
    elif option == 'indicators':
        ilist = ('SMA', 'WMA', 'EMA', 'KAMA')
        dd['Date'] = []
        for d in r_date:
            if r_date.index(d) > mf._I2__period: dd['Date'].append(pd.Timestamp(d))
        for i in ilist: dd[i] = [float('%0.3f' % eval("mf.%s(date='%s')" % (i, d))) for d in dd['Date']]
    return pd.DataFrame(dd)
