from trial01 import I2
import pandas
import numpy

def fdc(**args):
    mf, option, dd = I2(code=args['code']), 'basic', {}
    r_date = mf.trade_day

    if 'option' in args.keys():option = args['option']

    if option == 'basic':
        data, hdr = [], {}
        for i in r_date:
            hdr = mf._I2__rangefinder(field='date', value=i)['D']
            hdr['date'] = pandas.Timestamp(i)
            data.append(hdr)
        for dk in ('date', 'open', 'high', 'low', 'close'): dd[dk.capitalize()] = [data[i][dk] for i in range(len(data))]
    elif option == 'indicators':
        ilist, hdr = ('SMA', 'WMA', 'EMA', 'KAMA'), []
        dd['Date'] = []
        for d in r_date:
            if r_date.index(d) > mf._I2__period:dd['Date'].append(d)
        for i in ilist:
            for d in dd['Date']:
                hdr.append(float('%0.3f' % eval("mf.%s(date='%s')" % (i, d))))
            dd[i] = hdr
    return pandas.DataFrame(dd)
