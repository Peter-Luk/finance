from trial01 import I2
import pandas
import numpy

def fdc(**args):
    mf = I2(code=args['code'])
    data, hdr, dd, r_date = [], {}, {}, mf.trade_day
    for i in r_date:
        hdr = mf._I2__rangefinder(field='date', value=i)['D']
        hdr['date'] = pandas.Timestamp(i)
        data.append(hdr)
    for dk in list(data[0].keys()): dd[dk.capitalize()] = [data[i][dk] for i in range(len(data))]
    ilist = ('SMA', 'WMA', 'EMA', 'KAMA', 'RSI')
    for i in ilist:
        hdr = []
        for j in range(len(r_date)):
            if j > mf._I2__period: hdr.append('%0.3f' % eval("mf.%s(date='%s')" % (i, r_date[j])))
            else: hdr.append(numpy.nan)
        dd[i] = hdr
    return pandas.DataFrame(dd)
