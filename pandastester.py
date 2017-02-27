from trial01 import I2
import pandas

def fdc(**args):
    try:
        ver = 'basic'
        mf = I2(code=args['code'])
        if 'version' in args.keys():ver = args['version']
        if ver == 'basic':
            data, hdr, dd, r_date = [], {}, {}, mf.trade_day
#            while i < len(mf._I2__data):
#                if mf._I2__data[i]['date'] not in r_date:r_date.append(mf._I2__data[i]['date'])
#                i += 1
            for i in r_date:
                hdr = mf._I2__rangefinder(field='date', value=i)['D']
                hdr['date'] = pandas.Timestamp(i)
                data.append(hdr)
            for dk in list(data[0].keys()):dd[dk.capitalize()] = [data[i][dk] for i in range(len(data))]
            return pandas.DataFrame(dd)
        elif ver == 'enhanced':
            data, ilist = {}, ('SMA', 'WMA', 'EMA', 'KAMA', 'RSI')
            for i in ilist:
                data[i] = [eval("mf.%s(date='%s')" % (i, mf.trade_day[j])) for j in range(mf._I2__period, len(mf.trade_day))]
            data['Date'] = [pandas.Timestamp(mf.trade_day[i]) for i in range(mf._I2__period, len(mf.trade_day))]
            return pandas.DataFrame(data)
    except:pass
