from trial01 import I2
import pandas as pd

class Pen:
    def __init__(self, **args):
        if 'code' in args.keys(): self.code = args['code']

    def __del__(self):
        self.code = None
        del(self.code)

    def fdc(self, **args):
        mf, option, dd = I2(code=self.code), 'B', {}
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
            dd['RSI'] = [mf.RSI(date=i) for i in r_date]
        return pd.DataFrame(dd)
