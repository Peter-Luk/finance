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

    def board(self, **args):
        import matplotlib.pyplot as plt
        ti, tb = self.fdc(option='I'), self.fdc()
        plt.subplot(211)
        plt.title('%s : with various MA indicators and daily close' % self.code.upper())
        plt.plot(ti.Date, ti.SMA, label='SMA')
        plt.plot(ti.Date, ti.WMA, label='WMA')
        plt.plot(ti.Date, ti.EMA, label='EMA')
        plt.plot(ti.Date, ti.KAMA, label='KAMA')
        plt.legend(loc='upper left', frameon=False)
        plt.plot(tb.Date, tb.Close, color='b', marker='x', linestyle='', label='Close')
        plt.xticks([tb.Date[i] for i in range(0, len(tb.Date), 7)], [r'$%s$' % tb.Date[i] for i in range(0, len(tb.Date), 7)])
        plt.grid(True)
        plt.subplot(212)
        plt.plot(ti.Date, ti.RSI, label='RSI')
        plt.xticks([ti.Date[i] for i in range(0, len(ti.Date), 4)], [r'$%s$' % ti.Date[i].strftime('%Y-%m-%d') for i in range(0, len(ti.Date), 4)])
        plt.legend(loc='lower left', frameon=False)
        plt.grid(True)
