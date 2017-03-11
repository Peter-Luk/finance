from trial01 import I2
import pandas as pd

class Pen:
    def __init__(self, **args):
        self.plt = None
        try:
            import matplotlib.pyplot as plt
            self.plt = plt
        except:pass
        if 'code' in args.keys(): self.code = args['code']

    def __del__(self):
        self.code = self.plt = None
        del(self.code)
        del(self.plt)

    def fdc(self, **args):
        mf, option, dd = I2(code=self.code), 'B', {}
        if 'option' in args.keys():option = args['option']

        if option == 'B':
            data, hdr = [], {}
            for i in mf.trade_day:
                hdr = mf._I2__rangefinder(field='date', value=i)['D']
                hdr['date'] = pd.Timestamp(i)
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

    def plot(self, **args):
        if self.plt:
            ti, tb = self.fdc(option='I'), self.fdc()
            self.plt.clf()
            self.plt.subplot(211)
            self.plt.title('%s : with various MA indicators and daily close' % self.code.upper())
            self.plt.plot(ti.Date, ti.SMA, label='SMA')
            self.plt.plot(ti.Date, ti.WMA, label='WMA')
            self.plt.plot(ti.Date, ti.EMA, label='EMA')
            self.plt.plot(ti.Date, ti.KAMA, label='KAMA')
            self.plt.legend(loc='upper left', frameon=False)
            self.plt.plot(tb.Date, tb.Close, color='b', marker='x', linestyle='', label='Close')
            self.plt.xticks([tb.Date[i] for i in range(0, len(tb.Date), 7)], [r'$%s$' % tb.Date[i].strftime('%Y-%m-%d') for i in range(0, len(tb.Date), 7)])
            self.plt.grid(True)
            self.plt.subplot(212)
            self.plt.plot(ti.Date, ti.RSI, label='RSI')
            self.plt.xticks([ti.Date[i] for i in range(0, len(ti.Date), 4)], [r'$%s$' % ti.Date[i].strftime('%Y-%m-%d') for i in range(0, len(ti.Date), 4)])
            self.plt.legend(loc='lower left', frameon=False)
            self.plt.grid(True)
            self.plt.tight_layout()
            self.plt.show()
