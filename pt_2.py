from trial01 import I2

class Pen(I2):
    """
Pandas DataFrame object for local Futures. Require parameter: 'code'
    """
    def __init__(self, *args, **kwargs):
        if args: self.__code = args[0]
        elif 'code' in kwargs.keys(): self.__code = kwargs['code']
        self.mf = I2(code=self.__code)
        if 'period' in kwargs.keys(): self.mf = I2(code=self.__code, period=kwargs['period'])
        self.period = self.mf._I2__period

    def __del__(self):
        self.period = self.mf = self.__code = None
        del self.period
        del self.mf
        del self.__code

    def fdc(self, *args, **kwargs):
        """
Generate Pandas DataFrame object. Parameter: 'option', valid choice: 'B'asic (default), 'I'ndicators or 'O'verlays.
         """
        from utilities import dvs
        import pandas as pd
        import numpy as np
        def ma_order(*args, **kwargs):
            if args: date = args[0]
            if 'date' in kwargs.keys(): date = kwargs['date']
            hdr, ti = {}, self.fdc(option='I')
            res = ti[ti.Date == pd.Timestamp(date)]
            for i in ['EMA', 'WMA', 'SMA', 'KAMA']: hdr[i] = eval("res.%s.values[0]" % i)
            return dvs(hdr)

        option, dd = 'B', {}
        if args: option = args[0]
        elif 'option' in kwargs.keys(): option = kwargs['option']

        if option in ['B', 'b']:
            data, hdr = [], {}
            for i in self.mf.trade_day[self.period+1:]:
                hdr = self.mf._I2__rangefinder(field='date', value=i)['D']
                hdr['date'] = pd.Timestamp(i)
                data.append(hdr)
            for dk in ('date', 'open', 'high', 'low', 'close', 'delta', 'volume'):
                dd[dk.capitalize()] = [data[i][dk] for i in range(len(data))]
            dd['ATR'] = [self.mf.ATR(date=d) for d in self.mf.trade_day[self.period+1:]]
            dd['MAO'] = [ma_order(date=d) for d in self.mf.trade_day[self.period+1:]]
            t = pd.DataFrame(dd)
            return t.loc[:, ['Date', 'Delta', 'ATR', 'MAO', 'Open', 'High', 'Low', 'Close', 'Volume']]
        elif option in ['I', 'i']:
            r_date = self.mf.trade_day[self.period+1:]
            dd['Date'] = [pd.Timestamp(d) for d in r_date]
            dd['SMA'] = [self.mf.SMA(date=i) for i in r_date]
            dd['WMA'] = [self.mf.WMA(date=i) for i in r_date]
            dd['EMA'] = [self.mf.EMA(date=i) for i in r_date]
            dd['KAMA'] = [self.mf.KAMA(date=i) for i in r_date]
            dd['RSI'] = [self.mf.RSI(date=i) for i in r_date]
            t = pd.DataFrame(dd)
            return t.loc[:, ['Date', 'WMA', 'SMA', 'EMA', 'KAMA', 'RSI']]
        elif option in ['O', 'o']:
            r_date = self.mf.trade_day[self.period+1:]
            dd['Date'] = [pd.Timestamp(d) for d in r_date]
            dd['APZ'] = [self.mf.APZ(date=i) for i in r_date]
            dd['BB'] = [self.mf.BB(date=i) for i in r_date]
            dd['KC'] = [self.mf.KC(date=i) for i in r_date]
            t = pd.DataFrame(dd)
            return t.loc[:, ['Date', 'APZ', 'BB', 'KC']]

    def plot(self, **kwargs):
        """
Generate basic matplotlib graph object.
        """
        from utilities import get_month
        plt, candle = None, False
        try:
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
            import matplotlib.ticker as mticker
        except: pass

        try:
            from mpl_finance import candlestick_ohlc
            candle = True
        except: pass

        def axis_decorator(*args, **kwargs):
            angle, labels = 45, 8
            if args: ax1 = args[0]
            if 'axis' in kwargs.keys(): ax1 = kwargs['axis']
            if 'angle' in kwargs.keys(): angle = kwargs['angle']
            if 'labels' in kwargs.keys(): labels = kwargs['labels']
            for label in ax1.xaxis.get_ticklabels(): label.set_rotation(angle)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax1.xaxis.set_major_locator(mticker.MaxNLocator(labels))

        if plt:
            r_index, ta, ti, tb = 'close', self.fdc(option='O'), self.fdc(option='I'), self.fdc()
            plt.clf()
            plt.subplot(211)
            plt.plot(ti.Date, ti.SMA, label='SMA')
            plt.plot(ti.Date, ti.WMA, label='WMA')
            plt.plot(ti.Date, ti.EMA, label='EMA')
            plt.plot(ti.Date, ti.KAMA, label='KAMA')
            plt.plot(ta.Date, [x[0] for x in ta.KC.values], color='r', linestyle=':')
            plt.plot(ta.Date, [x[-1] for x in ta.KC.values], color='r', linestyle=':')
            plt.plot(ta.Date, [x[0] for x in ta.BB.values], color='c', linestyle='-.')
            plt.plot(ta.Date, [x[-1] for x in ta.BB.values], color='c', linestyle='-.')
            plt.plot(ta.Date, [x[0] for x in ta.APZ.values], color='m', linestyle='--')
            plt.plot(ta.Date, [x[-1] for x in ta.APZ.values], color='m', linestyle='--')
            plt.legend(loc='upper left', frameon=False)
            if candle:
                x, ohlc, r_index = 0, [], 'candlestick'
                while x < len(tb):
                    append_me = tb.Date[x].toordinal(), tb.Open[x], tb.High[x], tb.Low[x], tb.Close[x], tb.Volume[x]
                    ohlc.append(append_me)
                    x += 1
                candlestick_ohlc(plt.gca(), ohlc, width=0.4, colorup='#77d879', colordown='#db3f3f')
            else: plt.plot(tb.Date, tb.Close, color='b', marker='x', linestyle='', label='Close')
            plt.title('%s (%s): with various MA indicators and daily %s' % (self.__code[:-2].upper(), ' '.join((get_month(self.__code[-2]), '201' + self.__code[-1])), r_index))
            axis_decorator(axis=plt.gca(), labels=10)
            plt.grid(True)
            plt.subplot(212)
            plt.plot(ti.Date, ti.RSI, label='RSI')
            plt.legend(loc='lower left', frameon=False)
            axis_decorator(axis=plt.gca(), labels=7)
            plt.grid(True)
            plt.tight_layout()

    def xfinder(self, *args, **kwargs):
        from utilities import gr
        import pandas as pd
        result, option = [], 'F'
        if args: option = args[0]
        elif 'option' in kwargs.keys(): option = kwargs['option']

        def snl(*args):
            idx, ratio = args[0], gr
            if len(args) > 1: ratio = args[1]
            if idx in ['rsi', 'RSI']:
                t = self.fdc(option='I')
                result = [t.RSI.mean() + ratio * i for i in [-t.RSI.std(), t.RSI.std()]]
            elif idx in ['Delta', 'delta']:
                t = self.fdc(option='B')
                result = [t.Delta.mean() + ratio * i for i in [-t.Delta.std(), t.Delta.std()]]
            elif idx in ['ATR', 'atr']:
                tt = self.fdc("B")
                t = tt[self.period:]
                result = [t.ATR.mean() + ratio * i for i in [-t.ATR.std(), t.ATR.std()]]
            return result

        if option in ['F', 'D', 'f', 'd']:
            tb = self.fdc()
            amd, bmd = tb[tb.Delta > snl('Delta')[-1]], tb[tb.Delta < snl('Delta')[0]]
            xmd = pd.concat([amd, bmd])
            xmds = xmd.sort_values('Date')
            bsxmd = xmds.loc[:, ['Date', 'Delta', 'ATR', 'MAO', 'Open', 'High', 'Low', 'Close', 'Volume']]
            result.append(bsxmd)
        if option in ['F', 'A', 'f', 'a']:
            tb = self.fdc()
            rtb = tb[self.period+1:]
            amtr, bmtr = rtb[rtb.ATR > snl('ATR')[-1]], rtb[rtb.ATR < snl('ATR')[0]]
            xmtr = pd.concat([amtr, bmtr])
            xmtrs = xmtr.sort_values('Date')
            bsxmtr = xmtrs.loc[:, ['Date', 'Delta', 'ATR', 'MAO', 'Open', 'High', 'Low', 'Close', 'Volume']]
            result.append(bsxmtr)
        if option in ['F', 'R', 'f', 'r']:
            ti = self.fdc('I')
            amrs, bmrs = ti[ti.RSI > snl('RSI')[-1]], ti[ti.RSI < snl('RSI')[0]]
            xmrs = pd.concat([amrs, bmrs])
            xmrss = xmrs.sort_values('Date')
            bsxmrs = xmrss.loc[:, ['Date', 'WMA', 'SMA', 'EMA', 'KAMA', 'RSI']]
            result.append(bsxmrs)
        if len(result) == 1: return result[0]
        return result
