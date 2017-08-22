"""
Local Futures (sqlite) analysis using pandas, matplotlib (visualize) via pyplot.
"""
him = getattr(__import__('handy'), 'him')
__ = him([{'utilities':('dvs', 'gr', 'get_month')}, ({'trial01':('I2',), 'pandas':(), 'sys':('version_info',), 'os':('linesep',)}, "alias={'I2':'I2', 'pandas':'pd'}")])
for _ in list(__.keys()): exec("%s=__['%s']" % (_, _))
if version_info.major == 2: __ = him([({'threading':('Thread',), 'Queue':('Queue',)}, "case='capitalize'")])
if version_info.major == 3: __ = him([({'threading':('Thread',), 'queue':('Queue',)}, "case='capitalize'")])
for _ in list(__.keys()): exec("%s=__['%s']" % (_, _))

class PI(I2):
    """
Base class to create Pandas DataFrame object for analyse local Futures data.
Require parameter: 'code'
    """
    def __init__(self, *args, **kwargs):
        if args:
            self.code = args[0]
            if len(args) >= 2: self.period = args[1]
        if 'code' in kwargs.keys(): self.code = kwargs['code']
        if 'period' in kwargs.keys(): self.period = kwargs['period']
        if (len(args) <= 2) or ('code' in kwargs.keys()):
            if (len(args) == 2) or ('period' in kwargs.keys()): super(PI, self).__init__(self.code, self.period)
            else: super(PI, self).__init__(self.code)
        self.__queue = Queue()
        self.__fdc = {}

    def __del__(self):
        self.code = self.period = self.__queue = self.__fdc = None
        del(self.period)
        del(self.code)
        del(self.__queue)
        del(self.__fdc)

    def fdc(self, *args, **kwargs):
        """
Create Pandas DataFrame object required parameter: 'option'.
Valid choice: 'B'asic (default), 'I'ndicators or 'O'verlays.
        """
        # from utilities import dvs
        # import pandas as pd
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
            if 'b' in list(self.__fdc.keys()):
                # self.__queue.put(self.__fdc['b'])
                return self.__fdc['b']
            else:
                data, hdr = [], {}
                for i in self.trade_day[self.period+1:]:
                    hdr = self._I2__rangefinder(field='date', value=i)['D']
                    hdr['date'] = pd.Timestamp(i)
                    data.append(hdr)
                for dk in ('date', 'open', 'high', 'low', 'close', 'delta', 'volume'):
                    dd[dk.capitalize()] = [data[i][dk] for i in range(len(data))]
                dd['ATR'] = [self.ATR(date=d) for d in self.trade_day[self.period+1:]]
                dd['MAO'] = [ma_order(date=d) for d in self.trade_day[self.period+1:]]
                res = pd.DataFrame(dd).loc[:, ['Date', 'Delta', 'ATR', 'MAO', 'Open', 'High', 'Low', 'Close', 'Volume']]
                self.__queue.put(res)
                return res
        elif option in ['I', 'i']:
            if 'i' in list(self.__fdc.keys()):
                # self.__queue.put(self.__fdc['i'])
                return self.__fdc['i']
            else:
                r_date = self.trade_day[self.period+1:]
                dd['Date'] = [pd.Timestamp(d) for d in r_date]
                dd['SMA'] = [self.SMA(date=i) for i in r_date]
                dd['WMA'] = [self.WMA(date=i) for i in r_date]
                dd['EMA'] = [self.EMA(date=i) for i in r_date]
                dd['KAMA'] = [self.KAMA(date=i) for i in r_date]
                dd['RSI'] = [self.RSI(date=i) for i in r_date]
                res = pd.DataFrame(dd).loc[:, ['Date', 'WMA', 'SMA', 'EMA', 'KAMA', 'RSI']]
                self.__queue.put(res)
                return res
        elif option in ['O', 'o']:
            if 'o' in list(self.__fdc.keys()):
                # self.__queue.put(self.__fdc['o'])
                return self.__fdc['o']
            else:
                r_date = self.trade_day[self.period+1:]
                dd['Date'] = [pd.Timestamp(d) for d in r_date]
                dd['APZ'] = [self.APZ(date=i) for i in r_date]
                dd['BB'] = [self.BB(date=i) for i in r_date]
                dd['KC'] = [self.KC(date=i) for i in r_date]
                res = pd.DataFrame(dd).loc[:, ['Date', 'APZ', 'BB', 'KC']]
                self.__queue.put(res)
                return res

    def ltdmos(self, *args, **kwargs):
        option, result = 'A', []
        if args: option = args[0]
        elif 'option' in kwargs.keys(): option = kwargs['option']
        reqo = ['i', 'o']
        for _ in reqo:
            if _ not in list(self.__fdc.keys()):
                p = Thread(target=self.fdc, name=_, args=(_,))
                p.start()
                self.__fdc[_] = self.__queue.get()
                p.join()

        if option in ['M', 'm', 'A', 'a']:
            itemp = self.__fdc['i']
            result.append(int(round(itemp.KAMA.values[-1])))
            result.append(int(round(itemp.EMA.values[-1])))
            result.append(int(round(itemp.SMA.values[-1])))
            result.append(int(round(itemp.WMA.values[-1])))
#            result.extend([int(round(eval('itemp.%s.values[%i]' % (k.upper(), -1)), 0)) for k in ['kama', 'ema', 'sma', 'wma']])
        if option in ['O', 'o', 'A', 'a']:
            otemp = self.__fdc['o']
            result.extend(list(otemp.KC.values[-1]))
            result.extend(list(otemp.APZ.values[-1]))
            result.extend(list(otemp.BB.values[-1]))
#            [result.extend(list(eval('otemp.%s.values[%i]' % (k.upper(), -1)))) for k in ['kc', 'apz', 'bb']]
        result.sort()
        self.__queue.put(result)
        return result

    def plot(self, **kwargs):
        """
Create basic matplotlib graph object (develpoing...)
        """
        # from utilities import get_month
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
            """
Internal decorative function required parameter: 'ax1'.
Both others 'labels' and 'angle' variables are optional. Default 8 and 45 respectively.
            """
            angle, labels = 45, 8
            if args:
                ax1 = args[0]
                if len(args) >= 3: angle = args[2]
                if len(args) >= 2: labels = args[1]
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
            plt.plot(ta.Date, [x[0] for x in ta.APZ.values], color='g', linestyle='--')
            plt.plot(ta.Date, [x[-1] for x in ta.APZ.values], color='g', linestyle='--')
            plt.legend(loc='upper left', frameon=False)
            if candle:
                x, ohlc, r_index = 0, [], 'candlestick'
                while x < len(tb):
                    append_me = tb.Date[x].toordinal(), tb.Open[x], tb.High[x], tb.Low[x], tb.Close[x], tb.Volume[x]
                    ohlc.append(append_me)
                    x += 1
                candlestick_ohlc(plt.gca(), ohlc, width=0.4, colorup='#77d879', colordown='#db3f3f')
            else: plt.plot(tb.Date, tb.Close, color='b', marker='x', linestyle='', label='Close')
            plt.title('%s (%s): with various MA indicators and daily %s' % (self.code[:-2].upper(), ' '.join((get_month(self.code[-2]), '201' + self.code[-1])), r_index))
            axis_decorator(plt.gca(), 9, 30)
            plt.grid(True)
            plt.subplot(212)
            plt.plot(ti.Date, ti.RSI, label='RSI')
            plt.legend(loc='lower left', frameon=False)
            axis_decorator(plt.gca(), 9, 30)
            plt.grid(True)
            plt.tight_layout()

    def xfinder(self, *args, **kwargs):
        """
Extreme finder for indicator(s), required parameter: 'option'. Valid choice: (A)TR, (D)elta, (F)ull (default) or (R)SI.
        """
        # from utilities import gr
        # import pandas as pd
        result, option = [], 'F'
        if args: option = args[0]
        elif 'option' in kwargs.keys(): option = kwargs['option']

        reqo = ['i', 'b']
        for _ in reqo:
            if _ not in list(self.__fdc.keys()):
                p = Thread(target=self.fdc, name=_, args=(_,))
                p.start()
                self.__fdc[_] = self.__queue.get()
                p.join()

        def snl(*args):
            idx, ratio = args[0], gr
            if len(args) > 1: ratio = args[1]
            if idx in ['rsi', 'RSI']:
                t = self.__fdc['i']
                result = [t.RSI.mean() + ratio * i for i in [-t.RSI.std(), t.RSI.std()]]
            elif idx in ['Delta', 'delta']:
                t = self.__fdc['b']
                result = [t.Delta.mean() + ratio * i for i in [-t.Delta.std(), t.Delta.std()]]
            elif idx in ['ATR', 'atr']:
                # tt = self.__fdc['b']
                t = self.__fdc['b'][self.period:]
                result = [t.ATR.mean() + ratio * i for i in [-t.ATR.std(), t.ATR.std()]]
            return result

        if option in ['F', 'D', 'f', 'd']:
            tb = self.__fdc['b']
            amd, bmd = tb[tb.Delta > snl('Delta')[-1]], tb[tb.Delta < snl('Delta')[0]]
            xmd = pd.concat([amd, bmd])
            xmds = xmd.sort_values('Date')
            bsxmd = xmds.loc[:, ['Date', 'Delta', 'ATR', 'MAO', 'Open', 'High', 'Low', 'Close', 'Volume']]
            result.append(bsxmd)
        if option in ['F', 'A', 'f', 'a']:
            tb = self.__fdc['b']
            rtb = tb[self.period+1:]
            amtr, bmtr = rtb[rtb.ATR > snl('ATR')[-1]], rtb[rtb.ATR < snl('ATR')[0]]
            xmtr = pd.concat([amtr, bmtr])
            xmtrs = xmtr.sort_values('Date')
            bsxmtr = xmtrs.loc[:, ['Date', 'Delta', 'ATR', 'MAO', 'Open', 'High', 'Low', 'Close', 'Volume']]
            result.append(bsxmtr)
        if option in ['F', 'R', 'f', 'r']:
            ti = self.__fdc['i']
            amrs, bmrs = ti[ti.RSI > snl('RSI')[-1]], ti[ti.RSI < snl('RSI')[0]]
            xmrs = pd.concat([amrs, bmrs])
            xmrss = xmrs.sort_values('Date')
            bsxmrs = xmrss.loc[:, ['Date', 'WMA', 'SMA', 'EMA', 'KAMA', 'RSI']]
            result.append(bsxmrs)
        if len(result) == 1: return result[0]
        self.__queue.put(result)
        return result

    def ds(self, *args, **kwargs):
        """
        Accept sole boolean argument `programmatic` only,
        If True return result in dict type,others in plain text.
        """
        programmatic = True
        if 'programmatic' in list(kwargs.keys()): programmatic = kwargs['programmatic']
        res = {'Code': self.code.upper(), 'Latest': self.trade_day[-1]}
        rest = '%s: (latest @ %s)' % (self.code.upper(), self.trade_day[-1])
        try:
            mos = self.ltdmos('a')
            ar = self.fdc('b')
            dm, ds = ar['Delta'].mean(), ar['Delta'].std()
            lv, vm, vs = ar['Volume'].values[-1], ar['Volume'].mean(), ar['Volume'].std()
            lc, cs = ar['Close'].values[-1], ar['Close'].std()
            res['Delta'] = {'mean': dm, 'std': ds}
            res['Volume'] = {'last': lv, 'mean': vm, 'std': vs}
            res['Close'] = {'last': lc, 'std': cs}
            rest += '%sClose: %i%s%sVolume over (mean = %.2f): %.2f %%%sVolume over (mean + std = %.2f): %.2f %%' % (linesep, lc, linesep, linesep, vm, lv / vm * 100., linesep, vm + vs, lv / (vm + vs) * 100.)
            il = list(filter(lambda _:(_ > lc - cs) and (_ < lc + cs), mos))
            ol = list(filter(lambda _:(_ < lc - cs) or (_ > lc + cs), mos))
            res['Range'] = il
            rest += '%s%sWithin statistical range: %s' % (linesep, linesep, il)
            ml = list(filter(lambda _:_ > lc, ol))
            csl = list(filter(lambda _:_ not in ml, ol))
            if ml:
                res['Moon'] = ml
                rest += '%s%sMoon shot: %s' % (linesep, linesep, ml)
            if csl:
                res['China'] = csl
                rest += '%sChina syndrome: %s' % (linesep, csl)
            xd = self.xfinder('d')
            dtxd = xd.transpose().to_dict()
            if len(dtxd.keys()):
                tl = []
                rest += '%s%sDelta extreme case (< %i or > %i)' % (linesep, linesep, dm - ds, dm + ds)
                for _ in list(dtxd.keys()):
                    tl.append({dtxd[_]['Date'].strftime('%d-%m-%Y'): dtxd[_]['Delta']})
                    rest += '%s%s: %i' % (linesep, dtxd[_]['Date'].strftime('%d-%m-%Y'), dtxd[_]['Delta'])
                res['Delta']['extreme'] = tl
        except:pass
        try:
            ai = self.fdc('i')
            xr = self.xfinder('r')
            rm, rs = ai['RSI'].mean(), ai['RSI'].std()
            dtxr = xr.transpose().to_dict()
            res['RSI'] = {'mean': rm, 'std': rs}
            if len(dtxr.keys()):
                tl = []
                rest += '%sRSI extreme case (< %.3f or > %.3f)' % (linesep, rm - rs, rm + rs)
                for _ in list(dtxr.keys()):
                    tl.append({dtxr[_]['Date'].strftime('%d-%m-%Y'):dtxr[_]['RSI']})
                    rest += '%s%s: %.3f' % (linesep, dtxr[_]['Date'].strftime('%d-%m-%Y'), dtxr[_]['RSI'])
                res['RSI']['extreme'] = tl
        except:pass
        if programmatic == True: return res
        return rest
