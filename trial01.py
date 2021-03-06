"""
Powerhouse for techinal analysis between frontend like pandas, matplotlib
and backend namely, sqlite3.
"""
db_name, db_table = 'Futures', 'records'
him = getattr(__import__('handy'), 'him')
iml = [
        {'utilities': ('gr', 'filepath'),
            'datetime': ('datetime',),
            'statistics': ('mean', 'stdev'),
            'os': ('sep', 'linesep')},
        (
            {'sqlite3': ()},
            "alias= 'lite'"),
        (
            {'tags': ('HTML', 'TITLE', 'TABLE', 'TH', 'TR', 'TD')},
            "case='upper'")]
__ = him(iml)
for _ in __.keys():
    exec(f"{_} = __['{_}']")


class I2(object):
    """
Base class to provide techinal analysis for financial derivatives.
Required 'product code'.
    """
    def __init__(self, *args, **kwargs):
        self.datetime, self.trade_day = datetime, []
        self.period, self.__db, self.__table = int(round(20 / gr, 0)), db_name, db_table
        if args:
            self.code = args[0]
            if len(args) >= 4:
                self.__table = args[3]
            if len(args) >= 3:
                self.__db = args[2]
            if len(args) >= 2:
                self.period = args[1]
        if 'code' in kwargs.keys():
            self.code = kwargs['code']
        if 'period' in kwargs.keys():
            self.period = kwargs['period']
        if 'db_name' in kwargs.keys():
            self.__db = kwargs['db_name']
        if 'db_table' in kwargs.keys():
            self.__table = kwargs['db_table']

        self.__conn = lite.connect(filepath(self.__db))
        self.__conn.row_factory = lite.Row
        self.__data = self.__conn.cursor().execute("SELECT * FROM %s WHERE code='%s' ORDER BY date ASC, session DESC" % (self.__table, self.code.upper())).fetchall()
        for i in range(len(self.__data)):
            if self.__data[i]['date'] not in self.trade_day:
                self.trade_day.append(self.__data[i]['date'])

    def __del__(self):
        self.__data = self.__conn = self.code = self.period = self.__db = self.__table = self.datetime = self.trade_day = None
        del self.__data
        del self.__conn
        del self.code
        del self.period
        del self.__db
        del self.__table
        del self.datetime
        del self.trade_day

    def __rangefinder(self, *args, **kwargs):
        """
Accept 'two' and 'only two' variables (i.e. field and value)
        """
        if args: field, value = args[0], args[1]
        if 'field' in kwargs.keys(): field = kwargs['field']
        if 'value' in kwargs.keys(): value = kwargs['value']
        res, hdr ={}, []
        for i in self.__data:
            if value == i[field]: hdr.append(i)
        if len(hdr) > 1:
            for i in hdr:
                if i['session'] == 'A':
                    dc, so, sc, sr = i['close'], i['open'], i['close'], i['high'] - i['low']
                    if sh:
                        dh, dl, dv = sh, sl, sv + i['volume']
                        if i['high'] > dh:dh = i['high']
                        if i['low'] < dl:dl = i['low']
                else:
                    do, sh, sl, sv = i['open'], i['high'], i['low'], i['volume']
                    sr = sh - sl
            dr = dh - dl
            res['A'] = {'delta':sr, 'open':so, 'close':sc, 'volume':sv, 'high':sh, 'low':sl}
        else:
            so, sc, sh, sl, sv = hdr[0]['open'], hdr[0]['close'], hdr[0]['high'], hdr[0]['low'], hdr[0]['volume']
            do, dc, sr, dv = so, sc, sh -sl, sv
            dr, dh, dl = sr, sh, sl
            res['M'] = {'delta':sr, 'open':so, 'close':sc, 'volume':sv, 'high':sh, 'low':sl}
        res['D'] = {'delta':dr, 'open':do, 'close':dc, 'volume':dv, 'high':dh, 'low':dl}
        return res

    def append(self, *args, **kwargs):
        date = self.datetime.today().strftime('%Y-%m-%d')
        if args:
            date = args[0]
            if len(args) == 2: volume = int(args[1])
        if 'date' not in kwargs.keys(): kwargs['date'] = date
        if 'volume' in kwargs.keys(): volume = int(kwargs['volume'])
        kt, vt = ('date', 'session', 'code'), (kwargs['date'], kwargs['session'], self.code.upper())
        for k, v in kwargs.items():
            if k not in ['date', 'session', 'code', 'volume']:
                kt += (k,)
                vt += (int(v),)
        if kwargs['session'] == 'A':
            hdr = self.__rangefinder(field='date', value=date)
            if 'M' in hdr.keys(): volume -= hdr['M']['volume']
        sq_str = "INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s) VALUES ('%s', '%s', '%s', %i, %i, %i, %i, %i)" % ((self.__table,) + kt + ('volume',) + vt + (volume,))
        try:
            self.__conn.cursor().execute(sq_str)
            self.__conn.commit()
        except: self.__conn.rollback()

    def ATR(self, *args, **kwargs):
        date, period = self.datetime.today().strftime('%Y-%m-%d'), self.period
        if args:
            date = args[0]
            if len(args) < 3:
                period = args[1]
        if 'date' in kwargs.keys():
            date = kwargs['date']
        if 'period' in kwargs.keys():
            period = kwargs['period']
        res, r_date, tr, i, hdr = {}, self.trade_day, [], 0, {}

        tr.append([
            self.__data[0]['date'],
            self.__data[0]['session'],
            self.__data[0]['high'] - self.__data[0]['low']])
        i += 1
        while i < len(self.__data):
            if self.__data[i]['date'] == self.__data[i - 1]['date']:
                if self.__data[i]['high'] > self.__data[i - 1]['high']:
                    ma = self.__data[i]['high']
                else:
                    ma = self.__data[i - 1]['high']
                if self.__data[i]['low'] < self.__data[i - 1]['low']:
                    mi = self.__data[i]['low']
                else:
                    mi = self.__data[i - 1]['low']
            else:
                if self.__data[i]['high'] > self.__data[i - 1]['close']:
                    ma = self.__data[i]['high']
                    mi = self.__data[i - 1]['close']
                elif self.__data[i]['low'] < self.__data[i - 1]['close']:
                    mi, ma = self.__data[i]['low'], self.__data[i - 1]['close']
            tr.append([
                self.__data[i]['date'],
                self.__data[i]['session'],
                ma - mi])
            i += 1

        for i in range(len(tr)):
            hdr[tr[i][0]] = tr[i][-1]

        res[r_date[period - 1]] = mean([hdr[x] for x in r_date[:period]])
        for d in r_date[period:]:
            res[d] = (res[
                r_date[r_date.index(d) - 1]] * (period - 1) + hdr[d]) / period

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:
            return res[date]
        return res[rkeys[-1]]

    def EMA(self, *args, **kwargs):
        data, date, period, option = self.__data, self.datetime.today().strftime('%Y-%m-%d'), self.period, 'C'
        if args:
            date = args[0]
            if len(args) == 2:
                period = args[1]
            if len(args) == 3:
                period, data = args[1:3]
            if len(args) == 4:
                period, data, option = args[1:]
        if 'data' in kwargs.keys():
            data = kwargs['data']
        if 'date' in kwargs.keys():
            date = kwargs['date']
        if 'period' in kwargs.keys():
            period = kwargs['period']
        if 'option' in kwargs.keys():
            option = kwargs['option']
        if type(data[0]) in (int, float):
            res, kratio = [], 2. / (1 + period)
            for i in range(len(data)):
                if i == 0:
                    res.append(data[i])
                else:
                    res.append(data[i] * kratio + res[-1] * (1. - kratio))
            return res[-1]
        else:
            res, r_date, hdr = {}, self.trade_day, {}

            for d in r_date:
                tmp = self.__rangefinder(field='date', value=d)['D']
                if option.upper() == 'C':
                    hdr[d] = tmp['close']
                elif option.upper() == 'H':
                    hdr[d] = tmp['high']
                elif option.upper() == 'L':
                    hdr[d] = tmp['low']
                elif option.upper() == 'HL':
                    hdr[d] = mean([tmp['high'], tmp['low']])
                elif option.upper() == 'F':
                    hdr[d] = mean([
                        tmp['open'], tmp['close'], tmp['high'], tmp['low']])

            res[r_date[period - 1]] = mean([hdr[x] for x in r_date[:period]])
            for d in r_date[period:]:
                res[d] = (res[
                    r_date[r_date.index(d) - 1]] * (period - 1) + hdr[d]
                    ) / period

            rkeys = list(res.keys())
            rkeys.sort()
            if date in rkeys:
                return res[date]
            return res[rkeys[-1]]

    def KAMA(self, *args, **kwargs):
        date, period = self.datetime.today().strftime('%Y-%m-%d'), self.period
        fast, slow = 2, period
        if args:
            largs, date = len(args), args[0]
            if largs == 2:
                period = args[1]
            if largs == 4:
                period, fast, slow = args[1:]
        if 'date' in kwargs.keys():
            date = kwargs['date']
        if 'period' in kwargs.keys():
            period = kwargs['period']
        if 'slow' in kwargs.keys():
            slow = kwargs['slow']
        if 'fast' in kwargs.keys():
            fast = kwargs['fast']
        res, trade_day, tr, hdr, er, sc = {}, self.trade_day, {}, {}, {}, {}

        for d in trade_day:
            tmp = self.__rangefinder(field='date', value=d)['D']
            tr[d] = tmp['close']

        i = period
        while i < len(trade_day):
            ch = abs(tr[trade_day[i]] - tr[trade_day[i - period]])
            vo = sum([
                abs(tr[trade_day[x]] - tr[trade_day[x - 1]])
                for x in range(i - period, i)])
            er[trade_day[i]] = ch / float(vo)
            i += 1

        i = slow
        while i < len(trade_day):
            sfc, ssc = 2. / (fast + 1), 2. / (slow + 1)
            sc[trade_day[i]] = (er[trade_day[i]] * (sfc - ssc) + ssc) ** 2
            i += 1

        i = slow
        while i < len(trade_day):
            if i == slow:
                res[trade_day[i]] = self.EMA(date=trade_day[i])
            else:
                res[trade_day[i]] = res[trade_day[i - 1]] + sc[trade_day[i]] * (tr[trade_day[i]] - res[trade_day[i - 1]])
            i += 1

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:
            return res[date]
        return res[rkeys[-1]]

    def RSI(self, *args, **kwargs):
        date, period = self.datetime.today().strftime('%Y-%m-%d'), self.period
        if args:
            date = args[0]
            if len(args) < 3: period = args[1]
        if 'date' in kwargs.keys(): date = kwargs['date']
        if 'period' in kwargs.keys(): period = kwargs['period']

        def __gt0(x):
            if x > 0:return True
            return False

        def __lt0(x):
            if x < 0:return True
            return False

        res, trade_day, tr, hdr, i = {}, self.trade_day, {}, {}, 1
        for d in trade_day:
            tmp = self.__rangefinder(field='date', value=d)['D']
            tr[d] = tmp['close']

        while i < len(trade_day):
            hdr[trade_day[i]] = tr[trade_day[i]] - tr[trade_day[i - 1]]
            i += 1

        i, ag, al = period, {}, {}
        trade_day = list(hdr.keys())
        trade_day.sort()
        while i < len(trade_day):
        # for i in range(period, len(trade_day)):
            if i == period:
                ag[trade_day[i]], al[trade_day[i]] = sum([hdr[trade_day[j]] for j in range(i - period, i) if __gt0(hdr[trade_day[j]])]) / float(period), abs(sum([hdr[trade_day[j]] for j in range(i - period, i) if __lt0(hdr[trade_day[j]])])) / float(period)
                # ag[trade_day[i]], al[trade_day[i]] = sum(list(filter(__gt0, [hdr[trade_day[j]] for j in range(i - period, i)]))) / float(period), abs(sum(list(filter(__lt0, [hdr[trade_day[j]] for j in range(i - period, i)])))) / float(period)
            else:
                if hdr[trade_day[i]] > 0:
                    ag[trade_day[i]] = (ag[trade_day[i - 1]] * (period - 1) + hdr[trade_day[i]]) / period
                    al[trade_day[i]] = al[trade_day[i - 1]] * (period - 1) / period
                else:
#                if hdr[trade_day[i]] < 0:
                    ag[trade_day[i]] = ag[trade_day[i - 1]] * (period - 1) / period
                    al[trade_day[i]] = (al[trade_day[i - 1]] * (period - 1) + abs(hdr[trade_day[i]])) / period
            res[trade_day[i]] = 100 - 100 / ( 1 + float(ag[trade_day[i]]) / al[trade_day[i]])
            i += 1

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys: return res[date]
        return res[rkeys[-1]]

    def BBW(self, *args, **kwargs):
        date, period = self.datetime.today().strftime('%Y-%m-%d'), self.period
        if args:
            date = args[0]
            if len(args) < 3:
                period = args[1]
        if 'date' in kwargs.keys():
            date = kwargs['date']
        if 'period' in kwargs.keys():
            period = kwargs['period']
        res, r_date, hdr = {}, self.trade_day, {}

        for d in r_date:
            tmp = self.__rangefinder(field='date', value=d)['D']
            hdr[d] = tmp['delta']

        for i in range(len(r_date) - period):
            res[r_date[period + i]] = 2 * 2 * stdev([
                hdr[r_date[x]] for x in range(i, period + i)]) / gr

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:
            return res[date]
        return res[rkeys[-1]]

    def SAR(self, **kwargs):
        date = self.datetime.today().strftime('%Y-%m-%d')
        period, option = self.period, 'C'
        if 'date' in kwargs.keys():
            date = kwargs['date']
        if 'period' in kwargs.keys():
            period = kwargs['period']
        if 'option' in kwargs.keys():
            option = kwargs['option']
        res, r_date, i, hdr = {}, [], 0, {}
        sp = {}
        while i < len(self.__data) - 1:
            if self.__data[i]['date'] not in r_date:
                r_date.append(self.__data[i]['date'])
#            if option.upper() == 'C':hdr[self.__data[i]['date']] = self.__data[i]['close']
#            if option.upper() == 'HL':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low']])
#            if option.upper() == 'F':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low'],self.__data[i]['open'], self.__data[i]['close']])
            i += 1

        for d in range(1, len(r_date)):
            if d == 1:
                tmp = self.__rangefinder(field='date', value=r_date[d - 1])
                sp[r_date[d]] = tmp['D']['low']
                if tmp['D']['close'] < tmp['D']['open']:
                    sp[d] = tmp['D']['high']
            # else:
            #     pass
        return sp
#        for i in range(len(r_date) - period):
#            res[r_date[period + i]] = mean([hdr[r_date[x]] for x in range(i, period + i)])

#        rkeys = list(res.keys())
#        rkeys.sort()
#        if date in rkeys:return res[date]
#        return res[rkeys[-1]]
#
    def SMA(self, *args, **kwargs):
        date = self.datetime.today().strftime('%Y-%m-%d')
        period, option = self.period, 'C'
        if args:
            date = args[0]
            if len(args) == 2:
                period = args[1]
            if len(args) == 3:
                period, option = args[1:]
        if 'date' in kwargs.keys():
            date = kwargs['date']
        if 'period' in kwargs.keys():
            period = kwargs['period']
        if 'option' in kwargs.keys():
            option = kwargs['option']
        res, r_date, hdr = {}, self.trade_day, {}

        for d in r_date:
            tmp = self.__rangefinder(field='date', value=d)['D']
            if option.upper() == 'C':
                hdr[d] = tmp['close']
            elif option.upper() == 'HL':
                hdr[d] = mean([tmp['high'], tmp['low']])
            elif option.upper() == 'F':
                hdr[d] = mean(
                        [tmp['open'], tmp['close'], tmp['high'], tmp['low']])

        for i in range(len(r_date) - period):
            res[r_date[period + i]] = mean(
                    [hdr[r_date[x]] for x in range(i, period + i)])

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys: return res[date]
        return res[rkeys[-1]]

    def APZ(self, *args, **kwargs):
        date, period = self.datetime.today().strftime('%Y-%m-%d'), self.period
        if args:
            date = args[0]
            if len(args) < 3:
                period = args[1]
        if 'date' in kwargs.keys():
            date = kwargs['date']
        if 'period' in kwargs.keys():
            period = kwargs['period']
        res, r_date = {}, self.trade_day

        for i in range(period, len(r_date)):
            eh = [self.EMA(
                date=r_date[i - j],
                period=period,
                option='H') for j in range(period)]
            el = [
                    self.EMA(date=r_date[i - j], period=period, option='L')
                    for j in range(period)]
            vv = self.EMA(
                    data=[eh[j] for j in range(len(eh))],
                    period=len(eh)) - self.EMA(
                            data=[el[j] for j in range(
                                len(el))],
                            period=len(el))
            ema = self.EMA(date=r_date[i], option='HL', period=period)
            res[r_date[i]] = int(round(ema - vv * gr, 0)),
            int(round(ema + vv * gr, 0))
#            kama = self.KAMA(date=r_date[i], option='HL', period=period)
#            res[r_date[i]] = int(round(kama - vv * gr, 0)), int(round(kama + vv * gr, 0))

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:
            return res[date]
        return res[rkeys[-1]]

    def BB(self, *args, **kwargs):
        date, period, option = self.datetime.today().strftime('%Y-%m-%d'), self.period, 'C'
        if args:
            date = args[0]
            if len(args) == 2: period = args[1]
            if len(args) == 3: period, option = args[1:]
        if 'date' in kwargs.keys(): date = kwargs['date']
        if 'period' in kwargs.keys(): period = kwargs['period']
        if 'option' in kwargs.keys(): option = kwargs['option']
        res, r_date = {}, self.trade_day

        for i in range(len(r_date) - period):
#             res[r_date[period + i]] = tuple([int(round(self.SMA(date=r_date[period + i], period=period, option=option) + x * gr / 2, 0)) for x in [-self.BBW(date=r_date[period + i], period=period), self.BBW(date=r_date[period + i], period=period)]])
#             res[r_date[period + i]] = tuple([int(round(self.SMA(date=r_date[period + i], period=period, option=option) + x * gr, 0)) for x in [-self.BBW(date=r_date[period + i], period=period), self.BBW(date=r_date[period + i], period=period)]])
            res[r_date[period + i]] = tuple([int(round(self.SMA(date=r_date[period + i], period=period, option=option) + x * 2., 0)) for x in [-self.BBW(date=r_date[period + i], period=period), self.BBW(date=r_date[period + i], period=period)]])

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys: return res[date]
        return res[rkeys[-1]]

    def WMA(self, *args, **kwargs):
        date, period, option = self.datetime.today().strftime('%Y-%m-%d'), self.period, 'C'
        if args:
            date = args[0]
            if len(args) == 2: period = args[1]
            if len(args) == 3: period, option = args[1:]
        if 'date' in kwargs.keys(): date = kwargs['date']
        if 'period' in kwargs.keys(): period = kwargs['period']
        if 'option' in kwargs.keys(): option = kwargs['option']
        res, r_date, hdr = {}, self.trade_day, {}

        for d in r_date:
            tmp = self.__rangefinder(field='date', value=d)['D']
            if option.upper() == 'C': hdr[d] = (tmp['close'], tmp['volume'])
            elif option.upper() == 'HL': hdr[d] = (mean([tmp['high'], tmp['low']]), tmp['volume'])
            elif option.upper() == 'F': hdr[d] = (mean([tmp['open'], tmp['close'], tmp['high'], tmp['low']]), tmp['volume'])

        for i in range(len(r_date) - period):
            res[r_date[period + i]] = sum([hdr[r_date[x]][0] * hdr[r_date[x]][-1] for x in range(i, period + i)]) / float(sum([hdr[r_date[x]][-1] for x in range(i, period + i)]))

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys: return res[date]
        return res[rkeys[-1]]

    def KC(self, *args, **kwargs):
        date = self.datetime.today().strftime('%Y-%m-%d')
        period, option = self.period, 'C'
        if args:
            date = args[0]
            if len(args) == 2:
                period = args[1]
            if len(args) == 3:
                period, option = args[1:]
        if 'date' in kwargs.keys():
            date = kwargs['date']
        if 'period' in kwargs.keys():
            period = kwargs['period']
        if 'option' in kwargs.keys():
            option = kwargs['option']
        width = self.ATR(date=date, period=period)
        base = self.KAMA(date=date, period=period, option=option)
        return (int(round(base - width * gr, 0)),
                int(round(base + width * gr, 0)))

    def daatr(self, *args, **kwargs):
        date, period = None, 5
        if args:
            date = args[0]
            if len(args) >= 2:
                period = args[1]
        if 'date' in kwargs.keys():
            date = kwargs['date']
        if 'period' in kwargs.keys():
            period = kwargs['period']
        if date:
            return self.ATR(date=date, period=period) - self.ATR(date=date)
        else:
            return self.ATR(period=period) - self.ATR()

    def estimate(self, *args, **kwargs):
        if args:
            pivot_point = args[0]
            if len(args) >= 5:
                concise = args[4]
            if len(args) >= 4:
                o_format = args[3]
            if len(args) >= 3:
                programmatic = args[2]
            if len(args) >= 2:
                t_date = args[1]
        if 'pivot_point' in kwargs.keys():
            pivot_point = int(kwargs['pivot_point'])
        if not pivot_point:
            return "Essential value 'pivot _point' is obmitted"
        t_date, programmatic = self.trade_day[-1], False
        o_format, concise = 'raw', False
        if 'date' in kwargs.keys():
            t_date = kwargs['date']
        if 'programmatic' in kwargs.keys():
            programmatic = kwargs['programmatic']
        if 'format' in kwargs.keys():
            o_format = kwargs['format'].lower()
        if 'concise' in kwargs.keys():
            concise = kwargs['concise']

        hdr = self.__rangefinder(field='date', value=t_date)
        dr, gap = hdr['D']['delta'], abs(pivot_point - hdr['D']['close'])
        if 'A' in hdr.keys():
            sr = hdr['A']['delta']
        elif 'M' in hdr.keys():
            sr = hdr['M']['delta']
            i_date = self.trade_day[self.trade_day.index(t_date) - 1]
            hdr2 = self.__rangefinder(field='date', value=i_date)
            dr = hdr2['D']['delta']

        ogr = 1. / gr
        sru = tuple([
            int(round(float(pivot_point)+x, 0))
            for x in [(1-ogr)*sr, ogr*sr]])
        srl = tuple([
            int(round(float(pivot_point)-x, 0))
            for x in [(1-ogr)*sr, ogr*sr]])
        dru = tuple([
            int(round(float(pivot_point)+x, 0))
            for x in [(1-ogr)*dr, ogr*dr]])
        drl = tuple([
            int(round(float(pivot_point)-x, 0))
            for x in [(1-ogr)*dr, ogr*dr]])
        gru = tuple([
            int(round(float(pivot_point) + x, 0))
            for x in [(1 - ogr) * gap, ogr * gap]])
        grl = tuple([
            int(round(float(pivot_point) - x, 0))
            for x in [(1 - ogr) * gap, ogr * gap]])

        rdata = {'Session': {'upper': sru, 'lower': srl}}
        rstr = ['Session delta (est.):\t' + (f' {sep} ').join([
            f'{x[0]} to {x[-1]}' for x in [sru, srl]])]
            # '%i to %i' % x for x in [sru, srl]])]
        rstr.append('Daily delta (est.):\t' + (f' {sep} ').join([
            f'{x[0]} to {x[-1]}' for x in [dru, drl]]))
        rdata['Daily'] = {'upper': dru, 'lower': drl}
        rstr.append('Gap (est.):\t' + (f' {sep} ').join([
            f'{x[0]} to {x[-1]}' for x in [gru, grl]]))
        rdata['Gap'] = {'upper': gru, 'lower': grl}

        if o_format == 'html':
            # from tags import HTML, TITLE, TABLE, TR, TD
            title = TITLE("Estimate of %s with reference on '%s' base on Pivot Point: %i" % (self.code.upper(), t_date, pivot_point))
            if concise:
                trs = [linesep.join([str(TR(TD(x))) for x in rstr])]
            else:
                trs = [TR(linesep.join([str(TD(x)) for x in [TD('Session delta (est.):'), TD(sru[0]), TD('to'), TD(sru[-1]), TD(sep), TD(srl[0]), TD('to'), TD(srl[-1])]]))]
                trs.append(TR(linesep.join([str(TD(x)) for x in [TD('Daily delta (est.):'), TD(dru[0]), TD('to'), TD(dru[-1]), TD(sep), TD(drl[0]), TD('to'), TD(drl[-1])]])))
                trs.append(TR(linesep.join([str(TD(x)) for x in [TD('Gap (est.):'), TD(gru[0]), TD('to'), TD(gru[-1]), TD(sep), TD(grl[0]), TD('to'), TD(grl[-1])]])))
            return str(HTML(linesep.join([str(x) for x in [title, TABLE(linesep.join(str(y) for y in trs))]])))
        if programmatic:
            return rdata
        return linesep.join(rstr)


def summary(*args, **kwargs):
    o_format = 'raw'
    if args:
        f_code = args[0].upper()
        if len(args) >= 3:
            o_format = args[2].lower()
        if len(args) >= 2:
            date = args[1]
    if 'format' in kwargs.keys():
        o_format = kwargs['format'].lower()
    if 'date' in kwargs.keys():
        date = kwargs['date']
    if 'code' in kwargs.keys():
        f_code = kwargs['code'].upper()
    if f_code:
        if o_format == 'html':
            hdr = TITLE("`%s` analyse" % f_code)
        mf = I2(code=f_code)
        period, tday = mf.period, mf.trade_day
        ltd = len(tday)
        if 'date' in kwargs.keys():
            if kwargs['date'] in tday:
                ltd = tday.index(kwargs['date']) + 1
            elif o_format == 'html':
                return str(HTML(linesep.join([str(x) for x in [hdr, 'Sorry, date entry invalid!']])))
            else:
                return 'Sorry, date entry invalid!'
        if ltd > period:
            i_fields, trs = ('Date', 'SMA', 'EMA', 'WMA', 'KAMA', 'RSI'), []
            if o_format == 'html':
                th = TH(TR(linesep.join([str(TD(x)) for x in i_fields])))
            else:
                hdr = '\t\t'.join(i_fields)
            for i in range(period + 1, ltd):
                i_values = []
                for x in i_fields[1:]:
                    i_values.append('%0.3f' % eval('mf.%s(date="%s")' % (x, tday[i])))
                if o_format == 'html':
                    trs.append(TR(linesep.join([str(TD(x)) for x in (('%s:' % tday[i],) + tuple(i_values))])))
                else:
                    hdr += '\t'.join(('\n%s',) + tuple(['%s' for k in i_fields[1:]])) % (('%s:' % tday[i],) + tuple(i_values))
        if ltd <= period:
            if o_format == 'html':
                hdr = str(HTML(linesep.join([str(x) for x in [hdr, 'Sorry, not enough data!']])))
            else:
                hdr = 'Sorry, not enough data!'
        elif o_format == 'html':
            hdr = str(HTML(linesep.join([str(x) for x in [hdr, TABLE(linesep.join(str(y) for y in ((th,) + tuple([str(z) for z in trs]))),{'width': '100%'})]])))
        return hdr
