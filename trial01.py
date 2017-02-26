import sqlite3 as lite
from utilities import gr, rnd, filepath, sep, linesep
from datetime import datetime
from statistics import mean, stdev

db_name, db_table = 'Futures', 'records'

class I2:
    def __init__(self, **args):
        self.trade_day = []
        self.__period, self.__db, self.__table = rnd(20 / gr), db_name, db_table
        if 'code' in args.keys():self.__code = args['code']
        if 'period' in args.keys():self.__period = args['period']
        if 'db_name' in args.keys():self.__db = args['db_name']
        if 'db_table' in args.keys():self.__table = args['db_table']

        self.__conn = lite.connect(filepath(self.__db))
        self.__conn.row_factory = lite.Row
        self.__data = self.__conn.cursor().execute("SELECT * FROM %s WHERE code='%s' ORDER BY date ASC, session DESC" % (self.__table, self.__code.upper())).fetchall()
        for i in range(len(self.__data)):
            if self.__data[i]['date'] not in self.trade_day:self.trade_day.append(self.__data[i]['date'])

    def __del__(self):
        self.__data = self.__conn = self.__code = self.__period = self.__db = self.__table = self.trade_day = None
        del(self.__data)
        del(self.__conn)
        del(self.__code)
        del(self.__period)
        del(self.__db)
        del(self.__table)
        del(self.trade_day)

    def __gt0(self, x):
        if x > 0:return True
        return False

    def __lt0(self, x):
        if x < 0:return True
        return False

    def __rangefinder(self, **args):
        if 'field' in args.keys():field = args['field']
        if 'value' in args.keys():value = args['value']
        res, hdr ={}, []
        for i in self.__data:
            if value == i[field]:hdr.append(i)
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
            res['A'] = {'range':sr, 'open':so, 'close':sc, 'volume':sv, 'high':sh, 'low':sl}
        else:
            so, sc, sh, sl, sv = hdr[0]['open'], hdr[0]['close'], hdr[0]['high'], hdr[0]['low'], hdr[0]['volume']
            do, dc, sr, dv = so, sc, sh -sl, sv
            dr, dh, dl = sr, sh, sl
            res['M'] = {'range':sr, 'open':so, 'close':sc, 'volume':sv, 'high':sh, 'low':sl}
        res['D'] = {'range':dr, 'open':do, 'close':dc, 'volume':dv, 'high':dh, 'low':dl}
        return res

    def append(self, **args):
        date = datetime.today().strftime('%Y-%m-%d')
        if 'date' not in args.keys():args['date'] = date
        if 'volume' in args.keys():volume = int(args['volume'])
        kt, vt = ('date', 'session', 'code'), (args['date'], args['session'], self.__code.upper())
        for k, v in args.items():
            if k not in ['date', 'session', 'code', 'volume']:
                kt += (k,)
                vt += (int(v),)
        if args['session'] == 'A':
            hdr = self.__rangefinder(field='date', value=date)
            if 'M' in hdr.keys():volume -= hdr['M']['volume']
        sq_str = "INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s) VALUES ('%s', '%s', '%s', %i, %i, %i, %i, %i)" % ((self.__table,) + kt + ('volume',) + vt + (volume,))
        try:
            self.__conn.cursor().execute(sq_str)
            self.__conn.commit()
        except:self.__conn.rollback()

    def ATR(self, **args):
        date, period = datetime.today().strftime('%Y-%m-%d'), self.__period
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        res, r_date, tr, i, hdr = {}, [], [], 0, {}

        tr.append([self.__data[0]['date'], self.__data[0]['session'], self.__data[0]['high'] - self.__data[0]['low']])
        i += 1
        while i < len(self.__data):
            if self.__data[i]['date'] == self.__data[i - 1]['date']:
                if self.__data[i]['high'] > self.__data[i - 1]['high']:ma = self.__data[i]['high']
                else:ma = self.__data[i - 1]['high']
                if self.__data[i]['low'] < self.__data[i - 1]['low']:mi = self.__data[i]['low']
                else:mi = self.__data[i - 1]['low']
            else:
                if self.__data[i]['high'] > self.__data[i - 1]['close']:ma, mi = self.__data[i]['high'], self.__data[i - 1]['close']
                elif self.__data[i]['low'] < self.__data[i - 1]['close']:mi, ma = self.__data[i]['low'], self.__data[i - 1]['close']
            tr.append([self.__data[i]['date'], self.__data[i]['session'], ma - mi])
            i += 1

        i = 0
        while i < len(tr):
            hdr[tr[i][0]] = tr[i][-1]
            if tr[i][0] not in r_date:r_date.append(tr[i][0])
            i += 1

        res[r_date[period - 1]] = mean([hdr[x] for x in r_date[:period]])
        for d in r_date[period:]:res[d] = (res[r_date[r_date.index(d) - 1]] * (period - 1) + hdr[d]) / period

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def EMA(self, **args):
        data, date, period, option = self.__data, datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'data' in args.keys():data = args['data']
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        if type(data[0]) in (int, float):
            res, kratio = [], 2. / (1 + period)
            for i in range(len(data)):
                if i == 0:res.append(data[i])
                else:res.append(data[i] * kratio + res[-1] * (1. - kratio))
            return res[-1]
        else:
            res, r_date, i, hdr = {}, [], 0, {}
            while i < len(data):
                if data[i]['date'] not in r_date:r_date.append(data[i]['date'])
                if option.upper() == 'C':hdr[data[i]['date']] = data[i]['close']
                if option.upper() == 'L':hdr[data[i]['date']] = data[i]['low']
                if option.upper() == 'H':hdr[data[i]['date']] = data[i]['high']
                if option.upper() == 'HL':hdr[data[i]['date']] = mean([data[i]['high'], data[i]['low']])
                if option.upper() == 'F':hdr[data[i]['date']] = mean([data[i]['high'], data[i]['low'],data[i]['open'], data[i]['close']])
                i += 1

            res[r_date[period - 1]] = mean([hdr[x] for x in r_date[:period]])
            for d in r_date[period:]:res[d] = (res[r_date[r_date.index(d) - 1]] * (period - 1) + hdr[d]) / period

            rkeys = list(res.keys())
            rkeys.sort()
            if date in rkeys:return res[date]
            return res[rkeys[-1]]

    def KAMA(self, **args):
        date, period = datetime.today().strftime('%Y-%m-%d'), self.__period
        fast, slow = rnd(period / gr ** 2), period
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'slow' in args.keys():slow = args['slow']
        if 'fast' in args.keys():fast = args['fast']
        res, trade_day, tr, i, hdr, er, sc = {}, [], {}, 0, {}, {}, {}

        while i < len(self.__data):
            if self.__data[i]['date'] in trade_day:
                if self.__data[i]['session'] == 'A':tr[self.__data[i]['date']] = self.__data[i]['close']
            else:
                trade_day.append(self.__data[i]['date'])
                tr[self.__data[i]['date']] = self.__data[i]['close']
            i += 1

        for i in range(period, len(trade_day)):
            ch, vo = abs(tr[trade_day[i]] - tr[trade_day[i-period]]), sum([abs(tr[trade_day[x]] - tr[trade_day[x - 1]]) for x in range(i-period, i)])
            er[trade_day[i]] = ch / float(vo)

        for i in range(slow, len(trade_day)):
            sfc, ssc = 2. / (fast + 1), 2. / (slow + 1)
            sc[trade_day[i]] = (er[trade_day[i]] * (sfc - ssc) + ssc) ** 2

        for i in range(slow, len(trade_day)):
            if i == slow:res[trade_day[i]] = self.EMA(date=trade_day[i])
            else:res[trade_day[i]] = res[trade_day[i - 1]] + sc[trade_day[i]] * (tr[trade_day[i]] - res[trade_day[i - 1]])

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def RSI(self, **args):
        date, period = datetime.today().strftime('%Y-%m-%d'), self.__period
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        res, trade_day, tr, i, hdr = {}, [], {}, 0, {}

        while i < len(self.__data):
            if self.__data[i]['date'] in trade_day:
                if self.__data[i]['session'] == 'A':tr[self.__data[i]['date']] = self.__data[i]['close']
            else:
                trade_day.append(self.__data[i]['date'])
                tr[self.__data[i]['date']] = self.__data[i]['close']
            i += 1

        i = 1
        while i < len(trade_day):
            hdr[trade_day[i]] = tr[trade_day[i]] - tr[trade_day[i - 1]]
            i += 1

        i, ag, al = period, {}, {}
        trade_day = list(hdr.keys())
        trade_day.sort()
        for i in range(period, len(trade_day)):
            if i == period:
                ag[trade_day[i]], al[trade_day[i]] = sum(list(filter(self.__gt0, [hdr[trade_day[j]] for j in range(i - period, i)]))) / float(period), abs(sum(list(filter(self.__lt0, [hdr[trade_day[j]] for j in range(i - period, i)])))) / float(period)
            else:
                if hdr[trade_day[i]] > 0:
                    ag[trade_day[i]] = (ag[trade_day[i - 1]] * (period - 1) + hdr[trade_day[i]]) / period
                    al[trade_day[i]] = al[trade_day[i - 1]] * (period - 1) / period
                if hdr[trade_day[i]] < 0:
                    ag[trade_day[i]] = ag[trade_day[i - 1]] * (period - 1) / period
                    al[trade_day[i]] = (al[trade_day[i - 1]] * (period - 1) + abs(hdr[trade_day[i]])) / period
            res[trade_day[i]] = 100 - 100 / ( 1 + float(ag[trade_day[i]]) / al[trade_day[i]])

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def BBW(self, **args):
        date, period = datetime.today().strftime('%Y-%m-%d'), self.__period
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        res, r_date, i, hdr = {}, [], 0, {}

        while i < len(self.__data):
            if self.__data[i]['date'] not in r_date:r_date.append(self.__data[i]['date'])
            hdr[self.__data[i]['date']] = self.__data[i]['high'] - self.__data[i]['low']
            i += 1

        for i in range(len(r_date) - period):
            res[r_date[period + i]] = 2 * 2 * stdev([hdr[r_date[x]] for x in range(i, period + i)]) / gr

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]
#
    def SAR(self, **args):
        date, period, option = datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        res, r_date, i, hdr = {}, [], 0, {}
        sp = {}
        while i < len(self.__data):
            if self.__data[i]['date'] not in r_date:r_date.append(self.__data[i]['date'])
#            if option.upper() == 'C':hdr[self.__data[i]['date']] = self.__data[i]['close']
#            if option.upper() == 'HL':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low']])
#            if option.upper() == 'F':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low'],self.__data[i]['open'], self.__data[i]['close']])
            i += 1

        for d in range(1,len(r_date)):
            if d == 1:
                tmp = self.__rangefinder(field='date', value=r_date[d - 1])
                sp[r_date[d]] = tmp['D']['low']
                if tmp['D']['close'] < tmp['D']['open']:sp[d] = tmp['D']['high']
            else:pass
        return sp
#        for i in range(len(r_date) - period):
#            res[r_date[period + i]] = mean([hdr[r_date[x]] for x in range(i, period + i)])

#        rkeys = list(res.keys())
#        rkeys.sort()
#        if date in rkeys:return res[date]
#        return res[rkeys[-1]]
#
    def SMA(self, **args):
        date, period, option = datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        res, r_date, i, hdr = {}, [], 0, {}

        while i < len(self.__data):
            if self.__data[i]['date'] not in r_date:r_date.append(self.__data[i]['date'])
            if option.upper() == 'C':hdr[self.__data[i]['date']] = self.__data[i]['close']
            if option.upper() == 'HL':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low']])
            if option.upper() == 'F':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low'],self.__data[i]['open'], self.__data[i]['close']])
            i += 1

        for i in range(len(r_date) - period):
            res[r_date[period + i]] = mean([hdr[r_date[x]] for x in range(i, period + i)])

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def APZ(self, **args):
        date, period, option = datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        res, r_date, i = {}, [], 0

        while i < len(self.__data):
            if self.__data[i]['date'] not in r_date:r_date.append(self.__data[i]['date'])
            i += 1

        for i in range(period, len(r_date)):
            eh = [self.EMA(date=r_date[i - j], period=period, option='H') for j in range(period)]
            el = [self.EMA(date=r_date[i - j], period=period, option='L') for j in range(period)]
            vv = self.EMA(data=[eh[j] for j in range(len(eh))], period=len(eh)) - self.EMA(data=[el[j] for j in range(len(el))], period=len(el))
            ema = self.EMA(date=r_date[i], option='HL', period=period)
            res[r_date[i]] = rnd(ema - (vv / 2)), rnd(ema + (vv / 2))

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def BB(self, **args):
        date, period, option = datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        res, r_date, i = {}, [], 0

        while i < len(self.__data):
            if self.__data[i]['date'] not in r_date:r_date.append(self.__data[i]['date'])
            i += 1

        for i in range(len(r_date) - period):
            res[r_date[period + i]] = tuple([rnd(self.SMA(date=r_date[period + i], period=period, option=option) + x) for x in [self.BBW(date=r_date[period + i], period=period) / 2, -self.BBW(date=r_date[period + i], period=period) / 2]])

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def WMA(self, **args):
        date, period, option = datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        res, r_date, i, hdr = {}, [], 0, {}

        while i < len(self.__data):
            if self.__data[i]['date'] not in r_date:r_date.append(self.__data[i]['date'])
            if option.upper() == 'C':hdr[self.__data[i]['date']] = (self.__data[i]['close'], self.__data[i]['volume'])
            if option.upper() == 'HL':hdr[self.__data[i]['date']] = (mean([self.__data[i]['high'], self.__data[i]['low']]), self.__data[i]['volume'])
            if option.upper() == 'F':hdr[self.__data[i]['date']] = (mean([self.__data[i]['high'], self.__data[i]['low'],self.__data[i]['open'], self.__data[i]['close']]), self.__data[i]['volume'])
            i += 1

        for i in range(len(r_date) - period):
            res[r_date[period + i]] = sum([hdr[r_date[x]][0] * hdr[r_date[x]][-1] for x in range(i, period + i)]) / float(sum([hdr[r_date[x]][-1] for x in range(i, period + i)]))

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def KC(self, **args):
        date, period, option = datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        width, base = self.ATR(date=date, period=period), self.EMA(date=date, period=period, option=option)
        return (rnd(base - width * gr), rnd(base + width * gr))

    def daatr(self, **args):
        date, period = None, 5
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if date:return self.ATR(date=date, period=period) - self.ATR(date=date)
        else: return self.ATR(period=period) - self.ATR()

    def estimate(self, **args):
        if 'pivot_point' in args.keys():pivot_point = int(args['pivot_point'])
        else:return "Essential value ('pivot _point') is obmitted"
        t_date, programmatic, o_format, concise = self.trade_day[-1], False, 'raw', False
        if 'date' in args.keys():t_date = args['date']
        if 'programmatic'in args.keys():programmatic = args['programmatic']
        if 'format'in args.keys():o_format = args['format'].lower()
        if 'concise'in args.keys():concise = args['concise']

        hdr = self.__rangefinder(field='date', value=t_date)
        dr, gap = hdr['D']['range'], abs(pivot_point - hdr['D']['close'])
        if 'A' in hdr.keys():sr = hdr['A']['range']
        elif 'M' in hdr.keys():
            sr = hdr['M']['range']
            i_date = self.trade_day[self.trade_day.index(t_date) - 1]
            hdr2 = self.__rangefinder(field='date', value=i_date)
            dr = hdr2['D']['range']

        ogr = 1. / gr
        sru = tuple([int(round(float(pivot_point)+x, 0)) for x in [(1-ogr)*sr, ogr*sr]])
        srl = tuple([int(round(float(pivot_point)-x, 0)) for x in [(1-ogr)*sr, ogr*sr]])
        dru = tuple([int(round(float(pivot_point)+x, 0)) for x in [(1-ogr)*dr, ogr*dr]])
        drl = tuple([int(round(float(pivot_point)-x, 0)) for x in [(1-ogr)*dr, ogr*dr]])
        gru = tuple([int(round(float(pivot_point)+x, 0)) for x in [(1-ogr)*gap, ogr*gap]])
        grl = tuple([int(round(float(pivot_point)-x, 0)) for x in [(1-ogr)*gap, ogr*gap]])

        rstr, rdata = ['Session delta (est.):\t' + (' %s ' % sep).join(['%i to %i' % x for x in [sru, srl]])], {'Session':{'upper':sru, 'lower':srl}}
        rstr.append('Daily delta (est.):\t' + (' %s ' % sep).join(['%i to %i' % x for x in [dru, drl]]))
        rdata['Daily'] = {'upper':dru, 'lower':drl}
        rstr.append('Gap (est.):\t' + (' %s ' % sep).join(['%i to %i' % x for x in [gru, grl]]))
        rdata['Gap'] = {'upper':gru, 'lower':grl}

        if o_format == 'html':
            from tags import HTML, TITLE, TABLE, TH, TR, TD
            title = TITLE("Estimate of %s with reference on '%s'" % (self.__code.upper(), t_date))
            if concise:
                trs = [linesep.join([str(TR(TD(x))) for x in rstr])]
            else:
                trs = [TR(linesep.join([str(TD(x)) for x in [TD('Session delta (est.):'), TD(sru[0]), TD('to'), TD(sru[-1]), TD(sep), TD(srl[0]), TD('to'), TD(srl[-1])]]))]
                trs.append(TR(linesep.join([str(TD(x)) for x in [TD('Daily delta (est.):'), TD(dru[0]), TD('to'), TD(dru[-1]), TD(sep), TD(drl[0]), TD('to'), TD(drl[-1])]])))
                trs.append(TR(linesep.join([str(TD(x)) for x in [TD('Gap (est.):'), TD(gru[0]), TD('to'), TD(gru[-1]), TD(sep), TD(grl[0]), TD('to'), TD(grl[-1])]])))
            return str(HTML(linesep.join([str(x) for x in [title, TABLE(linesep.join(str(y) for y in trs))]])))
        if programmatic:return rdata
        return linesep.join(rstr)

def summary(**args):
    o_format = 'raw'
    if 'format' in args.keys():
        o_format = args['format'].lower()
        from tags import HTML, TITLE, TABLE, TH, TR, TD
    if 'code' in args.keys():
        f_code = args['code'].upper()
        if o_format == 'html':hdr = TITLE("`%s` analyse" % f_code)
        mf = I2(code=f_code)
        period, tday = mf._I2__period, mf.trade_day
        ltd = len(tday)
        if 'date' in args.keys():
            if args['date'] in tday:ltd = tday.index(args['date']) + 1
            elif o_format == 'html':return str(HTML(linesep.join([str(x) for x in [hdr, 'Sorry, date entry invalid!']])))
            else:return 'Sorry, date entry invalid!'
        if ltd > rnd(period * gr):
            i_fields, trs = ('Date', 'SMA', 'EMA', 'WMA', 'KAMA', 'RSI'), []
            if o_format == 'html':th = TH(TR(linesep.join([str(TD(x)) for x in i_fields])))
            else:hdr = '\t\t'.join(i_fields)
            for i in range(rnd(period * gr), ltd):
                i_values = []
                for x in i_fields[1:]:i_values.append('%0.3f' % eval('mf.%s(date="%s")' % (x, tday[i])))
                if o_format == 'html':trs.append(TR(linesep.join([str(TD(x)) for x in (('%s:' % tday[i],) + tuple(i_values))])))
                else:hdr += '\t'.join(('\n%s',) + tuple(['%s' for k in i_fields[1:]])) % (('%s:' % tday[i],) + tuple(i_values))
        elif ltd > period:
            i_fields, trs = ('Date', 'SMA', 'EMA', 'WMA', 'RSI'), []
            if o_format == 'html':th = TH(TR(linesep.join([str(TD(x)) for x in i_fields])))
            else:hdr = '\t\t'.join(i_fields)
            for i in range(period, ltd):
                i_values = []
                for x in i_fields[1:]:i_values.append('%0.3f' % eval('mf.%s(date="%s")' % (x, tday[i])))
                if o_format == 'html':trs.append(TR(linesep.join([str(TD(x)) for x in (('%s:' % tday[i],) + tuple(i_values))])))
                else:hdr += '\t'.join(('\n%s',) + tuple(['%s' for k in i_fields[1:]])) % (('%s:' % tday[i],) + tuple(i_values))
        if ltd <= period:
            if o_format == 'html':hdr = str(HTML(linesep.join([str(x) for x in [hdr, 'Sorry, not enough data!']])))
            else:hdr = 'Sorry, not enough data!'
        elif o_format == 'html':hdr = str(HTML(linesep.join([str(x) for x in [hdr, TABLE(linesep.join(str(y) for y in ((th,) + tuple([str(z) for z in trs]))),{'width':'100%'})]])))
        return hdr
