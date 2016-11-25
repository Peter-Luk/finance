from sqlite3 import connect
from SQLiteHelper import Query
from utilities import filepath, gr
from sys import argv, version_info
from os import environ
from datetime import datetime

fullpath, today = filepath('Futures'), datetime.today()
futures_type, month_initial, year,month, month_string = ('HSI', 'MHI', 'HHI', 'MCH'), {'January':'F', 'February':'G', 'March':'H', 'April':'J', 'May':'K', 'June':'M', 'July':'N', 'August':'Q', 'September':'U', 'October':'V', 'November':'X', 'December':'Z'}, '%i' % today.year, today.month, today.strftime('%B')
avail_indicators, __cal_month = ('wma','kama','ema','hv'), (3, 6, 9, 12)

def __ms(contract):
        if version_info.major == 2:action = raw_input('(I)nsert / (A)nalysis: ')
        if version_info.major == 3:action = input('(I)nsert / (A)nalysis: ')
        if action.upper() == 'A':
            an = Analyser(contract.upper())
            res = an.daily(type=avail_indicators)
            if len(res):
                keys = list(res.keys())
                keys.sort()
                tmpstr = 'date'.upper()
                for i in avail_indicators:tmpstr += '\t\t%s' % i.upper()
                print(tmpstr)
                for k in keys:print('%s:\t%0.3f\t%0.3f\t%0.3f\t%0.5f' % (k,res[k]['wma10'],res[k]['kama10'],res[k]['ema10'],res[k]['hv10']))
        elif action.upper() == 'I':
            if version_info[0] == 2:open, high, low, close, volume = raw_input('Open at: '), raw_input('Highest at: '), raw_input('Lowest at: '), raw_input('Close at: '), raw_input('Volume: ')
            if version_info[0] == 3:open, high, low, close, volume = input('Open at: '), input('Highest at: '), input('Lowest at: '), input('Close at: '), input('Volume: ')
            if open and high and low and close and volume:
                an._Futures__fu.append((contract.upper(), int(open), int(high), int(low), int(close), int(volume)))
            res = an._Analyser__fu.show(criteria={'date':an._Analyser__today})
            if len(res):
                for r in res:print(r)

def calandex():
    idx, __m = (), []
    for i in __cal_month:
        delta_month = i - month
        if i < month:delta_month += 12
        __m.append(delta_month)
    __m.sort()
    for i in __m:idx += (dex(i),)
    return idx

def dex(n=0):
    if n in range(12):
        n_month, n_year = month + n, today.year
        if n_month > 12 and n_month != n_month % 12:n_month, n_year = n_month % 12, n_year + 1
        return month_initial[datetime(n_year, n_month, 1).strftime('%B')] + ('%i' % n_year)[-1]
    else:print("Out of range (0 - 11)")

def waf(delta=0):
    futures = [''.join((f,dex(delta))) for f in futures_type[:-2]]
    futures += [''.join((f,dex(delta+1))) for f in futures_type[:-2]]
    return tuple(futures)

def estimate(pp, icode=waf()[0], gr=1/gr, extended=False):
    thdr,rf = [],('session','open','high','low','close','volume')
    an = Analyser(icode)
    adate,ancur,ti = an._Analyser__unique_date(),an._Analyser__cur,an._Analyser__fu._Futures__table
    idate,cday,csession = adate[-1],an._Futures__today,an._Futures__session
    dd = ancur.execute("SELECT %s, %s, %s, %s, %s, %s FROM %s WHERE date='%s' AND code='%s'" % (rf+(ti,idate,icode))).fetchall()
    for d in dd:
        rdict = {}
        for r in range(len(rf)):rdict['%s'%rf[r]] = d[r]
        thdr.append(rdict)
    so = do = float(thdr[-1]['open'])
    sh = dh = float(thdr[-1]['high'])
    sc = dc = float(thdr[-1]['close'])
    sl = dl = float(thdr[-1]['low'])
    if cday > idate:
        if len(thdr) > 1:
            if thdr[0]['session'] == 'M':do = float(thdr[0]['open'])
            if thdr[0]['high'] > dh:dh = float(thdr[0]['high'])
            if thdr[0]['low'] < dl:dl = float(thdr[0]['low'])
            if thdr[0]['session'] == 'A':dc = float(thdr[0]['close'])
    elif cday == idate:
        dd = ancur.execute("SELECT %s, %s, %s, %s, %s, %s FROM %s WHERE date='%s' AND code='%s'" % (rf+(ti,adate[-2],icode))).fetchall()
        for d in dd:
            rdict = {}
            for r in range(len(rf)):rdict['%s'%rf[r]] = d[r]
        sh,sc,sl = float(rdict['high']),float(rdict['close']),float(rdict['low'])
    sru = tuple([int(round(float(pp)+x,0)) for x in [(1-gr)*(sh-sl),gr*(sh-sl)]])
    srl = tuple([int(round(float(pp)-x,0)) for x in [(1-gr)*(sh-sl),gr*(sh-sl)]])
    dru = tuple([int(round(float(pp)+x,0)) for x in [(1-gr)*(dh-dl),gr*(dh-dl)]])
    drl = tuple([int(round(float(pp)-x,0)) for x in [(1-gr)*(dh-dl),gr*(dh-dl)]])

    rstr,uday = ["Session delta (est.): %i to %i / %i to %i,"%(sru+srl)],an._Analyser__unique_date()
    if extended:
        for i in avail_indicators[:-1]:
            if uday:
                res = an._Analyser__cur.execute("SELECT %s FROM '%s' WHERE code='%s' AND date='%s'"%(i,'indicators',icode,uday[-1])).fetchone()
                ivalue = float(res[0])
                iru = tuple([int(round(ivalue+x,0)) for x in [(1-gr)*(sh-sl),gr*(sh-sl)]])
                irl = tuple([int(round(ivalue-x,0)) for x in [(1-gr)*(sh-sl),gr*(sh-sl)]])
                rstr.append("'%s': %i to %i / %i to %i,"%((i.upper(),)+iru+irl))
    rstr.append("Daily delta (est.): %i to %i / %i to %i and"%(dru+drl))
    mrf = (rf[0],rf[-2])
    dm = ancur.execute("SELECT %s, %s FROM  %s WHERE date='%s' AND code='%s'" % (mrf + (ti,adate[-2],icode))).fetchall()
    if dm:mc = float(dm[0][-1])
    if len(dm)>1:
        if dm[1][0]=='A':mc = float(dm[1][-1])
    gru = tuple([int(round(float(pp)+x,0)) for x in [(1-gr)*(do-mc),gr*(do-mc)]])
    grl = tuple([int(round(float(pp)-x,0)) for x in [(1-gr)*(do-mc),gr*(do-mc)]])
    rstr.append("Gap (est.): %i to %i / %i to %i."%(gru+grl))
    return '\n'.join(rstr)

def indicate(code=waf()):
    methods,ti,qstr,ui,ii,plstr = ('wma','kama','ema','hv'),'indicators',[],0,0,'s'
    for c in code:
        an = Analyser(c)
        res = an.daily(type=methods)
        rkeys = list(res.keys())
        rkeys.sort()
        for k in rkeys:
            uq, temp = "", an._Analyser__cur.execute("SELECT %s, %s, %s, %s FROM %s WHERE code='%s' AND date='%s'" % (methods + (ti, c, k))).fetchone()
            if temp and len(temp)==len(methods):
                uv = ""
                for i in range(len(temp)):
                    if '%0.10f'%temp[i] != '%0.10f'%res[k]['%s10'%methods[i]]:uv += ",%s=%0.10f" % (methods[i], res[k]['%s10'%methods[i]])
                if uv:uq += "UPDATE %s SET %s WHERE code='%s' AND date='%s'" % (ti, uv[1:], c, k)
                if uq:
                    ui += 1
                    qstr.append(uq)
            else:
                fname, fval = "('%s', '%s'" % ('code', 'date'), "('%s', '%s'" % (c , k)
                for j in list(res[k].keys()):
                    fname += ", '%s'" % j[:-2]
                    fval += ", %.10f" % res[k][j]
                ii += 1
                qstr.append("INSERT INTO %s %s) VALUES %s)" % (ti, fname, fval))
    [an._Analyser__cur.execute(q) for q in qstr]
    an._Analyser__conn.commit()
    if ui == ii == False:
        print('No records altered.')
    else:
        if ui > 1:print('%i record%s updated.\n'%(ui,plstr))
        else:print('%i record updated.\n'%ui)
        if ii > 1:print('%i record%s inserted.'%(ii,plstr))
        else:print('%i record inserted.'%ii)

class Futures(Query):
    __today, __hour, __minute, __session = today.date(), today.hour, today.minute, 'A'
    if __hour < 13: __session = 'M'
    elif __hour == 12 and __minute < 50: __session = 'M'

    def __init__(self):
        self.__conn = connect(fullpath)
        self.__table, self.__cur, self.__qu = 'records', self.__conn.cursor(), Query(self.__conn)

    def __del__(self):
        self.__table = self.__cur = self.__qu = self.__conn = None
        del(self.__table)
        del(self.__qu)
        del(self.__cur)
        del(self.__conn)

    def amend(self, **args):
        if ('criteria' in args) and ('value' in args):
            self.__qu.amend(self.__table, criteria=args['criteria'], value=args['value'])
            self.__conn.commit()

    def append(self, value, req_date=__today, session=__session):
        try:
            fields, vDict, mv = ('code', 'open', 'high', 'low', 'close', 'volume', 'date', 'session'), {}, 0
            if session == 'A':
                req_fields, cond = ('volume',), {'date':req_date, 'code':value[0], 'session':'M'}
                mv = self.show(fields=req_fields, criteria=cond)[0][0]
            if len(value) == len(fields) - 2:volume = value[-1] - mv
            if type(value) is tuple:
                i = 0
                for f in fields[:-3]:
                    vDict[f] = value[i]
                    i += 1
            vDict['volume'], vDict['date'], vDict['session'] = volume, req_date, session
            self.__qu.insert(self.__table, values=vDict)
            self.__conn.commit()
        except:self.__conn.rollback()

    def daily(self, contract_code='MHI', month_initial=dex(), req_date=__today):
        try:
            if contract_code in futures_type:
                afternoon, code, req_fields = False, contract_code + month_initial, ('open', 'high', 'low', 'close', 'volume')
                try:
                    cond = {'code':code, 'date':req_date, 'session':'A'}
                    afternoon = self.show(fields=req_fields, criteria=cond)[0]
                except:pass
                cond['session'] = 'M'
                morning = self.show(fields=req_fields, criteria=cond)[0]
                if afternoon:
                    if morning:
                        maximum, minimum = morning[1], morning[2]
                        if afternoon[1] > maximum:maximum = afternoon[1]
                        if afternoon[2] < minimum:minimum = afternoon[2]
                        return (morning[0], maximum, minimum, afternoon[-2], morning[-1] + afternoon[-1])
                    else:return afternoon
                elif morning:return morning
                return "No trade data for %s on %s" % (code, date)
        except:pass

    def show(self, **args):
        if 'criteria' in args:
            if 'fields' in args:
                if 'order' in args:
                    return self.__qu.show(self.__table, criteria=args['criteria'],fields=args['fields'],order=args['order'])
                else:
                    return self.__qu.show(self.__table, criteria=args['criteria'],fields=args['fields'])
            else:
                if 'order' in args:
                    return self.__qu.show(self.__table, criteria=args['criteria'],order=args['order'])
                else:
                    return self.__qu.show(self.__table, criteria=args['criteria'])
        else:
            if 'fields' in args:
                if 'order' in args:
                    return self.__qu.show(self.__table, fields=args['fields'],order=args['order'])
                else:
                    return self.__qu.show(self.__table, fields=args['fields'])
            else:
                if 'order' in args:
                    return self.__qu.show(self.__table, order=args['order'])
                else:
                    return self.__qu.show(self.__table)

class Analyser(Futures):
    __today = today.strftime('%d/%m/%Y')

    def __init__(self, contract):
        self.__contract, self.__conn = contract, connect(fullpath)
        self.__cur, self.__fu = self.__conn.cursor(), Futures()

    def __del__(self):
        self.__contract = self.__conn = self.__cur = self.__fu = None
        del(self.__contract)
        del(self.__fu)
        del(self.__cur)
        del(self.__conn)

    def __s2dt(self, dateString, separator='-'):
        if type(dateString) is str:
            if separator == '/':day, month, year = list(map(int, dateString.split(separator)))
            elif separator == '-':year, month, day = list(map(int, dateString.split(separator)))
            return datetime(year, month, day)

    def __unique_date(self):
        re = self.__fu.show(fields=('date',), criteria={'code':self.__contract})
        days = []
        for r in re:
            tmp = self.__s2dt(str(r[0])).date()
            if tmp not in days:days.append(tmp)
        return days

    def daily(self, n=10, **args):
        days = self.__unique_date()
        if 'type' in args:
            v, self.__type = {}, args['type']
            if type(self.__type) is str:self.__type = (self.__type,)
            if type(self.__type) is tuple:
                for t in self.__type:
                    if t.upper() == 'SMA':self.__sma = self.sma(n=n)
                    if t.upper() == 'WMA':self.__wma = self.wma(n=n)
                    if t.upper() == 'EMA':self.__ema = self.ema(n=n)
                    if t.upper() == 'KAMA':self.__kama = self.kama(n=n)
                    if t.upper() == 'HV':self.__hv = self.hv(n=n)
                for d in days[n:]:
                    tmp = {}
                    for t in self.__type:
                        if t.upper() == 'SMA':holder, keys = self.__sma, list(self.__sma.keys())
                        if t.upper() == 'EMA':holder, keys = self.__ema, list(self.__ema.keys())
                        if t.upper() == 'WMA':holder, keys = self.__wma, list(self.__wma.keys())
                        if t.upper() == 'KAMA':holder, keys = self.__kama, list(self.__kama.keys())
                        if t.upper() == 'HV':holder, keys = self.__hv, list(self.__hv.keys())
                        for k in keys:
                            if k == d:tmp['%s%s'%(t, n)] = holder[k]
                    v[d] = tmp
                return v

    def ema(self, n=10, k=.2, type='C'):
        days = self.__unique_date()
        if len(days) > n:
            result, j = {}, 0
            while j < (len(days) - n):
                r_days = self.previous_n(n=n, date=days[n + j])
                r_days.append(days[n + j])
                today = self.__fu.daily(self.__contract[:3], self.__contract[-2:], days[n + j])
                if j == 0:result[days[n]] = self.wma(type=type)[days[n]]
                else:
                    if type == 'HL':result[days[n + j]] = ((1 - k) * result[days[n + j - 1]]) + (k * sum(today[1:3]) / 2.)
                    elif type == 'OC':result[days[n + j]] = ((1 - k) * result[days[n + j - 1]]) + (k * sum((today[0], today[3])) / 2.)
                    elif type == 'C':result[days[n + j]] = ((1 - k) * result[days[n + j - 1]]) + (k * today[3])
                j += 1
            return result

    def hv(self, n=10, type='C'):
        days = self.__unique_date()
        if len(days) > n:
            result, j = {}, 0
            while j < (len(days) - n):
                r_days = self.previous_n(n=n, date=days[n + j])
                r_days.append(days[n + j])
                inner = [self.__fu.daily(self.__contract[:3], self.__contract[-2:], x) for x in r_days]
                delta = 0.
                for i in inner:delta += (i[1] - i[2]) / float(i[0])
                result[days[n + j]] = delta / n
                j += 1
            return result

    def kama(self, n=10, fast=5, slow=10, k=.2, type='C'):
        fsc, ssc, days = 2. / (fast + 1.), 2. / (slow + 1.), self.__unique_date()
        if len(days) > n:
            result, j = {}, 0
            while j < (len(days) - n):
                r_days = self.previous_n(n=n, date=days[n + j])
                r_days.append(days[n + j])
                inner = [self.__fu.daily(self.__contract[:3], self.__contract[-2:], x) for x in r_days]
                today = self.__fu.daily(self.__contract[:3], self.__contract[-2:], days[n + j])
                if j == 0:result[days[n]] = self.ema(type=type)[days[n]]
                else:
                    direction = inner[-1][-2] - inner[0][-2]
                    t_range, i = 0., 0
                    while i < len(inner) - 1:
                         t_range += abs(inner[i + 1][-2] - inner[i][-2])
                         i += 1
                    wsc = abs(direction / t_range) * (fsc - ssc) + ssc
                    c = wsc ** 2
                    result[days[n + j]] = c * today[-2] + (1. - c) * result[days[n + j - 1]]
#                    if type == 'HL':result[days[n + j]] = ((1 - k) * result[days[n + j - 1]]) + (k * sum(today[1:3]) / 2.)  
#                    elif type == 'OC':result[days[n + j]] = ((1 - k) * result[days[n + j - 1]]) + (k * sum((today[0], today[3])) / 2.)
#                    elif type == 'C':result[days[n + j]] = ((1 - k) * result[days[n + j - 1]]) + (k * today[3])  
                j += 1
            return result

    def previous_n(self, **args):
        if ('n' in args) and ('date' in args):
            result, res, days = [], [], self.__unique_date()
            for d in days:
                if d < args['date']:res.append(d)
            try:
                for d in res[-(args['n'] - 1):]:result.append(d)
                return result
            except:pass

    def sar(self, af=0.02):
        days = self.__unique_date()
        if len(days) > 1:
            holder, ep, j = {}, {}, 0
            while j < (len(days) - 1):
                if j == 0:
                    previous_day = self.__fu.daily(self.__contract[:3], self.__contract[-2:], self.previous_n(n=1, date=days[1 + j])[0])
                    ep[days[j + 1]] = previous_day[1]
                    holder[days[j + 1]] = previous_day[2]
                else:pass
                j += 1
            return holder

    def session(self, n=20):
        res = self.__fu.show(fields=('date', 'open', 'high', 'low', 'close', 'volume'), criteria={'code':self.__contract}, order=('date ASC', 'session DESC'))
        if len(res)> n:
            i = 0
            while i < len(res):
                tlist = list(res[i])
                tdate = tlist.pop(0)
                tlist.insert(0, self.__s2dt(str(tdate)))
                res[i] = tuple(tlist)
                i += 1
            return self.__process_1(res, n)
        else:return "Ooch, not enough data!"

    def session_volume(self, **cond):
        try:
            return self.__fu.show(fields=('volume',), criteria={'code':cond['code'],'date':cond['date'],'session':cond['session']})[0][0]
        except:pass

    def sma(self, n=10, type='C'):
        days = self.__unique_date()
        if len(days) > n:
            result, j = {}, 0
            while j < (len(days) - n):
                r_days = self.previous_n(n=n, date=days[n + j])
                r_days.append(days[n + j])
#                inner = map(lambda x:self.__fu.daily(self.__contract[:3], self.__contract[-2:], x), r_days)
#                if type == 'HL':total = sum(map(lambda x:sum(x[1:3]) / 2., inner))
#                elif type == 'OC':total = sum(map(lambda x:sum((x[0],x[3])) / 2., inner))
#                elif type == 'C':total = sum(map(lambda x:x[3], inner))
                inner = [self.__fu.daily(self.__contract[:3], self.__contract[-2:], x) for x in r_days]
                if type == 'HL':total = sum([sum(x[1:3]) / 2. for x in inner])
                elif type == 'OC':total = sum([sum((x[0],x[3])) / 2. for x in inner])
                elif type == 'C':total = sum([x[3] for x in inner])
                result[days[n + j]] = total / float(n)
                j += 1
            return result

    def wma(self, n=10, type='C'):
        days = self.__unique_date()
        if len(days) > n:
            result, j = {}, 0
            while j < (len(days) - n):
                r_days = self.previous_n(n=n, date=days[n + j])
                r_days.append(days[n + j])
                inner = [self.__fu.daily(self.__contract[:3], self.__contract[-2:], x) for x in r_days]
                if type == 'HL':total = sum([sum(x[1:3]) / 2. * x[-1] for x in inner])
                elif type == 'OC':total = sum([sum((x[0],x[3])) / 2. * x[-1] for x in inner])
                elif type == 'C':total = sum([x[3] * x[-1] for x in inner])
                result[days[n + j]] = total / float(sum([x[-1] for x in inner]))
                j += 1
            return result

if __name__ == "__main__":
    try:
        contract = argv[1]
        __ms(contract)
        if version_info.major == 2:confirm = raw_input('Proceed Y/N? ')
        if version_info.major == 3:confirm = input('Proceed Y/N? ')
    except:confirm = 'Y'
    while confirm.upper() != 'N':
        if version_info[0].major == 2:contract = raw_input("Contract code: ")
        if version_info.major == 3:contract = input("Contract code: ")
        __ms(contract)
        if version_info.major == 2:confirm = raw_input('Proceed Y/N? ')
        if version_info.major == 3:confirm = input('Proceed Y/N? ')
