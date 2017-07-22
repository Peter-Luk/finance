e = getattr(__import__('handy'), 'encoder')
__ = e({'datetime':('datetime',),'utilities':('today', 'waf','filepath'), 'os':('sep',), 'bt':('LF',)})
for _ in list(__.keys()): exec("%s=__['%s']" % (_, _))
# trday, dstr = lf(waf()[1]).fp.trade_day, ''
# pdate = datetime.strptime(trday[-1], '%Y-%m-%d').date()
# if (today.date() in [datetime.strptime(_, '%Y-%m-%d').date() for _ in trday]):
#     pdate = today.date()
#     if today.hour < 13:dstr='m'
fi = []
for _ in range(int(len(waf())/2)):
    cfv = lf(waf()[_]).fp.fdc('b')['Volume'].values[-1]
    try:
        nfv = lf(waf()[_+2]).fp.fdc('b')['Volume'].values[-1]
        if cfv > nfv:fi.append(waf()[_])
        else:fi.append(waf()[_+2])
    except:fi.append(waf()[_])
# for _ in waf()[:2]:
for _ in fi:
    trday, dstr = lf(_).fp.trade_day, ''
    pdate = datetime.strptime(trday[-1], '%Y-%m-%d').date()
    if (today.date() in [datetime.strptime(__, '%Y-%m-%d').date() for __ in trday]):
        pdate = today.date()
        if today.hour < 13:dstr='m'
    eval("getattr(lf('%s'),'plot')(filepath('%sbt%02i%s.html', type=sep.join(('data','plot')), subpath=sep.join(('%i','%s'))))" % (_, _[0].lower(), pdate.day, dstr, pdate.year, pdate.strftime('%B')))
    print("%s (%s) plotted." % (_, pdate.strftime('%d-%m-%Y')))
# for _ in waf()[:2]: eval("getattr(lf('%s'),'plot')('plot/%sbt%s.html')" % (_, _[0].lower(), today.strftime('%d%m')))
# if __name__ == '__main__': pass 
