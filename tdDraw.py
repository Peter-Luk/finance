e = getattr(__import__('handy'), 'encoder')
__ = e({'datetime':('datetime',),'utilities':('today', 'waf','filepath'), 'os':('sep',), 'bt':('LF',)})
for _ in list(__.keys()): exec("%s=__['%s']" % (_, _))
fi = []
for _ in range(int(len(waf())/2)):
    try:
        nfv = lf(waf()[_+2]).fp.fdc('b')['Volume'].values[-1]
        cfv = lf(waf()[_]).fp.fdc('b')['Volume'].values[-1]
        if cfv > nfv:fi.append(waf()[_])
        else:fi.append(waf()[_+2])
    except:fi.append(waf()[_])
for _ in fi:
    trday, dstr = lf(_).fp.trade_day, ''
    pdate = datetime.strptime(trday[-1], '%Y-%m-%d').date()
    if (today.date() in [datetime.strptime(__, '%Y-%m-%d').date() for __ in trday]):
        pdate = today.date()
        if today.hour < 13:dstr='m'
    eval("getattr(lf('%s'),'plot')(filepath('%sbt%02i%s.html', type=sep.join(('data','plot')), subpath=sep.join(('%i','%s'))))" % (_, _[0].lower(), pdate.day, dstr, pdate.year, pdate.strftime('%B')))
    print("%s (%s) plotted." % (_, pdate.strftime('%d-%m-%Y')))
