him = getattr(__import__('handy'), 'him')
__ = him({'datetime':('datetime',),'utilities':('today', 'filepath', 'mtf'), 'os':('sep',), 'bt':('LF',)})
for _ in list(__.keys()): exec("%s=__['%s']" % (_, _))
for _ in mtf():
    trday, dstr = lf(_).fp.trade_day, ''
    pdate = datetime.strptime(trday[-1], '%Y-%m-%d').date()
    if (today.date() in [datetime.strptime(__, '%Y-%m-%d').date() for __ in trday]):
        pdate = today.date()
        if today.hour < 13: dstr = 'm'
    eval("getattr(lf('%s'), 'plot')(filepath('%sbt%02i%s.html', type=sep.join(('data', 'plot')), subpath=sep.join(('%i','%s'))))" % (_, _[0].lower(), pdate.day, dstr, pdate.year, pdate.strftime('%B')))
    st = 'full'
    if dstr == 'm': st = 'morning'
    print("%s (%s %s session) plotted." % (_, pdate.strftime('%d-%m-%Y'), st))
