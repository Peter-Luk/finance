e = getattr(__import__('handy'), 'encoder')
__ = e({'utilities':('today', 'waf','filepath'), 'os':('sep',), 'bt':('LF',)})
for _ in list(__.keys()): exec("%s=__['%s']" % (_, _))
dstr = ''
if today.hour < 13:dstr='m'
for _ in waf()[:2]: eval("getattr(lf('%s'),'plot')(filepath('%sbt%02i%s.html', type=sep.join(('data','plot')), subpath=sep.join(('%i','%s'))))" % (_, _[0].lower(), today.day, dstr, today.year, today.strftime('%B')))
# for _ in waf()[:2]: eval("getattr(lf('%s'),'plot')('plot/%sbt%s.html')" % (_, _[0].lower(), today.strftime('%d%m')))
# if __name__ == '__main__': pass
