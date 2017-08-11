him = getattr(__import__('handy'), 'him')
__ = him({'utilities':('mtf',), 'bt':('LF',), 'os':('linesep',)})
for _ in list(__.keys()): exec("%s=__['%s']" % (_,_))
fit = 'mhi'
pf = mtf(fit)
mpd = getattr(lf(pf), 'fp')
ai, ar, mos = getattr(mpd, 'fdc')('i'), getattr(mpd, 'fdc')('b'), getattr(mpd, 'ltdmos')('a')
print('%s: (latest @ %s)' % (pf, mpd.trade_day[-1]))
xd = getattr(mpd, 'xfinder')('d')
xr = getattr(mpd, 'xfinder')('r')
try:
    dtxd = xd.transpose().to_dict()
    for _ in list(dtxd.keys()):
        print("%s: delta @ %i (mean %i)" % (dtxd[_]['Date'].strftime('%d-%m-%Y'), dtxd[_]['Delta'], ar['Delta'].mean()))
except:pass
try:
    dtxr = xr.transpose().to_dict()
    for _ in list(dtxr.keys()):
        print("%s: RSI @ %.3f (mean %.3f)" % (dtxr[_]['Date'].strftime('%d-%m-%Y'), dtxr[_]['RSI'], ai['RSI'].mean()))
except:pass
lv, vm, vs = ar['Volume'].values[-1], ar['Volume'].mean(), ar['Volume'].std()
print("%sVolume over mean: %.2f%%" % (linesep, lv / vm* 100.))
print("Volume over (mean + std): %.2f%%" % (lv / (vm +vs) * 100.))
lc, cs = ar['Close'].values[-1], ar['Close'].std()
il = list(filter(lambda _:(_ > lc - cs) and (_ < lc + cs), mos))
ol = list(filter(lambda _:(_ < lc - cs) or (_ > lc + cs), mos))
print('%sLast Close: %i' % (linesep, lc))
print('%sWithin statistical range:' % linesep, il)
ml = list(filter(lambda _:_ > lc, ol))
csl = list(filter(lambda _:_ not in ml, ol))
print('%sMoon shot:' % linesep, ml)
print('China syndrome:', csl)
