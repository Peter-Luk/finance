him = getattr(__import__('handy'),'him')
__ = him({'utilities':('mtf',),'bt':('LF',)})
for _ in list(__.keys()):exec("%s=__['%s']"%(_,_))
fit = 'mhi'
pf = mtf(fit)
mpd = getattr(lf(pf[-1]),'fp')
print('%s: (latest @ %s)'%(pf[-1],mpd.trade_day[-1]))
print(getattr(mpd,'xfinder')('d'))
print(getattr(mpd,'xfinder')('r'))
ar, mos = getattr(mpd,'fdc')('b'), getattr(mpd,'ltdmos')('a')
lc, cs = ar['Close'].values[-1], ar['Close'].std()
il = list(filter(lambda _:(_>lc-cs) and (_<lc+cs),mos))
ol = list(filter(lambda _:(_<lc-cs) or (_>lc+cs),mos))
print(il,lc,ol)
