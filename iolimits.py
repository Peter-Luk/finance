e = getattr(__import__('handy'),'encoder')
__ = e({'utilities':('mtf',),'bt':('LF',)})
for _ in list(__.keys()):exec("%s=__['%s']"%(_,_))
pf = mtf()
mpd = getattr(lf(pf[-1]),'fp')
print('%s: '%pf[-1])
print(getattr(mpd,'xfinder')('d'))
print(getattr(mpd,'xfinder')('r'))
ar, mos = getattr(mpd,'fdc')('b'), getattr(mpd,'ltdmos')('a')
lc, cs = ar['Close'].values[-1], ar['Close'].std()
il = list(filter(lambda _:(_>lc-cs) and (_<lc+cs),mos))
ol = list(filter(lambda _:(_<lc-cs) or (_>lc+cs),mos))
print(il,lc,ol)

