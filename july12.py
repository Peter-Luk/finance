e = getattr(__import__('handy'), 'encoder')
rim = {'bokeh.io': ('output_notebook','show'), 'bt': ('LF',)}
res = {}
for k, v in list(rim.items()):
    ps, kl, i, xtr, ttr, tstr = '%s=', k.split('.'), 0, 'getattr(', ", '%s')", ''
    while i < len(kl):
        ps += xtr
        tstr += ttr
        i += 1
    ps += "__import__('%s')" + tstr
    _kt = tuple(kl)
    for i in v:
#         _kt = (i,) + _kt + (i,)
        exec(ps % ((i,) + _kt + (i,)))
        res[i] = eval('%s'%i)
