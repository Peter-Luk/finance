def encoder(*args, **kwargs):
    res, case = {}, 'lower'
    try:
        if args:mfp = args[0]
        if 'mfp' in list(kwargs.keys()): mfp = kwargs['mfp']
        if 'case' in list(kwargs.keys()): case = kwargs['case']
        for k, v in list(mfp.items()):
            if i:
                for i in v:
                    _i = i
                    if case == 'lower': _i = i.lower()
                    if case == 'upper': _i = i.upper()
                    if case == 'capitalize': _i = i.capitalize()
                    exec("%s=getattr(__import__('%s'),'%s')" % (_i, k, i))
                    res[_i] = eval('%s'%_i)
            else:
                exec("%s=__import__('%s')" % (k, k))
                res[k] = eval('%s'%k)
    except: pass
    return res
