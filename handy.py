def encoder(*args, **kwargs):
    res, case = {}, 'lower'
    try:
        if args:mfp = args[0]
        if 'mfp' in list(kwargs.keys()): mfp = kwargs['mfp']
        if 'case' in list(kwargs.keys()): case = kwargs['case']
        if 'alias' in list(kwargs.keys()):
            if not isinstance(kwargs['alias'],dict): la = kwargs['alias']
            if isinstance(kwargs['alias'], str):
                la = []
                la.append(kwargs['alias'])
            elif isinstance(kwargs['alias'], tuple): la = list(kwargs['alias'])
        for k, v in list(mfp.items()):
            ks, es = k.split('.'), "getattr(__import__('%s'),"%k
            if len(ks) > 1:es = "getattr(" + es + "'%s'),"%ks[-1]
            if v:
                for i in v:
                    _i = i
                    if case == 'lower': _i = i.lower()
                    if case == 'upper': _i = i.upper()
                    if case == 'capitalize': _i = i.capitalize()
                    try: _i = alias[i]
                    except: pass
                    exec("%s=%s'%s')" % (_i, es, i))
                    try:
                        _a = la.pop()
                        res[_a] = eval('%s'%_i)
                    except: res[_i] = eval('%s'%_i)
            else:
                _k = k
                exec("%s=__import__('%s')" % (_k, _k))
                try: _k = la.pop()
                except: pass
                res[_k] = eval('%s'%k)
    except: pass
    return res
