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
            ks, es = k.split('.'), "getattr(__import__('%s')," % k
            for i in ks[1:]:es = "getattr(" + es + "'%s')," % i
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

def him(*args):
    if args: __, iml = {}, args[0]
    if isinstance(iml, dict):
        _ = eval("encoder(%s)"%iml)
        for k, v in list(_.items()):
            if k not in list(__.keys()): __[k] = v
    elif isinstance(iml, list):
        for i in iml:
            if len(i) == 1: _ = eval("encoder(%s)"%i)
            if len(i) == 2: _ = eval("encoder(%s,%s)"%i)
            if isinstance(i, dict): _ = eval("encoder(%s)"%i)
            for k, v in list(_.items()):
                if k not in list(__.keys()): __[k] = v
    return __
