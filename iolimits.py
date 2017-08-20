him = getattr(__import__('handy'), 'him')
__ = him([{'utilities':('mtf','waf'), 'pt_2':('PI',), 'sys':('version_info',)},({'multiprocessing':('Queue','Process','Pool')}, "case='capitalize'")])
for _ in list(__.keys()): exec("%s=__['%s']" % (_,_))

# def f(x):
    # q.put(pi(x).ds(programmatic=False))

if __name__ == "__main__":
    confirm = 'Y'
    # dmp, q = {}, Queue()
    # for _ in waf():
        # p = Process(target=f, name=_, args=(_,))
        # p.start()
        # dmp[_] = q.get()
        # p.join()
    # p = Pool(4)
    # res = p.map(f, waf())

    while confirm.upper() != 'N':
        code = ''
        if version_info.major == 2: ct = raw_input("Futures (C)ode/(T)ype: ")
        if version_info.major == 3: ct = input("Futures (C)ode/(T)ype: ")
        if ct.upper() == 'C':
            if version_info.major == 2: contract = raw_input('Contract: ')
            if version_info.major == 3: contract = input('Contract: ')
            if contract.upper() in waf(): code = contract.upper()
        elif ct.upper() == 'T':
            if version_info.major == 2: tp = raw_input('Type (H)SI/(M)HI: ')
            if version_info.major == 3: tp = input('Type (H)SI/(M)HI: ')
            if tp.upper() == 'H': code = mtf('hsi')
            elif tp.upper() == 'M': code = mtf('mhi')
        try: print(pi(code).ds(programmatic=False))
        # try: print(dmp[code])
        # try: print(res[waf().index(code)])
        except: print("Only for local futures (current or next calender) month")
        if version_info.major == 2: confirm = raw_input("Proceed (Y)es/(N)o: ")
        if version_info.major == 3: confirm = input("Proceed (Y)es/(N)o: ")
