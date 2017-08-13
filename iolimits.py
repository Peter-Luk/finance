him = getattr(__import__('handy'), 'him')
__ = him({'utilities':('mtf','waf'), 'pt_2':('PI',), 'sys':('version_info',)})
for _ in list(__.keys()): exec("%s=__['%s']" % (_,_))

if __name__ == "__main__":
    confirm = 'Y'
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
        try: pi(code).ds()
        except: print("Only for local futures (current or next calender) month")
        if version_info.major == 2: confirm = raw_input("Proceed (Y)es/(N)o: ")
        if version_info.major == 3: confirm = input("Proceed (Y)es/(N)o: ")
