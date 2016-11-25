from sqlite3 import connect
from SQLiteHelper import Query
from utilities import filepath
from sys import argv, version_info

# file_name = 'Sec12'
# if platform == 'win32':file_drive, file_path = '%s:'%'C', sep.join(('dbTable', 'sqlite3'))
# if platform == 'linux-armv7l':file_drive, file_path = '', sep.join(('mnt', 'sdcard', 'data', 'sqlite3'))
# if platform in ('linux', 'linux2'):file_drive, file_path = '', sep.join(('data', 'data', 'com.termux', 'files', 'home', 'data', 'sqlite3'))
fullpath = filepath('Sec12')

class Equities(Query):
    def __init__(self, connection=connect(fullpath)):
        self.__table, self.__conn = 'equities', connection
        self.__cur, self.__qu = self.__conn.cursor(), Query(self.__conn)

    def __del__(self):
        self.__table = self.__conn = self.__cur = self.__qu = None
        del(self.__table)
        del(self.__qu)
        del(self.__cur)
        del(self.__conn)

    def append(self, **args):
        vDict, fnameList = {}, ('code', 'name', 'lot')
        if type(args['values']) is tuple:
           k = 0
           for f in fnameList:
               vDict['%s'%f] = args['values'][k]
               k += 1
        if 'optional' in args:
            if 'options' in args['optional']:
                vDict['options'], vDict['abbrev'] = True, args['optional']['options']
                if 'futures' in args['optional']:Dict['short'], vDict['futures'] = True, True
                elif 'short' in args['optional']:vDict['short'] = True
            if 'gem' in args['optional']:
                vDict['gem']= True
                if 'short' in args['optional']:vDict['short'] = True
        return self.__qu.insert(self.__table, values=vDict)

    def amend(self, **args):
        if 'criteria' in args:
            if ('values' in args) and (type(args['values']) is dict):
                if type(args['criteria']) is dict:return self.__qu.amend(self.__table, values=args['values'], criteria=args['criteria'])

    def show(self, **args):
        if 'fields' in args:
            if 'criteria' in args:return self.__qu.show(self.__table, fields=args['fields'], criteria=args['criteria'])
            else:return self.__qu.show(self.__table, fields=args['fields'])
        elif 'criteria' in args: return self.__qu.show(self.__table, criteria=args['criteria'])
        else:return self.__qu.show(self.__table)

if __name__ == "__main__":
    e = Equities()
    if version_info[0] == 2:confirm = raw_input('(I)nsert / (S)ummary: ')
    if version_info[0] == 3:confirm = input('(I)nsert / (S)ummary: ')
    if confirm.upper() == 'S':
        res = "# of equities with 'Futures' transaction: %i\n" % len(e.show(criteria={'futures':1}))
        res += "# of equities with 'Options' transaction: %i\n" % len(e.show(criteria={'options':1}))
        res += "# of equities permit 'Short Sell': %i\n\n" % len(e.show(criteria={'short':1}))
        res += "# of equities on 'GEM' board: %i\n\n" % len(e.show(criteria={'gem':1}))
        res += "Total: %i record(s)" % len(e.show())
        print(res)
    elif confirm.upper() == 'I':
        if version_info[0] == 2:code, name, lots = raw_input('Stock code: '), raw_input('Stock name: '), raw_input('Lot size: ')
        if version_info[0] == 3:code, name, lots = input('Stock code: '), input('Stock name: '), input('Lot size: ')
        if code and name and lots:
            e.append(values=(code, name, int(lots)))
