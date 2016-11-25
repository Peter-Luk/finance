class Query:
    def __init__(self, connection):
        self.__conn = connection
        self.__cur = self.__conn.cursor()

    def __del__(self):
        self.__cur = self.__conn = None
        del(self.__cur)
        del(self.__conn)

    def avail_tables(self):
        self.res = self.__cur.execute("SELECT * FROM sqlite_master").fetchall()
        t_name = ()
        for r in self.res:t_name += (r[1],)
        return t_name

    def avail_fields(self, table):
        self.res = self.__cur.execute("SELECT * FROM sqlite_master").fetchall()
        for r in self.res:
            if r[1] == table:
                raw_fields = r[4].split('(')[1:][0][:-1].split(', ')
                fields = {}
                for rf in raw_fields:
                    temp = rf.split(' ')
                    type_str = temp[1]
                    if type_str == 'TEXT':type_str = 'STRING'
                    if not(' '.join(temp[-2:]) == 'PRIMARY KEY'):fields[temp[0]] = type_str
                return fields

    def amend(self, table, **args):
        if ('criteria' in args) and ('value' in args):
            cStr, vStr, criteria, value =  '', '', args['criteria'], args['value']
            for c in criteria.keys():
                if type(criteria[c]) is int:cStr = ' AND '.join((cStr, "%s=%i" % (c, criteria[c])))
                elif type(criteria[c]) is float:cStr = ' AND '.join((cStr, "%s=%0.4f" % (c, criteria[c])))
                else:cStr = ' AND '.join((cStr, "%s='%s'" % (c, criteria[c])))
            condStr = ' '.join(('WHERE', cStr[5:]))
            no_affected = self.__cur.execute(' '.join(("SELECT COUNT(*) FROM", table, condStr))).fetchone()[0]
            if no_affected > 0:
                for v in value.keys():
                    if type(value[v]) is int:vStr = ', '.join((vStr, "%s=%i" % (v, value[v])))
                    elif type(value[v]) is float:vStr = ', '.join((vStr, "%s=%0.4f" % (v, value[v])))
                    else:vStr = ', '.join((vStr, "%s='%s'" % (v, value[v])))
                valueStr = ' '.join(('SET', vStr[2:]))
                try:
                    self.__cur.execute(' '.join(('UPDATE', table, valueStr, condStr)))
                    self.__conn.commit()
                except:pass

    def delete(self, table, **args):
        if 'criteria' in args:
            cStr, criteria, afr = '', args['criteria'], self.avail_fields(table)
            ckeys, af = criteria.keys(), afr.keys()
            ckeys = filter(lambda x:x in af, ckeys)
            for c in ckeys:
                if type(criteria[c]) is int:cStr = ' AND '.join((cStr, "%s=%i" % (c, criteria[c])))
                elif type(criteria[c]) is float:cStr = ' AND '.join((cStr, "%s=%0.4f" % (c, criteria[c])))
                else:cStr = ' AND '.join((cStr, "%s='%s'" * (c, criteria[c])))
            condStr = ' '.join(('WHERE', cStr[5:]))
            try:
                self.__cur.execute(' '.join(('DELETE FROM', table, condStr)))
                self.__conn.commit()
            except:pass

    def insert(self, table, **args):
        if 'values' in args:
            nStr, vStr, vkeys = '', '', args['values'].keys()
            afr = self.avail_fields(table)
            af = afr.keys()
            vkeys = filter(lambda x:x in af, vkeys)
            for v in vkeys:
                nStr = ', '.join((nStr, v))
                if afr[v] == 'INTEGER':vStr = ', '.join((vStr, '%i'%args['values'][v]))
                elif afr[v] == 'FLOAT':vStr = ', '.join((vStr, '%f'%args['values'][v]))
                else:vStr = ', '.join((vStr,"'%s'"%args['values'][v]))
            try:
                self.__cur.execute('INSERT INTO %s (%s) VALUES (%s)'%(table, nStr[2:], vStr[2:]))
                self.__conn.commit()
            except:pass

    def show(self, table, **args):
        fStr, cStr, oStr, condStr = '', '', '', ''
        af = self.avail_fields(table).keys()
        try:fields = args['fields']
        except:fields = af
        fields = filter(lambda x:x in af, fields)
        for f in fields:fStr = ', '.join((fStr, f))
        if 'criteria' in args:
            cf = args['criteria'].keys()
            cf = filter(lambda x:x in af, cf)
            for k in cf:
                 if type(args['criteria'][k]) is int:cStr = ' AND '.join((cStr, "%s=%i" % (k, args['criteria'][k])))
                 elif type(args['criteria'][k]) is float:cStr = ' AND '.join((cStr, "%s=%0.4f" % (k, args['criteria'][k])))
                 else:cStr = ' AND '.join((cStr, "%s='%s'" % (k, args['criteria'][k])))
            condStr = ' '.join(('WHERE', cStr[5:]))
        if 'order' in args:
            for o in args['order']:oStr = ', '.join((oStr, o))
            return self.__cur.execute(' '.join(("SELECT", fStr[2:], 'FROM', table, condStr, "ORDER BY", oStr[2:]))).fetchall()
        else:return self.__cur.execute(' '.join(("SELECT", fStr[2:], 'FROM', table, condStr))).fetchall()
