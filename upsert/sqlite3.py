import upsert

class Row:
    def __init__(self, parent, selector, setter):
        self.parent = parent
        self.selector = selector
        self.setter = setter
        for missing in (set(self.selector.keys()) - set(self.setter.keys())):
            self.setter[missing] = self.selector[missing]

class Sqlite3(upsert.AnsiIdent):
    row_class = Row

    def __init__(self, parent):
        self.parent = parent
        self.buffer = []

    def ready(self):
        if len(self.buffer) == 0:
            return
        first_row = self.buffer.pop()
        selector = first_row.selector
        setter = first_row.setter

        pi = ','.join(['%s']*len(setter))
        pv = ','.join(['?']*len(setter))
        a = 'INSERT OR IGNORE INTO %s (' + pi + ') VALUES ( ' + pv + ')'
        b = [self.parent.table_name] + setter.keys()
        t = self.parent.fill_ident_placeholders(a, b)
        vv = setter.values()
        self.execute(t, vv)
        
        selector_piv = ' AND '.join(['%s=?']*len(selector))
        setter_piv = ','.join(['%s=?']*len(setter))
        a = 'UPDATE %s SET ' + setter_piv + ' WHERE ' + selector_piv
        b = [self.parent.table_name] + setter.keys() + selector.keys()
        t = self.parent.fill_ident_placeholders(a, b)
        vv = setter.values() + selector.values()
        self.execute(t, vv)

    # so is this following DB-API and the others are breaking it?
    def execute(self, template, values = ()):
        self.parent.cursor.execute(template, values)
