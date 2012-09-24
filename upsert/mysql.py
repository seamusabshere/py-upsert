import upsert

class Mysql:
    row_class = upsert.Row

    def __init__(self, controller):
        self.controller = controller
        self.buffer = []

    def ready(self):
        if len(self.buffer) == 0:
            return
        first_row = self.buffer.pop()
        selector = first_row.selector
        setter = first_row.setter

        pi = ','.join(['%s']*len(setter))
        pv = ','.join(['?']*len(setter))
        a = 'INSERT IGNORE INTO %s (' + pi + ') VALUES ( ' + pv + ')'
        b = [self.controller.table_name] + setter.keys()
        t = self.controller.fill_ident_placeholders(a, b)
        vv = setter.values()
        self.execute(t, vv)
        
        selector_piv = ' AND '.join(['%s=?']*len(selector))
        setter_piv = ','.join(['%s=?']*len(setter))
        a = 'UPDATE %s SET ' + setter_piv + ' WHERE ' + selector_piv
        b = [self.controller.table_name] + setter.keys() + selector.keys()
        t = self.controller.fill_ident_placeholders(a, b)
        vv = setter.values() + selector.values()
        self.execute(t, vv)

    # expects placeholders as ?
    def execute(self, template, values = ()):
        template = template.replace('?', '%s')
        self.controller.cursor.execute(template, values)

    def quote_ident(self, str, errors="strict"):
        # unless it's in ANSI mode
        return '`' + str + '`'
