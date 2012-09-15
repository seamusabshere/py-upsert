import codecs

class Upsert:
    """
    TBD the description oh yah
    """

    def __init__(self, connection, table_name):
        self.connection = connection
        self.table_name = table_name

    def row(self, selector):
        values = (selector.values()[0],)
        template = self.fill_ident_placeholders('INSERT INTO %s (%s)', (self.table_name, selector.keys()[0]))
        self.execute((template + ' VALUES (%s) '), values)

    def execute(self, template, values = ()):
        return Upsert.execute_functions[self.type_key()](self, template, values)

    # http://stackoverflow.com/questions/6514274/how-do-you-escape-strings-for-sqlite-table-column-names-in-python
    def quote_ident(self, str, errors="strict"):
        if self.type_key() == Upsert.mysql_key:
            # unless it's in ANSI mode
            return '`' + str + '`'
        else:
            encodable = str.encode("utf-8", errors).decode("utf-8")
            nul_index = encodable.find("\x00")
            if nul_index >= 0:
                error = UnicodeEncodeError("NUL-terminated utf-8", encodable, nul_index, nul_index + 1, "NUL not allowed")
                error_handler = codecs.lookup_error(errors)
                replacement, _ = error_handler(error)
                encodable = encodable.replace("\x00", replacement)
            return '"' + encodable.replace('"', '""') + '"'
    
    def fill_ident_placeholders(self, template, idents):
        quoted = tuple(self.quote_ident(str) for str in idents)
        return template % quoted

    def type_key(self):
        return str(type(self.connection))

    # so is this following DB-API and the others are breaking it?
    def execute_sqlite3(self, template, values):
        template = template.replace('%s', '?')
        cur = self.connection.cursor()
        cur.execute(template, values)

    def execute_mysql(self, template, values):
        cur = self.connection.cursor()
        cur.execute(template, values)

    def execute_postgresql(self, template, values):
        cur = self.connection.cursor()
        cur.execute(template, values)

    sqlite3_key =     "<type 'sqlite3.Connection'>"
    mysql_key =       "<class 'MySQLdb.connections.Connection'>"
    postgresql_key =  "<type 'psycopg2._psycopg.connection'>"

    execute_functions = {
        sqlite3_key:    execute_sqlite3,
        mysql_key:      execute_mysql,
        postgresql_key: execute_postgresql
    }
