import codecs

class AnsiIdent:
    # http://stackoverflow.com/questions/6514274/how-do-you-escape-strings-for-sqlite-table-column-names-in-python
    def quote_ident(self, str, errors="strict"):
        encodable = str.encode("utf-8", errors).decode("utf-8")
        nul_index = encodable.find("\x00")
        if nul_index >= 0:
            error = UnicodeEncodeError("NUL-terminated utf-8", encodable, nul_index, nul_index + 1, "NUL not allowed")
            error_handler = codecs.lookup_error(errors)
            replacement, _ = error_handler(error)
            encodable = encodable.replace("\x00", replacement)
        return '"' + encodable.replace('"', '""') + '"'

class Mysql:
    def __init__(self, parent):
        self.parent = parent

    def execute(self, template, values = ()):
        cur = self.parent.connection.cursor()
        cur.execute(template, values)

    def quote_ident(self, str, errors="strict"):
        # unless it's in ANSI mode
        return '`' + str + '`'

class Postgresql(AnsiIdent):
    def __init__(self, parent):
        self.parent = parent

    def execute(self, template, values = ()):
        cur = self.parent.connection.cursor()
        cur.execute(template, values)

class Sqlite3(AnsiIdent):
    def __init__(self, parent):
        self.parent = parent

    # so is this following DB-API and the others are breaking it?
    def execute(self, template, values = ()):
        template = template.replace('%s', '?')
        cur = self.parent.connection.cursor()
        cur.execute(template, values)

class Upsert:
    """
    TBD the description oh yah
    """

    def __init__(self, connection, table_name):
        self.connection = connection
        self.table_name = table_name
        self.implementation = Upsert.implementations[str(type(connection))](self)

    # Thinking in Python: Fronting for an Implementation
    def __getattr__(self, name):
        return getattr(self.implementation, name)

    def row(self, selector):
        values = (selector.values()[0],)
        template = self.fill_ident_placeholders('INSERT INTO %s (%s)', (self.table_name, selector.keys()[0]))
        self.execute((template + ' VALUES (%s) '), values)
    
    def fill_ident_placeholders(self, template, idents):
        quoted = tuple(self.quote_ident(str) for str in idents)
        return template % quoted

    sqlite3_key =     "<type 'sqlite3.Connection'>"
    mysql_key =       "<class 'MySQLdb.connections.Connection'>"
    postgresql_key =  "<type 'psycopg2._psycopg.connection'>"

    implementations = {
        sqlite3_key:    Sqlite3,
        mysql_key:      Mysql,
        postgresql_key: Postgresql
    }
