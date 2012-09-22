from upsert.ansi_ident import AnsiIdent
from upsert.sqlite3 import Sqlite3

class Upsert:
    """
    TBD the description oh yah
    """

    def __init__(self, cursor, table_name):
        self.cursor = cursor
        self.table_name = table_name
        self.implementation = Upsert.implementations[str(type(cursor))](self)

    # Thinking in Python: Fronting for an Implementation
    def __getattr__(self, name):
        return getattr(self.implementation, name)

    def row(self, selector, setter = None):
        if setter is None:
            setter = {}
        self.buffer.append(self.row_class(self, selector, setter))
        self.ready()
        return None
    
    def fill_ident_placeholders(self, template, idents):
        quoted = tuple(self.quote_ident(str) for str in idents)
        return template % quoted

    sqlite3_key =     "<type 'sqlite3.Cursor'>"
    # mysql_key =       "<class 'MySQLdb.connections.Connection'>"
    # postgresql_key =  "<type 'psycopg2._psycopg.connection'>"

    implementations = {
        sqlite3_key:    Sqlite3,
        # mysql_key:      Mysql,
        # postgresql_key: Postgresql
    }
