class Mysql:
    def __init__(self, parent):
        self.parent = parent

    def execute(self, template, values = ()):
        self.parent.cursor.execute(template, values)

    def quote_ident(self, str, errors="strict"):
        # unless it's in ANSI mode
        return '`' + str + '`'