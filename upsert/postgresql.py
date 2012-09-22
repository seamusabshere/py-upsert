class Postgresql(AnsiIdent):
    def __init__(self, parent):
        self.parent = parent

    def execute(self, template, values = ()):
        self.parent.cursor.execute(template, values)
