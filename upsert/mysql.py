class Mysql:
    def __init__(self, controller):
        self.controller = controller

    def execute(self, template, values = ()):
        self.controller.cursor.execute(template, values)

    def quote_ident(self, str, errors="strict"):
        # unless it's in ANSI mode
        return '`' + str + '`'