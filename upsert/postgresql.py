class Postgresql(AnsiIdent):
    def __init__(self, controller):
        self.controller = controller

    def execute(self, template, values = ()):
        self.controller.cursor.execute(template, values)
