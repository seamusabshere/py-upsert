import copy
from collections import OrderedDict

class Row:
    def __init__(self, controller, selector, setter):
        self.controller = controller
        full_setter = copy.copy(selector)
        full_setter.update(setter)
        self.selector = OrderedDict(sorted(selector.items(), key=lambda t: t[0]))
        self.setter = OrderedDict(sorted(full_setter.items(), key=lambda t: t[0]))
