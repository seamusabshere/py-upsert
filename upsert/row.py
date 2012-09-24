import copy
from collections import OrderedDict

class Row:
    def __init__(self, parent, selector, setter):
        self.parent = parent
        setter = copy.copy(setter)
        for missing in (set(selector.keys()) - set(setter.keys())):
            setter[missing] = selector[missing]
        self.selector = OrderedDict(sorted(selector.items(), key=lambda t: t[0]))
        self.setter = OrderedDict(sorted(setter.items(), key=lambda t: t[0]))
