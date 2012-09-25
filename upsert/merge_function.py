import upsert
import hashlib

class MergeFunction:
    @classmethod
    @upsert.memoize
    def lookup(cls, controller, selector_keys, setter_keys):
        return cls(controller, selector_keys, setter_keys)

    @classmethod
    def name(cls, controller, selector_keys, setter_keys):
        uniq = '_'.join([controller.table_name, 'SEL' ] + selector_keys + ['SET'] + setter_keys)
        if (len(uniq) + 7) > 63:
            md5 = hashlib.md5(uniq).hexdigest()
            uniq = uniq[0:63-16-7] + md5[0:16]
        return 'upsert_' + uniq

    def __init__(self, controller, selector_keys, setter_keys):
        self.controller = controller
        self.selector_keys = list(selector_keys)
        self.setter_keys = list(setter_keys)
        self.name = self.__class__.name(controller, self.selector_keys, self.setter_keys)
        self.call_template = self.__class__.call_template(controller, self.name, self.selector_keys, self.setter_keys)
        self.create_or_replace()

    def create_or_replace(self):
        # print '[upsert] Creating or replacing function {0}'.format(self.name)
        self.drop()
        self.create()
