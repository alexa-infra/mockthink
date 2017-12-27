from pprint import pprint


class NotInScopeErr(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__()

class Scope(object):
    def __init__(self, values=None):
        self.values = values or {}
        self.parent = None

    def _get_sym(self, key):
        if key in self.values:
            return self.values[key]
        if self.parent:
            return self.parent._get_sym(key)
        return None

    def has_sym(self, key):
        if key in self.values:
            return True
        if self.parent:
            return self.parent.has_sym(key)
        return False

    def get_sym(self, key):
        if not self.has_sym(key):
            msg = "symbol not defined: %s" % key
            raise NotInScopeErr(msg)
        return self._get_sym(key)

    def push(self, vals):
        scope = Scope(vals)
        scope.parent = self
        return scope

    def get_flattened(self):
        vals = {k: v for k, v in self.values.items()}
        if not hasattr(self, 'parent'):
            return vals
        parent_vals = self.parent.get_flattened()
        parent_vals.update(vals)
        return parent_vals

    def log(self):
        pprint(self.get_flattened())
