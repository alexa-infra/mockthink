from rethinkdb import (RqlCompileError, RqlRuntimeError,
                       ReqlNonExistenceError)

from . import util
from .util import GroupResults


class AttrHaving: # pylint: disable=too-few-public-methods
    def __init__(self, attrs):
        for k, v in attrs.items():
            setattr(self, k, v)


# #################
#   Base classes
# #################

class RBase:
    def __init__(self, *args, **kwargs): # pylint: disable=unused-argument
        self.optargs = kwargs.get('optargs', {})

    def find_table_scope(self):
        # pylint: disable=no-member
        result = None
        if hasattr(self, 'left'):
            result = self.left.find_table_scope()
        return result

    def find_db_scope(self):
        # pylint: disable=no-member
        result = None
        if hasattr(self, 'left'):
            result = self.left.find_db_scope()
        return result

    def has_table_scope(self):
        result = None
        for part in ('left', 'middle', 'right'):
            if hasattr(self, part):
                result = getattr(self, part).has_table_scope()
                if result:
                    break
        return result

    def find_index_func_for_scope(self, index_name, db_arg):
        db_scope = self.find_db_scope()
        table_scope = self.find_table_scope()
        func = db_arg.get_index_func_in_table_in_db(
            db_scope,
            table_scope,
            index_name
        )
        is_multi = db_arg.is_multi_index(
            db_scope, table_scope, index_name
        )
        return func, is_multi

    def raise_rql_runtime_error(self, msg): # pylint: disable=no-self-use
        # temporary jankiness to get it working
        # doing it this way means error messages won't
        # be properly printed
        term = AttrHaving({
            'args': (),
            'optargs': {},
            'compose': (lambda x, y: 'COMPOSED')
        })
        raise RqlRuntimeError(msg, term, [])

    def raise_rql_compile_error(self, msg): # pylint: disable=no-self-use
        term = AttrHaving({
            'args': (),
            'optargs': {},
            'compose': (lambda x, y: 'COMPOSED')
        })
        raise RqlCompileError(msg, term, [])

    def raise_rql_not_found_error(self, msg): # pylint: disable=no-self-use
        term = AttrHaving({
            'args': (),
            'optargs': {},
            'compose': (lambda x, y: 'COMPOSED')
        })
        raise ReqlNonExistenceError(msg, term, [])

class RDatum(RBase):
    def __init__(self, val, *args, **kwargs):
        self.val = val
        super().__init__(*args, **kwargs)

    def __str__(self):
        return "<DATUM: %s>" % self.val

    def run(self, arg, scope): # pylint: disable=unused-argument
        return self.val

class RFunc(RBase):
    def __init__(self, param_names, body, *args, **kwargs):
        self.param_names = param_names
        self.body = body
        super().__init__(*args, **kwargs)

    def has_table_scope(self):
        return self.body.has_table_scope()

    def __str__(self):
        params = ", ".join(str(x) for x in self.param_names)
        return "<RFunc: [%s] { %s }>" % (params, self.body)

    def run(self, args, context, scope):
        if not isinstance(args, list):
            args = [args]
        bound = util.as_obj(zip(self.param_names, args))
        call_scope = scope.push(bound)
        return self.body.run(context, call_scope)

class MonExp(RBase):
    def __init__(self, left, *args, **kwargs):
        self.left = left
        super().__init__(*args, **kwargs)

    def __str__(self):
        class_name = self.__class__.__name__
        return "<%s: %s>" % (class_name, self.left)

    def do_run(self, left, arg, scope):
        raise NotImplementedError("method do_run not defined in class %s" % self.__class__.__name__)

    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        if isinstance(left, GroupResults):
            ret = GroupResults()
            for k, v in left.items():
                ret[k] = self.do_run(v, arg, scope)
            return ret
        return self.do_run(left, arg, scope)


class BinExp(RBase):
    def __init__(self, left, right, *args, **kwargs):
        self.left = left
        self.right = right
        super().__init__(*args, **kwargs)

    def __str__(self):
        class_name = self.__class__.__name__
        return "<%s: (%s, %s)>" % (class_name, self.left, self.right)

    def do_run(self, left, right, arg, scope):
        raise NotImplementedError("method do_run not defined in class %s" % self.__class__.__name__)

    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        right = self.right.run(arg, scope)
        if isinstance(left, GroupResults):
            ret = GroupResults()
            for k, v in left.items():
                ret[k] = self.do_run(v, right, arg, scope)
            return ret
        return self.do_run(left, right, arg, scope)


class Ternary(RBase):
    def __init__(self, left, middle, right, *args, **kwargs):
        self.left = left
        self.middle = middle
        self.right = right
        super().__init__(*args, **kwargs)

    # pylint: disable=too-many-arguments
    def do_run(self, left, middle, right, arg, scope):
        raise NotImplementedError("method do_run not defined in class %s" % self.__class__.__name__)

    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        middle = self.middle.run(arg, scope)
        right = self.right.run(arg, scope)
        if isinstance(left, GroupResults):
            ret = GroupResults()
            for k, v in left.items():
                ret[k] = self.do_run(v, middle, right, arg, scope)
            return ret
        return self.do_run(left, middle, right, arg, scope)

class ByFuncBase(RBase):
    def __init__(self, left, right, *args, **kwargs):
        self.left = left
        self.right = right
        super().__init__(*args, **kwargs)

    def do_run(self, left, map_fn, arg, scope):
        raise NotImplementedError("method do_run not defined in class %s" % self.__class__.__name__)

    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        map_fn = lambda x: self.right.run(x, arg, scope)
        if isinstance(left, GroupResults):
            ret = GroupResults()
            for k, v in left.items():
                ret[k] = self.do_run(v, map_fn, arg, scope)
            return ret
        return self.do_run(left, map_fn, arg, scope)

class MakeObj(RBase):
    def __init__(self, vals, *args, **kwargs):
        self.vals = vals
        super().__init__(*args, **kwargs)

    def run(self, arg, scope):
        out = {}
        for k, v in self.vals.items():
            if isinstance(v, RFunc):
                out[k] = v
            else:
                out[k] = v.run(arg, scope)
        return out

class MakeArray(RBase):
    def __init__(self, vals, *args, **kwargs):
        self.vals = vals
        super().__init__(*args, **kwargs)

    def run(self, arg, scope):
        out = []
        for elem in self.vals:
            if isinstance(elem, RFunc):
                out.append(elem)
            else:
                out.append(elem.run(arg, scope))
        return out

class LITERAL_OBJECT(dict):
    @staticmethod
    def from_dict(a_dict):
        out = LITERAL_OBJECT()
        for k, v in a_dict.items():
            out[k] = v
        return out

class LITERAL_LIST(list):
    @staticmethod
    def from_list(a_list):
        return LITERAL_LIST([elem for elem in a_list])

def contains_literals(to_check):
    if is_literal(to_check):
        return True
    elif isinstance(to_check, dict):
        for v in to_check.values():
            if is_literal(v) or contains_literals(v):
                return True
        return False
    elif isinstance(to_check, list):
        for v in to_check:
            if is_literal(v) or contains_literals(v):
                return True
        return False
    return False

def has_nested_literal(to_check):
    if isinstance(to_check, LITERAL_OBJECT):
        for v in to_check.values():
            if contains_literals(v):
                return True
    elif isinstance(to_check, dict):
        for v in to_check.values():
            if has_nested_literal(v):
                return True
    elif isinstance(to_check, LITERAL_LIST):
        for v in to_check:
            if contains_literals(v):
                return True
    elif isinstance(to_check, list):
        for v in to_check:
            if has_nested_literal(v):
                return True
    return False

def is_literal(x):
    return isinstance(x, (LITERAL_OBJECT, LITERAL_LIST))


def rql_merge_with(ext_with, to_extend):
    out = {}
    out.update(to_extend)
    if is_literal(ext_with):
        if has_nested_literal(ext_with):
            raise RqlRuntimeError('No nested r.literal()!')

    for k, v in ext_with.items():
        if is_literal(v):
            if has_nested_literal(v):
                raise RqlRuntimeError('No nested r.literal()!')

        if k not in to_extend:
            out[k] = util.clone(v)
        else:
            d1_val = to_extend[k]
            if is_literal(v):
                out[k] = util.clone(v)
            elif isinstance(d1_val, dict) and isinstance(v, (dict, LITERAL_OBJECT)):
                out[k] = rql_merge_with(v, d1_val)
            elif isinstance(d1_val, list) and isinstance(v, (list, LITERAL_LIST)):
                out[k] = util.cat(d1_val, v)
            else:
                out[k] = util.clone(v)
    return out

def rql_merge_with_pred(ext_with):
    def handler(to_extend):
        return rql_merge_with(ext_with, to_extend)
    return handler
