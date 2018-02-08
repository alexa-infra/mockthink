from collections import defaultdict
from functools import partial, total_ordering


@total_ordering
class MinType(object):
    def __le__(self, other):
        return True

    def __eq__(self, other):
        return (self is other)

@total_ordering
class MaxType(object):
    def __ge__(self, other):
        return True

    def __eq__(self, other):
        return (self is other)

Min = MinType()
Max = MaxType()

none2min = lambda x: Min if x is None else x

class GroupResults(defaultdict):
    def __init__(self, *args, **kwargs):
        super().__init__(lambda: [], *args, **kwargs)

def extend(*dicts):
    out = {}
    for one_dict in dicts:
        out.update(one_dict)
    return out

def clone(x):
    if isinstance(x, dict):
        return obj_clone(x)
    if isinstance(x, list):
        return clone_array(x)
    return x

def cat(*lists):
    out = []
    for one_list in lists:
        out.extend(one_list)
    return out

def append(elem, a_list):
    return cat(a_list, [elem])

def prepend(elem, a_list):
    return cat([elem], a_list)

def splice_at(to_splice, index, a_list):
    return cat(a_list[0:index], to_splice, a_list[index:])

def insert_at(val, index, a_list):
    return splice_at([val], index, a_list)

def change_at(val, index, a_list):
    right_start = index + 1
    return cat(a_list[0:index], [val], a_list[right_start:])

def maybe_map(fn, thing):
    if isinstance(thing, dict):
        return fn(thing)
    if is_iterable(thing):
        return list(map(fn, thing))
    return fn(thing)

def maybe_filter(fn, thing):
    if isinstance(thing, dict):
        return fn(thing)
    if is_iterable(thing):
        return list(filter(fn, thing))
    return fn(thing)

def has_attrs(attr_list, thing):
    for attr in attr_list:
        if attr not in thing:
            return False
    return True

has_attrs_pred = lambda attr_list: partial(has_attrs, attr_list)

def nth(n, things):
    if is_iterable(things):
        things = list(things)
    return things[n]

def getter(key, thing):
    if isinstance(thing, dict):
        return thing.get(key, None)
    return getattr(thing, key, None)

getter_pred = lambda key: partial(getter, key)

def match_attr(key, val, thing):
    return getter(key, thing) == val

match_attr_pred = lambda key, val: partial(match_attr, key, val)

def match_attr_multi(key, good_vals, thing):
    thing_val = getter(key, thing)
    result = False
    for val in good_vals:
        if thing_val == val:
            result = True
            break
    return result

match_attr_multi_pred = lambda key, good_vals: partial(match_attr_multi, key, good_vals)

def ensure_list(x):
    if not isinstance(x, list):
        x = [x]
    return x

def match_attrs(to_match, to_test):
    for k, v in to_match.items():
        if getter(k, to_test) != v:
            return False
    return True

match_attrs_pred = lambda to_match: partial(match_attrs, to_match)

def find_first(pred, things):
    for thing in things:
        if pred(thing):
            return thing
    return None

def pluck_with(*attrs):
    def inner_pluck(thing):
        return {k: v for k, v in thing.items() if k in attrs}
    return inner_pluck

def as_obj(pairs):
    return {p[0]: p[1] for p in pairs}

def clone_array(x):
    return [elem for elem in x]

def without(bad_attrs, thing):
    return {k: v for k, v in thing.items() if k not in bad_attrs}

without_pred = lambda bad_attrs: partial(without, bad_attrs)

def obj_clone(a_dict):
    return {k: v for k, v in a_dict.items()}

def is_iterable(x):
    return hasattr(x, '__iter__')

def drop(n, a_list):
    return a_list[n:]

def take(n, a_list):
    return a_list[0:n]

def slice_with(start, end, a_list):
    return a_list[start:end]

def max_mapped(func, sequence):
    pred = lambda x: none2min(func(x))
    return max(sequence, key=pred)

def min_mapped(func, sequence):
    pred = lambda x: none2min(func(x))
    return min(sequence, key=pred)

def group_by_func(func, sequence):
    output = GroupResults()
    for elem in sequence:
        output[func(elem)].append(elem)
    return output

def is_num(x):
    return isinstance(x, (int, float))

def safe_sum(nums):
    return sum(filter(is_num, nums))

def safe_average(nums):
    actual_nums = list(filter(is_num, nums))
    return sum(actual_nums) / len(actual_nums)

def array_of_string(string):
    return [char for char in string]

def rql_str_split(string, split_on, limit=-1):
    if not split_on:
        if isinstance(split_on, str):
            # rql's string.split mimics python's except for splitting on empty string.
            # in that case python throws an error, while rql converts to char array
            return array_of_string(string)
        # pythons string.split() seems not to allow a limit with default split_on
        return string.split()
    return string.split(split_on, limit)

def indices_of_passing(pred, sequence):
    out = []
    for index in range(0, len(list(sequence))):
        if pred(sequence[index]):
            out.append(index)
    return out

def without_indices(indices, sequence):
    indices = set(indices)
    for index in range(0, len(sequence)):
        if index not in indices:
            yield sequence[index]

def eq(x, y):
    return x == y

eq_pred = lambda x: partial(eq, x)

def sorted_iteritems(a_dict):
    keys = a_dict.keys()
    for k in sorted(keys):
        yield k, a_dict[k]

def make_hashable(x):
    if isinstance(x, list):
        return tuple(make_hashable(elem) for elem in sorted(x))
    if isinstance(x, dict):
        out = []
        for k, v in sorted_iteritems(x):
            out.append((k, make_hashable(v)))
        return tuple(elem for elem in out)
    return x

class DictableSet(set):
    def __init__(self, elems):
        elems = map(make_hashable, elems)
        super().__init__(elems)

    def add(self, elem):
        elem = make_hashable(elem)
        super().add(elem)

    def has(self, elem):
        return make_hashable(elem) in self


def dictable_distinct(sequence):
    seen = DictableSet([])
    for elem in sequence:
        if not seen.has(elem):
            seen.add(elem)
            yield elem


def any_passing(pred, sequence):
    for elem in sequence:
        if pred(elem):
            return True
    return False
