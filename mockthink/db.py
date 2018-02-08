import contextlib
from pprint import pprint
from copy import deepcopy
from collections import Iterable

import rethinkdb

from . import rtime, util
from .rql_rewrite import rewrite_query
from .scope import Scope

def fill_missing_report_results(report):
    defaults = {
        'errors': 0,
        'replaced': 0,
        'inserted': 0,
        'deleted': 0,
        'changes': []
    }
    return util.extend(defaults, report)

def replace_array_elems_by_id(existing, replace_with):
    report = {
        'replaced': 0,
        'changes': []
    }
    elem_index_by_id = {}
    for index in range(0, len(existing)):
        elem = existing[index]
        elem_index_by_id[util.getter('id', elem)] = index

    to_return = util.clone_array(existing)

    for elem in replace_with:
        index = elem_index_by_id[util.getter('id', elem)]
        change = {
            'old_val': existing[index],
            'new_val': elem
        }
        report['changes'].append(change)
        report['replaced'] += 1
        to_return[index] = elem

    return to_return, fill_missing_report_results(report)

def remove_array_elems_by_id(existing, to_remove):
    report = {
        'deleted': 0,
        'changes': []
    }
    result = util.clone_array(existing)
    for elem in to_remove:
        if elem in result:
            report['deleted'] += 1
            report['changes'].append({'old_val': elem, 'new_val': None})
            result.remove(elem)
    return result, report

def insert_into_table_with_conflict_setting(existing, to_insert, conflict):
    assert conflict in ('error', 'update', 'replace')
    existing_by_id = {row['id']: row for row in existing}
    seen = set([])
    result = []
    result_report = {
        'errors': 0,
        'inserted': 0,
        'replaced': 0,
        'changes': []
    }
    for doc in to_insert:
        change = {}
        if doc['id'] in existing_by_id:
            existing_row = existing_by_id[doc['id']]
            change['old_val'] = existing_row
            if conflict == 'error':
                result_report['errors'] += 1
                continue
            elif conflict == 'update':
                result_row = util.extend(existing_row, doc)
            elif conflict == 'replace':
                result_row = doc
            result_report['replaced'] += 1
            seen.add(doc['id'])
        else:
            change['old_val'] = None
            result_report['inserted'] += 1
            result_row = doc
        result.append(result_row)
        change['new_val'] = result_row
        result_report['changes'].append(change)

    not_updated = [row for row in existing if row['id'] not in seen]
    result = not_updated + result
    return result, fill_missing_report_results(result_report)

class MockTableData(object):
    def __init__(self, name, rows, indexes):
        self.name = name
        self.rows = rows
        self.indexes = indexes

    def replace_all(self, rows, indexes):
        return MockTableData(self.name, rows, indexes)

    def update_by_id(self, updated_rows):
        if not isinstance(updated_rows, list):
            updated_rows = [updated_rows]
        new_data, report = replace_array_elems_by_id(self.rows, updated_rows)
        return MockTableData(self.name, new_data, self.indexes), report

    def insert(self, new_rows, conflict):
        assert conflict in ('error', 'update', 'replace')
        if not isinstance(new_rows, list):
            new_rows = [new_rows]
        new_data, report = insert_into_table_with_conflict_setting(self.rows, new_rows, conflict)
        return MockTableData(self.name, new_data, self.indexes), report

    def remove_by_id(self, to_remove):
        if not isinstance(to_remove, list):
            to_remove = [to_remove]
        new_data, report = remove_array_elems_by_id(self.rows, to_remove)
        return MockTableData(self.name, new_data, self.indexes), report

    def get_rows(self):
        return self.rows

    def create_index(self, index_name, index_func, multi=False):
        to_add = {
            'func': index_func,
            'multi': multi
        }
        return MockTableData(self.name, self.rows, util.extend(self.indexes, {index_name: to_add}))

    def rename_index(self, old_name, new_name):
        new_indexes = util.without([old_name], self.indexes)
        new_indexes[new_name] = self.indexes[old_name]
        return MockTableData(self.name, self.rows, new_indexes)

    def drop_index(self, index_name):
        new_indexes = util.without([index_name], self.indexes)
        return MockTableData(self.name, self.rows, new_indexes)

    def list_indexes(self):
        return self.indexes.keys()

    def index_exists(self, index):
        return index in self.indexes

    def _index_values(self, index_name):
        func = self.get_index_func(index_name)
        out = [func(elem) for elem in self.rows]
        pprint({'func': func, 'out': out})
        return out

    def get_index_func(self, index):
        return self.indexes[index].get('func')

    def is_multi_index(self, index):
        return self.indexes[index].get('multi', False)

    def __iter__(self):
        for elem in self.rows:
            yield elem

    def __getitem__(self, index):
        return self.rows[index]

    def __repr__(self):
        return '<MockTableData name="%s"/>' % self.name

class MockDbData(object):
    def __init__(self, tables_by_name):
        self.tables_by_name = tables_by_name

    def create_table(self, table_name):
        return self.set_table(table_name, MockTableData(table_name, [], {}))

    def list_tables(self):
        return self.tables_by_name.keys()

    def drop_table(self, table_name):
        return MockDbData(util.without([table_name], self.tables_by_name))

    def get_table(self, table_name):
        return self.tables_by_name[table_name]

    def set_table(self, table_name, new_table_instance):
        assert isinstance(new_table_instance, MockTableData)
        tables = util.obj_clone(self.tables_by_name)
        tables[table_name] = new_table_instance
        return MockDbData(tables)

class MockDb(object):
    def __init__(self, dbs_by_name, mockthink):
        self.dbs_by_name = dbs_by_name
        self.mockthink = mockthink

    def get_db(self, db_name):
        if db_name is None:
            db_name = self.default_db
        assert db_name is not None
        return self.dbs_by_name[db_name]

    def set_db(self, db_name, db_data_instance):
        if db_name is None:
            db_name = self.default_db
        assert db_name is not None
        assert isinstance(db_data_instance, MockDbData)
        dbs_by_name = util.obj_clone(self.dbs_by_name)
        dbs_by_name[db_name] = db_data_instance
        return MockDb(dbs_by_name, self.mockthink)

    def create_table_in_db(self, db_name, table_name):
        new_db = self.get_db(db_name)
        new_db = new_db.create_table(table_name)
        return self.set_db(db_name, new_db)

    def drop_table_in_db(self, db_name, table_name):
        new_db = self.get_db(db_name)
        new_db = new_db.drop_table(table_name)
        return self.set_db(db_name, new_db)

    def list_tables_in_db(self, db_name):
        return self.get_db(db_name).list_tables()

    def create_db(self, db_name):
        return self.set_db(db_name, MockDbData({}))

    def drop_db(self, db_name):
        return MockDb(util.without([db_name], self.dbs_by_name),
                      self.mockthink)

    def list_dbs(self):
        return self.dbs_by_name.keys()

    def replace_table_in_db(self, db_name, table_name, table_data_instance):
        assert isinstance(table_data_instance, MockTableData)
        db = self.get_db(db_name)
        new_db = db.set_table(table_name, table_data_instance)
        return self.set_db(db_name, new_db)

    def insert_into_table_in_db(self, db_name, table_name, elem_list, conflict):
        assert conflict in ('error', 'update', 'replace')
        db = self.get_db(db_name)
        table = db.get_table(table_name)
        new_table_data, report = table.insert(elem_list, conflict)
        return self._replace_table(db_name, table_name, new_table_data), report

    def update_by_id_in_table_in_db(self, db_name, table_name, elem_list):
        new_table_data, report = self.get_db(db_name).get_table(table_name).update_by_id(elem_list)
        return self._replace_table(db_name, table_name, new_table_data), report

    def _replace_table(self, db_name, table_name, new_table_data):
        new_db = self.get_db(db_name).set_table(table_name, new_table_data)
        return self.set_db(db_name, new_db)

    def remove_by_id_in_table_in_db(self, db_name, table_name, elem_list):
        new_table_data, report = self.get_db(db_name).get_table(table_name).remove_by_id(elem_list)
        return self._replace_table(db_name, table_name, new_table_data), report

    def create_index_in_table_in_db(self, db_name, table_name, index_name, index_func, multi=False):
        new_table_data = self.get_db(db_name)\
            .get_table(table_name)\
            .create_index(index_name, index_func, multi=multi)
        return self._replace_table(db_name, table_name, new_table_data)

    def drop_index_in_table_in_db(self, db_name, table_name, index_name):
        new_table_data = self.get_db(db_name).get_table(table_name).drop_index(index_name)
        return self._replace_table(db_name, table_name, new_table_data)

    def rename_index_in_table_in_db(self, db_name, table_name, old_index_name, new_index_name):
        db = self.get_db(db_name)
        table = db.get_table(table_name)
        new_table_data = table.rename_index(old_index_name, new_index_name)
        return self._replace_table(db_name, table_name, new_table_data)

    def list_indexes_in_table_in_db(self, db_name, table_name):
        return self.get_db(db_name).get_table(table_name).list_indexes()

    def index_exists_in_table_in_db(self, db_name, table_name, index_name):
        return self.get_db(db_name).get_table(table_name).index_exists(index_name)

    def get_index_func_in_table_in_db(self, db_name, table_name, index_name):
        return self.get_db(db_name).get_table(table_name).get_index_func(index_name)

    def is_multi_index(self, db_name, table_name, index_name):
        return self.get_db(db_name).get_table(table_name).is_multi_index(index_name)

    @property
    def now_time(self):
        return self.mockthink.now_time

    @property
    def default_db(self):
        return self.mockthink.default_db

def objects_from_pods(data):
    dbs_by_name = {}
    for db_name, db_data in data['dbs'].items():
        tables_by_name = {}
        for table_name, table_data in db_data['tables'].items():
            if isinstance(table_data, list):
                indexes = {}
            else:
                indexes = table_data.get('indexes', {})
                table_data = table_data.get('rows', [])
            tables_by_name[table_name] = MockTableData(
                table_name, table_data, indexes
            )
        dbs_by_name[db_name] = MockDbData(tables_by_name)
    return MockDb(dbs_by_name, None)

class MockThinkConn(object):
    def __init__(self, mockthink_parent, db=None):
        self.mockthink_parent = mockthink_parent
        self.db = db
    def reset_data(self, data):
        self.mockthink_parent._modify_initial_data(data)
    def _start(self, rql_query, **global_optargs):
        query = rewrite_query(rql_query)
        return self.mockthink_parent.run_query(query, self.db)
    def use(self, db_name):
        self.db = db_name

class MockThink(object):
    def __init__(self, initial_data):
        self.data = None
        self._modify_initial_data(initial_data)
        self.tzinfo = rethinkdb.make_timezone('00:00')
        self._now_time = None
        self._default_db = None

    def _modify_initial_data(self, new_data):
        self.initial_data = new_data
        self.reset()

    def run_query(self, query, db=None):
        try:
            temp_now_time = False
            temp_default_db = False

            # RethinkDB only evaluates `r.now()` once per query,
            # so it should have the same result each time within that query.
            # But we don't do anything if now_time has already been set.

            if not self.now_time:
                temp_now_time = True
                self._now_time = self.now_time

            if not self.default_db:
                temp_default_db = True
                self._default_db = db

            scope = Scope()
            result = query.run(self.data, scope)
            changes = None
            if isinstance(result, tuple) and isinstance(result[0], MockDb):
                changes = result[1]
                result = result[0]
            if isinstance(result, MockDb):
                self.data = result
                result = changes
            elif isinstance(result, MockTableData):
                result = result.get_rows()

            if isinstance(result, util.GroupResults):
                return deepcopy(dict(result))
            if isinstance(result, (dict, list, str)):
                return deepcopy(result)
            if isinstance(result, Iterable):
                return list(map(deepcopy, result))
            return result
        finally:
            if temp_now_time:
                self._now_time = None
            if temp_default_db:
                self._default_db = None

    def reset(self):
        self.data = objects_from_pods(self.initial_data)
        self.data.mockthink = self

    def get_conn(self, db=None):
        conn = MockThinkConn(self, db)
        return conn

    @property
    def now_time(self):
        if self._now_time:
            return self._now_time
        return rtime.now()

    @property
    def default_db(self):
        return self._default_db

    @contextlib.contextmanager
    def connect(self, db=None):
        conn = MockThinkConn(self, db)
        yield conn
        self.reset()
