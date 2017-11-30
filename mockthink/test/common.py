import unittest

import rethinkdb as r

import mockthink.util as util
from mockthink.db import MockThinkConn


def real_stock_data_load(data, connection):
    for db in list(r.db_list().run(connection)):
        if db == u"rethinkdb":
            # This db is special and can't be deleted.
            continue
        r.db_drop(db).run(connection)
    for db_name, db_data in data['dbs'].items():
        r.db_create(db_name).run(connection)
        for table_name, table_data in db_data['tables'].items():
            r.db(db_name).table_create(table_name).run(connection)
            r.db(db_name).table(table_name).insert(table_data).run(connection)

def mock_stock_data_load(data, connection):
    connection.reset_data(data)

def load_stock_data(data, connection):
    if isinstance(connection, MockThinkConn):
        return mock_stock_data_load(data, connection)
    elif isinstance(connection, r.net.Connection):
        return real_stock_data_load(data, connection)


def assertEqUnordered(x, y, msg=''):
    for x_elem in x:
        assert x_elem in y


def assertEqual(x, y, msg=''):
    assert x == y


def assertCountEqual(x, y, msg=''):
    case = unittest.TestCase()
    case.assertCountEqual(x, y)


def as_db_and_table(db_name, table_name, data):
    return {
        'dbs': {
            db_name: {
                'tables': {
                    table_name: data
                }
            }
        }
    }

class TestCase(unittest.TestCase):
    def assertEqUnordered(self, x, y, msg=''):
        return assertEqUnordered(x, y, msg)

    def assert_key_equality(self, keys, dict1, dict2):
        pluck = util.pluck_with(*keys)
        assertEqual(pluck(dict1), pluck(dict2))
