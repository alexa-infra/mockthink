import rethinkdb as r
from rethinkdb import ReqlNonExistenceError
from mockthink.test.common import as_db_and_table, assertEqual
from mockthink.test.functional.common import MockTest
from pprint import pprint

class TestBracketMapping(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 1, 'animals': ['frog', 'cow']},
            {'id': 2, 'animals': ['horse'], 'area': 10}
        ]
        return as_db_and_table('x', 'farms', data)

    def test_simple(self, conn):
        res = r.db('x').table('farms').map(
            lambda doc: doc['animals'][0]
        ).run(conn)
        assertEqual(
            set(['frog', 'horse']),
            set(list(res))
        )

    def test_filter_by_bracket(self, conn):
        res = r.db('x').table('farms').filter(
            lambda doc: doc['id'] < 2
        ).run(conn)
        expected = [1]
        results = [doc['id'] for doc in res]
        assertEqual(expected, results)

    def test_order_by_bracket(self, conn):
        res = r.db('x').table('farms').order_by(
            lambda doc: doc['id']
        ).map(lambda doc: doc['id']).run(conn)
        expected = [1, 2]
        assertEqual(expected, list(res))

    def test_missing(self, conn):
        res = r.db('x').table('farms').filter(
            lambda doc: doc['area'].default(1) > 5
        ).run(conn)
        expected = [2]
        results = [doc['id'] for doc in res]
        assertEqual(expected, results)

class TestGetField(MockTest):

    @staticmethod
    def get_data():
        data = [
            {'id': 1, 'animals': ['frog', 'cow']},
            {'id': 2, 'animals': ['horse'], 'area': 10}
        ]
        return as_db_and_table('x', 'farms', data)

    def test_get_field(self, conn):
        results = r.db('x').table('farms').get_field('id').run(conn)
        expected = [1, 2]
        assertEqual(expected, results)

    def test_get_field_missing(self, conn):
        results = r.db('x').table('farms').get_field('area').run(conn)
        expected = [10]
        assertEqual(expected, results)

    def test_get_field_object(self, conn):
        results = r.expr(dict(name=1)).get_field('name').run(conn)
        expected = 1
        assertEqual(expected, results)

    def test_get_field_object_missing(self, conn):
        try:
            query = r.expr(dict(name=1))
            results = query.get_field('value').run(conn)
            error = False
        except ReqlNonExistenceError as k:
            error = True
        assert(error)
