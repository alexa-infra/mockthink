from datetime import timedelta
import rethinkdb as r
from mockthink.rtime import now
from mockthink.test.common import as_db_and_table, assertEqUnordered
from mockthink.test.functional.common import MockTest

class TestBetween(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'},
            {'id': 'zuul', 'first_name': 'Adam', 'last_name': 'Zuul'}

        ]
        return as_db_and_table('s', 'people', data)

    def test_between_id_default_range(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        result = r.db('s').table('people').between(
            'bob', 'zuul'
        ).run(conn)
        assertEqUnordered(expected, list(result))

    def test_between_id_closed_right(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        result = r.db('s').table('people').between(
            'bob', 'tom', right_bound='closed'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)

    def test_between_id_open_left(self, conn):
        expected = [
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'}
        ]
        result = r.db('s').table('people').between(
            'bob', 'tom', left_bound='open'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)

    def test_between_id_open_left_closed_right(self, conn):
        expected = [
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        result = r.db('s').table('people').between(
            'bob', 'tom', left_bound='open', right_bound='closed'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)

    def test_between_index_default_range(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        r.db('s').table('people').index_wait().run(conn)
        result = r.db('s').table('people').between(
            'Builder', 'Smith', index='last_name'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)

    def test_between_index_closed_right(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'}
        ]
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        r.db('s').table('people').index_wait().run(conn)
        result = r.db('s').table('people').between(
            'Builder', 'Smith', index='last_name', right_bound='closed'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)

    def test_between_index_open_left(self, conn):
        expected = [
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'}
        ]
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        r.db('s').table('people').index_wait().run(conn)
        result = r.db('s').table('people').between(
            'Builder',
            'Smith',
            index='last_name',
            left_bound='open',
            right_bound='closed'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)

class TestBetweenMinMax(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'bob', 'first_name': 'Bob', 'age': 15},
            {'id': 'joe', 'first_name': 'Joseph', 'age': 27},
            {'id': 'tom', 'first_name': 'Tom', 'age': 50},
            {'id': 'zuul', 'first_name': 'Adam', 'age': 30}

        ]
        return as_db_and_table('s', 'people', data)

    def _create_index(self, conn):
        r.db('s').table('people').index_create(
            'age'
        ).run(conn)

    def test_minval(self, conn):
        self._create_index(conn)
        query = r.db('s').table('people')
        query = query.between(r.minval, 30, index='age')
        query = query.get_field('id')
        results = list(query.run(conn))
        expected = ['bob', 'joe']
        assertEqUnordered(expected, results)

    def test_maxval(self, conn):
        self._create_index(conn)
        query = r.db('s').table('people')
        query = query.between(30, r.maxval, index='age')
        query = query.get_field('id')
        results = list(query.run(conn))
        expected = ['zuul', 'tom']
        assertEqUnordered(expected, results)

    def test_minmax(self, conn):
        self._create_index(conn)
        query = r.db('s').table('people')
        query = query.between(r.minval, r.maxval, index='age')
        query = query.get_field('id')
        results = list(query.run(conn))
        expected = ['bob', 'joe', 'zuul', 'tom']
        assertEqUnordered(expected, results)

class TestBetweenMinMaxDatetime(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'bob', 'first_name': 'Bob', 'created': now() - timedelta(days=1)},
            {'id': 'joe', 'first_name': 'Joseph', 'created': now() - timedelta(days=2)},
            {'id': 'tom', 'first_name': 'Tom', 'created': now() + timedelta(days=3)},
            {'id': 'zuul', 'first_name': 'Adam', 'created': now() + timedelta(hours=1)}

        ]
        return as_db_and_table('s', 'people', data)

    def _create_index(self, conn):
        r.db('s').table('people').index_create(
            'created'
        ).run(conn)

    def test_minval(self, conn):
        self._create_index(conn)
        query = r.db('s').table('people')
        query = query.between(r.minval, r.now(), index='created')
        query = query.get_field('id')
        results = list(query.run(conn))
        expected = ['bob', 'joe']
        assertEqUnordered(expected, results)

    def test_maxval(self, conn):
        self._create_index(conn)
        query = r.db('s').table('people')
        query = query.between(r.now(), r.maxval, index='created')
        query = query.get_field('id')
        results = list(query.run(conn))
        expected = ['tom', 'zuul']
        assertEqUnordered(expected, results)

class TestBetweenCompound(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'bob', 'first_name': 'Bob', 'age': 20, 'party': 'one'},
            {'id': 'joe', 'first_name': 'Joseph', 'age': 25, 'party': 'one'},
            {'id': 'tom', 'first_name': 'Tom', 'age': 35, 'party': 'two'},
            {'id': 'zuul', 'first_name': 'Adam', 'age': 24, 'party': 'two'}

        ]
        return as_db_and_table('s', 'people', data)

    def _create_index(self, conn):
        r.db('s').table('people').index_create(
            'party_age',
            [r.row['party'], r.row['age']]
        ).run(conn)

    def test_between(self, conn):
        self._create_index(conn)
        query = r.db('s').table('people')
        query = query.between(
            ['one', 20], ['one', 30], index='party_age'
        )
        query = query.get_field('id')
        results = list(query.run(conn))
        expected = ['bob', 'joe']
        assertEqUnordered(expected, results)
        query = r.db('s').table('people')
        query = query.between(
            ['two', 20], ['two', 30], index='party_age'
        )
        query = query.get_field('id')
        results = list(query.run(conn))
        expected = ['zuul']
        assertEqUnordered(expected, results)
