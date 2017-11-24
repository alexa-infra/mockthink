import rethinkdb as r
from mockthink import MockThink
from mockthink.test.common import as_db_and_table, assertEqual
from mockthink.test.functional.common import MockTest
from pprint import pprint


class TestToplevelTable(object):

    def test_table_list(self):
        db = MockThink(as_db_and_table('my', 'xx', []))
        conn = db.get_conn(db='my')
        tables = list(r.table_list().run(conn))
        expected = ['xx']
        assertEqual(expected, tables)

    def test_table_list_with_use(self):
        db = MockThink(as_db_and_table('my', 'xx', []))
        conn = db.get_conn()
        conn.use('my')
        tables = list(r.table_list().run(conn))
        expected = ['xx']
        assertEqual(expected, tables)

    def test_table_list_with_scope(self):
        db = MockThink(as_db_and_table('my', 'xx', []))
        with db.connect('my') as conn:
            tables = list(r.table_list().run(conn))
            expected = ['xx']
            assertEqual(expected, tables)

    def test_table_list_fails(self):
        db = MockThink(as_db_and_table('my', 'xx', []))
        conn = db.get_conn()
        try:
            tables = list(r.table_list().run(conn))
            error = False
        except:
            error = True
        assert(error)

    def test_table_create(self):
        db = MockThink(as_db_and_table('my', 'xx', []))
        conn = db.get_conn(db='my')
        r.table_create('yy').run(conn)
        tables = list(r.table_list().run(conn))
        expected = ['xx', 'yy']
        assertEqual(expected, tables)

    def test_table_drop(self):
        db = MockThink(as_db_and_table('my', 'xx', []))
        conn = db.get_conn(db='my')
        r.table_drop('xx').run(conn)
        tables = list(r.table_list().run(conn))
        expected = []
        assertEqual(expected, tables)

    def test_table_get_items(self):
        db = MockThink(as_db_and_table('my', 'xx', [
            dict(id=1, name=1),
            dict(id=2, name=2)
        ]))
        conn = db.get_conn(db='my')
        results = r.table('xx').get_field('id').run(conn)
        expected = [1, 2]
        assertEqual(expected, results)

    def test_nested_query(self):
        db = MockThink({'dbs': {
            'my': {
                'tables': {
                    'items': [
                        {'id': 1, 'name': 'one', 'ref': 1},
                        {'id': 2, 'name': 'two', 'ref': 2},
                    ],
                    'refs': [
                        {'id': 1, 'name': 'eins'},
                        {'id': 2, 'name': 'zwei'},
                    ]
                }
            }
        }})
        conn = db.get_conn()
        conn.use('my')
        query = r.table('items').get_field('ref')
        query = query.map(lambda refid:
            r.table('refs').get(refid).get_field('name')
        )
        results = query.run(conn)
        expected = ['eins', 'zwei']
        assertEqual(expected, results)
