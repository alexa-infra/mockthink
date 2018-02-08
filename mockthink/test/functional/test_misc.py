import rethinkdb as r
from rethinkdb import RqlRuntimeError

from mockthink import util
from mockthink.test.common import as_db_and_table, assertEqUnordered, assertEqual
from mockthink.test.functional.common import MockTest


class TestImmutable(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'joe-id', 'name': 'joe'},
            {'id': 'bob-id', 'name': 'bob'}
        ]
        return as_db_and_table('x', 'people', data)

    def test_immutable(self, conn):
        result = r.db('x').table('people').get('bob-id').run(conn)
        assert 'id' in result and result['id'] == 'bob-id'
        del result['id']
        result = r.db('x').table('people').get('bob-id').run(conn)
        assert 'id' in result and result['id'] == 'bob-id'

class TestGet(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'joe-id', 'name': 'joe'},
            {'id': 'bob-id', 'name': 'bob'}
        ]
        return as_db_and_table('x', 'people', data)

    def test_get_one_by_id(self, conn):
        result = r.db('x').table('people').get('bob-id').run(conn)
        assertEqual({'id': 'bob-id', 'name': 'bob'}, result)


class TestGetAll(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'sam-id', 'name': 'sam'},
            {'id': 'anne-id', 'name': 'anne'},
            {'id': 'joe-id', 'name': 'joe'},
            {'id': 'bob-id', 'name': 'bob'}
        ]
        return as_db_and_table('x', 'people', data)

    def test_get_all_by_id(self, conn):
        expected = [
            {'id': 'anne-id', 'name': 'anne'},
            {'id': 'joe-id', 'name': 'joe'},
        ]
        result = r.db('x').table('people').get_all('anne-id', 'joe-id').run(conn)
        assertEqUnordered(expected, result)

    def test_get_all_just_one(self, conn):
        expected = [
            {'id': 'bob-id', 'name': 'bob'},
        ]
        result = r.db('x').table('people').get_all('bob-id').run(conn)
        assertEqual(expected, list(result))

    def test_get_all_nested(self, conn):
        q = r.expr(['anne-id', 'joe-id'])
        q = q.concat_map(lambda idx:
            r.db('x').table('people').get_all(idx))
        q = q.get_field('name')
        results = list(q.run(conn))
        expected = ['anne', 'joe']
        assertEqUnordered(expected, results)

class TestFiltering(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'joe-id', 'name': 'joe', 'age': 28},
            {'id': 'bob-id', 'name': 'bob', 'age': 19},
            {'id': 'bill-id', 'name': 'bill', 'age': 35},
            {'id': 'kimye-id', 'name': 'kimye', 'age': 17}
        ]
        return as_db_and_table('x', 'people', data)

    def test_filter_lambda_gt(self, conn):
        expected = [
            {'id': 'joe-id', 'name': 'joe', 'age': 28},
            {'id': 'bill-id', 'name': 'bill', 'age': 35}
        ]
        result = r.db('x').table('people').filter(lambda p: p['age'] > 20).run(conn)
        assertEqUnordered(expected, list(result))

    def test_filter_lambda_lt(self, conn):
        expected = [
            {'id': 'bob-id', 'name': 'bob', 'age': 19},
            {'id': 'kimye-id', 'name': 'kimye', 'age': 17}
        ]
        result = r.db('x').table('people').filter(lambda p: p['age'] < 20).run(conn)
        assertEqUnordered(expected, list(result))

    def test_filter_dict_match(self, conn):
        expected = [{'id': 'bill-id', 'name': 'bill', 'age': 35}]
        result = r.db('x').table('people').filter({'age': 35}).run(conn)
        assertEqual(expected, list(result))


class TestMapping(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'joe-id', 'name': 'joe', 'age': 28},
            {'id': 'bob-id', 'name': 'bob', 'age': 19},
            {'id': 'bill-id', 'name': 'bill', 'age': 35},
            {'id': 'kimye-id', 'name': 'kimye', 'age': 17}
        ]
        return as_db_and_table('x', 'people', data)

    def test_map_gt(self, conn):
        expected = [
            True, False, True, False
        ]
        result = r.db('x').table('people').map(
            lambda p: p['age'] > 20
        ).run(conn)
        assertEqUnordered(expected, list(result))

    def test_map_missing_field_no_default(self, conn):
        err = None
        try:
            result = r.db('x').table('people').map(
                lambda p: p['missing'] > 15
            ).run(conn)
        except RqlRuntimeError as e:
            err = e
        assert(isinstance(err, RqlRuntimeError))

    def test_map_none_arg(self, conn):
        q = r.expr([None, 1, 2])
        q = q.map(lambda x: {'v': x})
        results = list(q.run(conn))
        expected = [dict(v=None), dict(v=1), dict(v=2)]
        assertEqual(expected, results)

class TestConcatMap(MockTest):
    @staticmethod
    def get_data():
        npc = [
            {'id': 'joe-id', 'name': 'joe', 'tags': ['friend']},
            {'id': 'bob-id', 'name': 'bob', 'tags': ['enemy']},
            {'id': 'bill-id', 'name': 'bill', 'tags': ['friend', 'hero'] },
            {'id': 'kimye-id', 'name': 'kimye', 'tags': ['enemy', 'zombie']}
        ]
        inventory = [
            {'id': 1, 'npc': 'joe-id', 'item': 'shovel'},
            {'id': 2, 'npc': 'bill-id', 'item': 'superpower'},
            {'id': 3, 'npc': 'bob-id', 'item': 'gun'},
            {'id': 4, 'npc': 'bob-id', 'item': 'lighter'},
        ]
        return {
            'dbs': {
                'x': {
                    'tables': {
                        'npc': npc,
                        'inventory': inventory,
                    }
                }
            }
        }

    def test_concat_map_objects(self, conn):
        expected = ['friend', 'enemy', 'friend', 'hero', 'enemy', 'zombie']
        results = r.db('x').table('npc').concat_map(
            r.row['tags']
        ).run(conn)
        assertEqual(expected, results)

    def test_concat_map_nested_query(self, conn):
        expected = ['shovel', 'gun', 'lighter', 'superpower']
        query = r.db('x').table('npc')
        query = query.concat_map(lambda npc:
            r.db('x').table('inventory').filter(
                lambda row: row['npc'] == npc['id']))
        query = query.get_field('item')
        results = list(query.run(conn))
        assertEqual(expected, results)

class TestCompoundIndex(MockTest):
    @staticmethod
    def get_data():
        npc = [
            {'id': 'joe-id', 'name': 'joe', 'team': 'red', 'deleted': False},
            {'id': 'bob-id', 'name': 'bob', 'team': 'red', 'deleted': True},
            {'id': 'bill-id', 'name': 'bill', 'team': 'red', 'deleted': True},
            {'id': 'kimye-id', 'name': 'kimye', 'team': 'green', 'deleted': False}
        ]
        return as_db_and_table('x', 'npc', npc)

    def create_index(self, conn):
        r.db('x').table('npc').index_create('team_current',
            [r.row['team'], r.row['deleted'].not_()]
        ).run(conn)

    def test_simple(self, conn):
        self.create_index(conn)
        query = r.db('x').table('npc')
        query = query.get_all(['red', True], index='team_current')
        query = query.get_field('name')
        results = list(query.run(conn))
        expected = ['joe']
        assertEqual(expected, results)

    def test_list(self, conn):
        self.create_index(conn)
        query = r.db('x').table('npc')
        query = query.get_all(['red', True], ['green', True], index='team_current')
        query = query.get_field('name')
        results = list(query.run(conn))
        expected = ['joe', 'kimye']
        assertEqual(expected, results)

    def test_nested(self, conn):
        self.create_index(conn)
        conn.use('x')
        query = r.table('npc').get_all('joe-id')
        query = query.concat_map(lambda x:
            r.table('npc').get_all([x['team'], False], index='team_current')
        )
        query = query.get_field('name')
        results = list(query.run(conn))
        expected = ['bob', 'bill']
        assertEqual(expected, results)


class TestMinMaxNone(MockTest):
    @staticmethod
    def get_data():
        return as_db_and_table('x', 'npc', [])

    def test_max(self, conn):
        query = r.expr([None, -2, 1, 3, 8]).max()
        result = query.run(conn)
        expected = 8
        assert expected == result

    def test_min(self, conn):
        query = r.expr([None, -2, 1, 3, 8]).min()
        result = query.run(conn)
        expected = None
        assert expected == result

    def test_min_without_none(self, conn):
        query = r.expr([-2, 1, 3, 8]).min()
        result = query.run(conn)
        expected = -2
        assert expected == result

    def test_max_field(self, conn):
        query = r.expr([dict(v=1), dict(v=None), dict(v=5)]).max('v')
        result = query.run(conn)
        expected = dict(v=5)
        assert expected == result

    def test_min_field(self, conn):
        query = r.expr([dict(v=1), dict(v=None), dict(v=5)]).min('v')
        result = query.run(conn)
        expected = dict(v=None)
        assert expected == result

    def test_min_field_without_none(self, conn):
        query = r.expr([dict(v=1), dict(v=5)]).min('v')
        result = query.run(conn)
        expected = dict(v=1)
        assert expected == result

    def test_max_func(self, conn):
        query = r.expr([dict(v=1), dict(v=None), dict(v=5)])
        query = query.max(lambda x: x['v'])
        result = query.run(conn)
        expected = dict(v=5)
        assert expected == result

    def test_min_func(self, conn):
        query = r.expr([dict(v=1), dict(v=None), dict(v=5)])
        query = query.min(lambda x: x['v'])
        result = query.run(conn)
        expected = dict(v=None)
        assert expected == result

    def test_min_func_without_none(self, conn):
        query = r.expr([dict(v=1), dict(v=3), dict(v=5)])
        query = query.min(lambda x: x['v'])
        result = query.run(conn)
        expected = dict(v=1)
        assert expected == result

class TestSortNone(MockTest):
    @staticmethod
    def get_data():
        return as_db_and_table('x', 'npc', [])

    def test_order_by(self, conn):
        query = r.expr([None, -2, 3, 1, 8])
        query = query.map(lambda x: {'v': x})
        query = query.order_by(r.desc('v'))
        query = query.get_field('v')
        result = list(query.run(conn))
        expected = [8, 3, 1, -2, None]
        assert expected == result

class TestNth(MockTest):
    @staticmethod
    def get_data():
        return as_db_and_table('x', 'npc', [
            {'id': 1, 'name': 'bill'},
            {'id': 2, 'name': 'john'},
            {'id': 3, 'name': 'tom'},
        ])

    def test_nth(self, conn):
        query = r.db('x').table('npc').order_by('name').nth(0)
        result = query.get_field('name').run(conn)
        expected = 'bill'
        assert expected == result

    def test_nth_expr(self, conn):
        query = r.expr([1, 2, 3]).nth(0)
        result = query.run(conn)
        expected = 1
        assert expected == result

class TestForEach(MockTest):
    @staticmethod
    def get_data():
        npc = []
        return as_db_and_table('x', 'npc', npc)

    def test_for_each_insert(self, conn):
        query = r.expr([1, 2, 3, 4])
        query = query.for_each(lambda x:
            r.db('x').table('npc').insert(dict(id=x, name='zombie' + x.coerce_to('string')))
        )
        res = query.run(conn)
        assert 'inserted' in res
        assert res['inserted'] == 4
        query = r.db('x').table('npc')
        query = query.get_field('name')
        results = query.run(conn)
        expected = ['zombie1', 'zombie2', 'zombie3', 'zombie4']
        assertEqUnordered(expected, results)

    def test_for_each_update(self, conn):
        query = r.db('x').table('npc')
        query = query.insert([
            dict(name='zombie1', kind='zombie'),
            dict(name='zombie2', kind='zombie'),
            dict(name='hero1', kind='hero')
        ])
        query.run(conn)
        query = r.db('x').table('npc')
        query = query.filter(r.row['kind'].eq('zombie'))
        query = query.for_each(lambda x:
            r.table('npc').get(x['id']).update(dict(is_dead=True))
        )
        res = query.run(conn)
        assert 'replaced' in res
        assert res['replaced'] == 2
        query = r.db('x').table('npc')
        query = query.filter(r.row.has_fields('is_dead'))
        query = query.get_field('name')
        results = query.run(conn)
        expected = ['zombie1', 'zombie2']
        assertEqUnordered(expected, results)


class TestArgs(MockTest):
    @staticmethod
    def get_data():
        npc = [
            {'id': 'joe-id', 'name': 'joe', 'team': 'red', 'deleted': False},
            {'id': 'bob-id', 'name': 'bob', 'team': 'red', 'deleted': True},
            {'id': 'bill-id', 'name': 'bill', 'team': 'red', 'deleted': True},
            {'id': 'kimye-id', 'name': 'kimye', 'team': 'green', 'deleted': False}
        ]
        return as_db_and_table('x', 'npc', npc)

    def test_get_all_args(self, conn):
        query = r.db('x').table('npc')
        args = ['joe-id', 'bill-id']
        query = query.get_all(r.args(args))
        query = query.get_field('name')
        results = query.run(conn)
        expected = ['joe', 'bill']
        assertEqUnordered(expected, results)

    def test_contains_args(self, conn):
        query = r.expr([1, 2, 3])
        query = query.contains(r.args([1, 2]))
        result = query.run(conn)
        assert result == True
        query = r.expr([1, 2, 3])
        query = query.contains(r.args([1, 2]), r.args([4]))
        result = query.run(conn)
        assert result == False
        query = r.expr([1, 2, 3])
        query = query.contains(r.args([1, 2]), r.args([3]))
        result = query.run(conn)
        assert result == True


class TestMathMonops(MockTest):
    @staticmethod
    def get_data():
        return as_db_and_table('x', 'a', [])

    def test_ceil(self, conn):
        result = r.ceil(12.54).run(conn)
        expected = 13
        assert expected == result
        result = r.ceil(12.34).run(conn)
        expected = 13
        assert expected == result
        result = r.expr(12.34).ceil().run(conn)
        expected = 13
        assert expected == result

    def test_round(self, conn):
        result = r.round(12.54).run(conn)
        expected = 13
        assert expected == result
        result = r.round(12.34).run(conn)
        expected = 12
        assert expected == result
        result = r.expr(12.34).round().run(conn)
        expected = 12
        assert expected == result

    def test_floor(self, conn):
        result = r.floor(12.54).run(conn)
        expected = 12
        assert expected == result
        result = r.floor(12.34).run(conn)
        expected = 12
        assert expected == result
        result = r.expr(12.34).floor().run(conn)
        expected = 12
        assert expected == result


class TestBracket(MockTest):
    @staticmethod
    def get_data():
        data = [
            {
                'id': 'thing-1',
                'other_val': 'other-1',
                'values': {
                    'a': 'a-1',
                    'b': 'b-1',
                    'c': 'c-1',
                    'd': 'd-1'
                }
            },
            {
                'id': 'thing-2',
                'other_val': 'other-2',
                'values': {
                    'a': 'a-2',
                    'b': 'b-2',
                    'c': 'c-2',
                    'd': 'd-2'
                }
            },
        ]
        return as_db_and_table('some_db', 'things', data)

    def test_one_level(self, conn):
        expected = ['other-1', 'other-2']
        result = r.db('some_db').table('things').map(lambda t: t['other_val']).run(conn)
        assertEqUnordered(expected, list(result))

    def test_nested(self, conn):
        expected = ['c-1', 'c-2']
        result = r.db('some_db').table('things').map(lambda t: t['values']['c']).run(conn)
        assertEqUnordered(expected, list(result))

class TestHasFields(MockTest):
    @staticmethod
    def get_data():
        people = [
            {'id': 'joe', 'first_name': 'Joe', 'age': 26},
            {'id': 'todd', 'first_name': 'Todd', 'last_name': 'Last', 'age': 35},
            {'id': 'phil', 'first_name': 'Phil', 'last_name': 'LastPhil'},
            {'id': 'sam', 'first_name': 'Sam', 'last_name': 'SamLast', 'age': 31}

        ]
        return as_db_and_table('x', 'people', people)

    def test_has_fields_1(self, conn):
        expected = [
            {'id': 'todd', 'first_name': 'Todd', 'last_name': 'Last', 'age': 35},
            {'id': 'sam', 'first_name': 'Sam', 'last_name': 'SamLast', 'age': 31}
        ]
        result = r.db('x').table('people').has_fields('last_name', 'age').run(conn)
        assertEqUnordered(expected, list(result))

    def test_has_fields_array(self, conn):
        expected = [
            {'id': 'todd', 'first_name': 'Todd', 'last_name': 'Last', 'age': 35},
            {'id': 'sam', 'first_name': 'Sam', 'last_name': 'SamLast', 'age': 31}
        ]
        result = r.db('x').table('people').has_fields(['last_name', 'age']).run(conn)
        assertEqUnordered(expected, list(result))

class TestIsEmpty(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'id-1', 'things': []},
            {'id': 'id-2', 'things': ['x', 'y']}
        ]
        return as_db_and_table('some_db', 'some_table', data)

    def test_is_empty_table(self, conn):
        query = r.db('some_db').table('some_table').is_empty()
        result = query.run(conn)
        assertEqual(False, result)

    def test_is_empty_nested(self, conn):
        expected = [
            {'id': 'id-1', 'things_empty': True, 'things': []},
            {'id': 'id-2', 'things_empty': False, 'things': ['x', 'y']}
        ]
        result = r.db('some_db').table('some_table').map(
            lambda d: d.merge({'things_empty': d['things'].is_empty()})
        ).run(conn)
        assertEqUnordered(expected, list(result))

    def test_is_empty_toplevel_empty(self, conn):
        result = r.db('some_db').table('some_table').filter({
            'some_key': 'some-value'
        }).is_empty().run(conn)
        assertEqual(True, result)

    def test_is_empty_toplevel_not_empty(self, conn):
        result = r.db('some_db').table('some_table').has_fields('things').is_empty().run(conn)
        assertEqual(False, result)


class TestDo(MockTest):
    @staticmethod
    def get_data():
        data = [
            {
                'id': 'one',
                'name': 'One',
                'pets': ['dog', 'cat', 'bird']
            },
            {
                'id': 'two',
                'name': 'Two',
                'pets': ['fish', 'another fish']
            },
            {
                'id': 'three',
                'name': 'Three',
                'pets': ['toad', 'rabbit']
            }
        ]
        return as_db_and_table('generic', 'table', data)

    def test_do_simple_1(self, conn):
        result = r.db('generic').table('table').get('one').do(
            lambda d: d['name']
        ).run(conn)
        assertEqual('One', result)

    def test_do_simple_2(self, conn):
        result = r.do(r.db('generic').table('table').get('two'),
            lambda d: d['name']
        ).run(conn)
        assertEqual('Two', result)

    def test_do_two(self, conn):
        base = r.db('generic').table('table')
        result = r.do(base.get('one'), base.get('two'),
            lambda d1, d2: [d1['name'], d2['name']]
        ).run(conn)
        assertEqual(['One', 'Two'], result)

    def test_do_three(self, conn):
        base = r.db('generic').table('table')
        result = r.do(
            base.get('one'),
            base.get('two'),
            base.get('three'),
            lambda d1, d2, d3: [d1['name'], d2['name'], d3['name']]
        ).run(conn)
        assertEqual(['One', 'Two', 'Three'], result)


class TestSets(MockTest):
    @staticmethod
    def get_data():
        data = [
            {
                'id': 'one',
                'simple': ['x', 'y'],
                'complex': [{'val': 10}, {'val': 16}]
            },
            {
                'id': 'two',
                'simple': ['x', 'z'],
                'complex': [{'val': 10}]
            }
        ]
        return as_db_and_table('z', 't', data)

    def test_set_insert(self, conn):
        expected = [
            set(['x', 'y']),
            set(['x', 'y', 'z'])
        ]
        result = r.db('z').table('t').map(
            lambda doc: doc['simple'].set_insert('y')
        ).run(conn)
        result = map(lambda d: set(d), result)
        assertEqUnordered(expected, result)

    def test_set_union(self, conn):
        expected = [
            set(['x', 'y', 'a']),
            set(['x', 'y', 'z', 'a'])
        ]
        result = r.db('z').table('t').map(
            lambda doc: doc['simple'].set_union(['y', 'a'])
        ).run(conn)
        result = map(lambda d: set(d), result)
        assertEqUnordered(expected, result)

    def test_set_intersection(self, conn):
        expected = [
            set(['x', 'y']),
            set(['x'])
        ]
        result = r.db('z').table('t').map(
            lambda doc: doc['simple'].set_intersection(['x', 'y'])
        ).run(conn)
        result = map(lambda d: set(d), result)
        assertEqUnordered(expected, result)

    def test_set_difference(self, conn):
        expected = [
            set(['x']),
            set(['x', 'z'])
        ]
        result = r.db('z').table('t').map(
            lambda doc: doc['simple'].set_difference(['y'])
        ).run(conn)
        result = list(result)
        result = map(lambda d: set(d), result)
        assertEqUnordered(expected, result)


class TestObjectManip(MockTest):
    @staticmethod
    def get_data():
        data = [
            {
                'id': 'joe',
                'attributes': {
                    'face': 'bad',
                    'toes': 'fugly'
                },
                'joe-attr': True
            },
            {
                'id': 'sam',
                'attributes': {
                    'face': 'eh',
                    'blog': 'dry'
                },
                'sam-attr': True
            }
        ]
        return as_db_and_table('y', 'people', data)

    def test_keys_document(self, conn):
        expected = [
            ['id', 'attributes', 'joe-attr'],
            ['id', 'attributes', 'sam-attr']
        ]
        result = list(r.db('y').table('people').map(
            lambda d: d.keys()
        ).run(conn))
        assertEqual(3, len(result[0]))
        assertEqual(3, len(result[1]))
        key_set = set(util.cat(result[0], result[1]))
        assertEqual(set(['id', 'attributes', 'joe-attr', 'sam-attr']), key_set)


    def test_keys_nested(self, conn):
        expected = [
            ['face', 'toes'],
            ['face', 'blog']
        ]
        result = list(r.db('y').table('people').map(
            lambda d: d['attributes'].keys()
        ).run(conn))
        assertEqual(2, len(result[0]))
        assertEqual(2, len(result[1]))

        key_set = set(util.cat(result[0], result[1]))
        assertEqual(set(['face', 'toes', 'blog']), key_set)



class TestJson(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'one'},
            {'id': 'two'}
        ]
        return as_db_and_table('d', 't', data)
    def test_update_with_json(self, conn):
        expected = [
            {'id': 'one', 'nums': [1, 2, 3]},
            {'id': 'two', 'nums': [1, 2, 3]}
        ]
        result = r.db('d').table('t').map(
            lambda doc: doc.merge(r.json('{"nums": [1, 2, 3]}'))
        ).run(conn)
        assertEqUnordered(expected, list(result))

class TestReduce(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'one', 'points': 10},
            {'id': 'two', 'points': 25},
            {'id': 'three', 'points': 100},
            {'id': 'four', 'points': 50},
            {'id': 'five', 'points': 6}
        ]
        return as_db_and_table('d', 'nums', data)

    def test_reduce_1(self, conn):
        expected = 191
        result = r.db('d').table('nums').map(
            lambda doc: doc['points']
        ).reduce(
            lambda elem, acc: elem + acc
        ).run(conn)
        assertEqual(expected, result)



class TestBranch(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'one', 'value': 5},
            {'id': 'three', 'value': 22},
            {'id': 'two', 'value': 12},
            {'id': 'four', 'value': 31}
        ]
        return as_db_and_table('x', 't', data)

    def test_branch_1(self, conn):
        expected = [
            {'id': 'one', 'value': 5, 'over_20': False},
            {'id': 'three', 'value': 22, 'over_20': True},
            {'id': 'two', 'value': 12, 'over_20': False},
            {'id': 'four', 'value': 31, 'over_20': True}
        ]
        result = r.db('x').table('t').map(
            r.branch(
                r.row['value'] > 20,
                r.row.merge({'over_20': True}),
                r.row.merge({'over_20': False})
            )
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, list(result))

    def test_branch_syntax(self, conn):
        result = r.expr(True).branch('True', 'False').run(conn)
        assert result == 'True'
        result = r.expr(False).branch('True', 'False').run(conn)
        assert result == 'False'

    def test_branch_2n_1(self, conn):
        test_func = r.branch(
            r.row['value'] > 20,
            'more_than_20',
            r.row['value'] > 15,
            'more_than_15',
            r.row['value'] > 10,
            'more_than_10',
            r.row['value'] > 5,
            'more_than_5',
            'more_than_0'
        )
        result = r.expr({'value': 25}).do(test_func).run(conn)
        assert result == 'more_than_20'
        result = r.expr({'value': 17}).do(test_func).run(conn)
        assert result == 'more_than_15'
        result = r.expr({'value': 13}).do(test_func).run(conn)
        assert result == 'more_than_10'
        result = r.expr({'value': 8}).do(test_func).run(conn)
        assert result == 'more_than_5'
        result = r.expr({'value': 3}).do(test_func).run(conn)
        assert result == 'more_than_0'

class TestSync(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'x', 'name': 'x-name'},
            {'id': 'y', 'name': 'y-name'}
        ]
        return as_db_and_table('d', 'things', data)

    def test_sync(self, conn):
        expected = [
            {'id': 'x', 'name': 'x-name'},
            {'id': 'y', 'name': 'y-name'},
            {'id': 'z', 'name': 'z-name'}
        ]

        r.db('d').table('things').insert(
            {'id': 'z', 'name': 'z-name'}
        ).run(conn)
        r.db('d').table('things').sync().run(conn)
        result = r.db('d').table('things').run(conn)
        assertEqUnordered(expected, list(result))


class TestError(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'foo'}
        ]
        return as_db_and_table('db', 'fonz', data)

    def test_error1(self, conn):
        try:
            r.error('msg').run(conn)
        except RqlRuntimeError as err:
            rql_err = err
            assertEqual('msg', err.message)
        assert(isinstance(rql_err, RqlRuntimeError))

