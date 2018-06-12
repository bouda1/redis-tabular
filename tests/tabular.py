#!/usr/bin/python

import re
import random
import unittest
from rmtest import ModuleTestCase

def le(array, t, i, j):
    for c in t:
        if c == 'a':
            if array[i] != array[j]:
                return array[i] < array[j]
        elif c == 'A':
            if array[i] != array[j]:
                return array[i] > array[j]
        elif c == 'n':
            a, b = int(array[i]), int(array[j])
            if a != b:
                return a < b
        elif c == 'N':
            a, b = int(array[i]), int(array[j])
            if a != b:
                return a > b
        i += 1
        j += 1
    return True

class TestRedisTabular(ModuleTestCase('../build/redistabular.so')):
    def testParseArgvStore(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'store')
    def testParseArgvFilterNotFinished(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'FILTER')
    def testParseArgvFilterNotFinished1(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'FILTER', 2)
    def testParseArgvFilterNotFinished2(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'FILTER', 1, 'name')
    def testParseArgvFilterNotFinished3(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'FILTER', 1, 'name', 'MATCH')
    def testParseArgvFilterNotFinished4(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'FILTER', 1, 'name', '*a*')
    def testParseArgvSortNotFinished(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'SORT')
    def testParseArgvSortNotFinished1(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'SORT', 2)
    def testParseArgvSortNotFinished2(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'SORT', 1, 'name')
    def testParseArgvSortNotFinished3(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'SORT', 1, 'name', 'foo')
    def testGetWithBadArg2(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 'foo', 'bar')

    def testGetWithBadArg3(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 0, 'bar')

    def testGetWithoutWindowWithoutCol(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test')

    def testGetWithoutCol(self):
        tab = self.cmd('tabular.get', 'test', 1, 30)
        self.assertTrue(len(tab) == 1)
        self.assertTrue(tab[0] == 0)

    def testGetWithHSetAsSet(self):
        for i in range(1, 10):
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        with self.assertResponseError():
            self.assertOk(self.cmd('tabular.get', 's1', 1000, 0, 'store', 'services_sort', 'SORT', 1, 'value', 'revalpha'))

    def testGetWithASingleRevAlphaCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 1000, 0, 'store', 'services_sort', 'SORT', 1, 'value', 'revalpha'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(tab[i] >= tab[i + 1]);

    def testGetWindowOutsideRange(self):
        for i in range(1, 30):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        tab = self.cmd('tabular.get', 'test', 1000, 1030, 'SORT', 1, 'value', 'revalpha')
        self.assertTrue(len(tab) == 1 and tab[0] == 0)

    def testGetWindowWithoutStore(self):
        for i in range(1, 30):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        tab = self.cmd('tabular.get', 'test', 1, 1030, 'SORT', 1, 'value', 'revalpha')
        self.assertTrue(len(tab) == 29)
        self.assertTrue(tab[0] == 29)

    def testGetWithASingleRevNumericalCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'store', 'services_sort', 'SORT', 1, 'value', 'revnum'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(int(tab[i]) >= int(tab[i + 1]));

    def testFilterTwoColsFilterBadArity(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i), 'value', random.randint(0, 1000),
                     'name', 'Descr' + str(i))
            with self.assertResponseError():
                self.cmd('tabular.get', 'test', 0, 1000,
                         'STORE', 'services_sort',
                         'SORT', 2, 'name', 'alpha', 'value', 'num',
                         'FILTER', 1, 'name', 'MATCH', 'Descr*1*', 'value', 'MATCH', '*2*')

    def testFilterWithTwoColsOrderNoImpact(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i), 'value', random.randint(0, 1000),
                    'name', 'Descr' + str(i))
        self.assertOk(self.cmd('tabular.get', 'test', 30, 60,
                               'STORE', 'services_sort',
                               'FILTER', 2, 'name', 'MATCH', 'Descr*1*', 'value', 'MATCH', '*2*',
                               'SORT', 2, 'name', 'alpha', 'value', 'num'))
        tab0 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        self.assertOk(self.cmd('tabular.get', 'test', 30, 60,
                               'STORE', 'services_sort',
                               'FILTER', 2, 'value', 'MATCH', '*2*', 'name', 'MATCH', 'Descr*1*',
                               'SORT', 2, 'name', 'alpha', 'value', 'num'))
        tab1 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        self.assertOk(self.cmd('tabular.get', 'test', 30, 60,
                               'STORE', 'services_sort',
                               'SORT', 2, 'name', 'alpha', 'value', 'num',
                               'FILTER', 2, 'value', 'MATCH', '*2*', 'name', 'MATCH', 'Descr*1*'))
        tab2 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        self.assertOk(self.cmd('tabular.get', 'test', 30, 60,
                               'SORT', 2, 'name', 'alpha', 'value', 'num',
                               'FILTER', 2, 'value', 'MATCH', '*2*', 'name', 'MATCH', 'Descr*1*',
                               'STORE', 'services_sort'))
        tab3 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        for i in range(0, 62):
            self.assertEqual(tab0[i], tab1[i])
            self.assertEqual(tab1[i], tab2[i])
            self.assertEqual(tab2[i], tab3[i])

    def testFilterEqual(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i), 'value', random.randint(0, 4),
                    'name', 'Descr' + str(i))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 60,
                               'STORE', 'services_sort',
                               'FILTER', 2, 'name', 'MATCH', 'Descr*1*', 'value', 'EQUAL', '2',
                               'SORT', 2, 'name', 'alpha', 'value', 'num'))
        tab0 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab0)):
            self.assertEqual(tab0[i], '2')

    def testFilterIn(self):
        for i in range(1, 1001):
            name = 'Descr' + str(i)
            if i % 2 != 0:
                self.cmd('SADD', 'bag', name)
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i), 'value', random.randint(0, 4),
                    'name', name)
        self.assertOk(self.cmd('TABULAR.GET', 'test', 0, 60,
                               'STORE', 'services_sort',
                               'FILTER', 1, 'name', 'IN', 'bag'))
        size = self.cmd('get', 'services_sort:size')
        self.assertEqual(size, '500')
        tab0 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name')
        for i in range(0, len(tab0)):
            self.assertEqual(self.cmd('sismember', 'bag', tab0[i]), 1)

    def testCountOnWindow(self):
        for i in range(1, 151):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.cmd('tabular.get', 'test', 1, 1030, 'SORT', 1, 'name', 'num', 'store', 'foo')
        tab1 = self.cmd('get', 'foo:size')

        self.cmd('tabular.get', 'test', 100, 130, 'SORT', 1, 'name', 'num', 'store', 'bar')
        tab2 = self.cmd('get', 'bar:size')
        self.assertEqual(tab1, tab2)

    def testSortWithBadNum(self):
        for i in range(1, 30):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        tab = self.cmd('tabular.get', 'test', 1, 1030, 'SORT', 1, 'name', 'num')
        self.assertTrue(len(tab) == 29)
        self.assertTrue(tab[0] == 29)

    def testSortWithBadStr(self):
        for i in range(1, 300):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        tab = self.cmd('tabular.get', 'test', 1, 1030,
                       'SORT', 1, 'name', 'alpha')
        self.assertTrue(len(tab) == 299)
        self.assertTrue(tab[0] == 299)

    def testCountEmptySet(self):
        tab = self.cmd('tabular.count', 'test', 'FILTER', 1, 'value', 'MATCH', '1')
        self.assertEqual(tab, None)

    def testCountBadArity(self):
        with self.assertResponseError():
            self.cmd('tabular.count', 'test')

    def testCountBadSet(self):
        self.cmd('set', 'test', 'foobar')
        with self.assertResponseError():
            self.cmd('tabular.count', 'test', 'filter', 1, 'test', 'EQUAL', '1')

    def testCountWithSort(self):
        for i in range(1, 300):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', i % 2)
        with self.assertResponseError():
            self.cmd('tabular.count', 'test', 'filter', 1, 'value', 'EQUAL', '1', 'SORT', 1, 'value', 'NUM')

    def testCountEmptySetStore(self):
        tab = self.cmd('tabular.count', 'test', 'FILTER', 1, 'value', 'MATCH', '1', 'STORE', 'test_count')
        self.assertEqual(tab, None)

    def testCountSimple(self):
        for i in range(1, 300):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', i % 2)
        tab = self.cmd('tabular.count', 'test', 'FILTER', 1, 'value', 'MATCH', '1')
        self.assertEqual(tab[0], 'value')
        self.assertEqual(tab[1], '1')
        self.assertEqual(tab[2], 'count')
        self.assertEqual(tab[3], 150L)
        self.assertEqual(tab[4], 'children')
        self.assertEqual(tab[5], None)

    def testCountMulti(self):
        for i in range(1, 300):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'state', i % 4)
            self.cmd('HSET', 's' + str(i), 'value', i % 2)
        tab = self.cmd('tabular.count', 'test', 'FILTER', 2, 'state', 'MATCH', '[0-3]', 'value', 'EQUAL', '1')

    def testCountSimpleStore(self):
        for i in range(1, 300):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', i % 2)
        tab = self.cmd('tabular.count', 'test', 'FILTER', 1, 'value', 'MATCH', '1', 'STORE', 'simple_count')
        tab = self.cmd('get', 'simple_count:count:1')
        self.assertEqual(tab, '150')

    def testCountMultiStore(self):
        for i in range(1, 301):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'state', i % 4)
            self.cmd('HSET', 's' + str(i), 'value', i % 2)
            self.cmd('tabular.count', 'test', 'FILTER', 2, 'state', 'MATCH', '[0-3]', 'value', 'EQUAL', '1', 'STORE', 'multi_test')
        tab = self.cmd('mget', 'multi_test:count:0', 'multi_test:count:1', 'multi_test:count:2', 'multi_test:count:3')
        self.assertEqual(tab[0], '75')
        self.assertEqual(tab[1], '75')
        self.assertEqual(tab[2], '75')
        self.assertEqual(tab[3], '75')
        tab = self.cmd('mget', 'multi_test:count:1:1', 'multi_test:count:3:1')
        self.assertEqual(tab[0], '75')
        self.assertEqual(tab[1], '75')

    def testCountMultiStoreWithStrings(self):
        self.cmd('sadd', 'rows', 'r:1', 'r:2', 'r:3', 'r:4',  'r:5')
        self.cmd('hmset', 'r:1', 'status', '0', 'name', 'Mizar', 'location', 'Tours')
        self.cmd('hmset', 'r:2', 'status', '0', 'name', 'Altair', 'location', 'Lyon')
        self.cmd('hmset', 'r:3', 'status', '1', 'name', 'Arctarus', 'location', 'Agen')
        self.cmd('hmset', 'r:4', 'status', '3', 'name', 'Vega', 'location', 'Bordeaux')
        self.cmd('hmset', 'r:5', 'status', '4', 'name', 'Vega', 'location', 'Versailles')
        self.cmd('tabular.count', 'rows', 'filter', '2', 'status', 'MATCH', '[0-4]', 'name', 'EQUAL', 'Vega', 'STORE', 'test')
        tab = self.cmd('keys', 'test:count:*')
        self.assertTrue('test:count:3:V' not in tab)
        self.assertTrue('test:count:4:V' not in tab)
        self.assertTrue('test:count:3:Vega' in tab)
        self.assertTrue('test:count:4:Vega' in tab)

if __name__ == '__main__':
    unittest.main()
