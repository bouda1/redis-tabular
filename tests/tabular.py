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
    def testSortWithoutWindowWithoutCol(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test')

    def testSortWithoutCol(self):
        tab = self.cmd('tabular.get', 'test', 1, 30)
        self.assertTrue(len(tab) == 0)

    def testSortWithASingleAlphaCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'store', 'services_sort', 'sort', 1, 'value', 'alpha'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(tab[i] <= tab[i + 1]);

    def testSortWithASingleNumericalCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'store', 'services_sort', 'sort', 1, 'value', 'num'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(int(tab[i]) <= int(tab[i + 1]));

    def testSortWithASingleRevAlphaCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'store', 'services_sort', 'sort', 1, 'value', 'revalpha'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(tab[i] >= tab[i + 1]);

    def testSortWithASingleRevNumericalCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'store', 'services_sort', 'sort', 1, 'value', 'revnum'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(int(tab[i]) >= int(tab[i + 1]));

    def testSortSimpleWithWindow(self):
        for i in range(1, 20):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', i)
        self.assertOk(self.cmd('tabular.get', 'test', 10, 15, 'store', 'services_sort', 'SORT', 1, 'value', 'num'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(int(tab[i]) == 11 + i)

    def testSortWithTwoColsAlphaNum(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i), 'value', random.randint(0, 999),
                    'name', 'Descr' + str(random.randint(0, 1000)))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'store', 'services_sort', 'SORT', 2, 'name', 'alpha', 'value', 'num'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        for i in range(0, len(tab) - 2, 2):
            self.assertTrue(le(tab, 'an', i, i + 2))
        self.assertOk(self.cmd('tabular.get', 'test', 300, 330, 'store', 'services_sort', 'SORT', 2, 'name', 'alpha', 'value', 'num'))
        tab1 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        for i in range(0, 60):
            self.assertEqual(tab[600 + i], tab1[i])

    def testFilterWithOneCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i),
                    'name', 'Descr' + str(i))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'store', 'services_sort', 'FILTER', 1, 'name', 'Descr*1*'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name')
        prog = re.compile('Descr.*1')
        for i in range(0, len(tab)):
            self.assertTrue(prog.match(tab[i]))
        self.assertEqual(len(tab), 271)

    def testFilterWithTwoCols(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i), 'value', 2 * i,
                    'name', 'Descr' + str(i))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'store', 'services_sort', 'FILTER', 2, 'name', 'Descr*1*',
                               'value', '*2*'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        prog = re.compile('Descr.*1')
        for i in range(0, len(tab), 2):
            self.assertTrue(prog.match(tab[i]))
        prog = re.compile('.*2')
        for i in range(1, len(tab), 2):
            self.assertTrue(prog.match(tab[i]))
        self.assertEqual(len(tab), 406)

    def testFilterWithTwoCols2(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i), 'value', random.randint(0, 1000),
                    'name', 'Descr' + str(i))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000,
                               'store', 'services_sort',
                               'SORT', 2, 'name', 'alpha', 'value', 'num',
                               'FILTER', 2, 'name', 'Descr*1*', 'value', '*2*'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        self.assertOk(self.cmd('tabular.get', 'test', 30, 60,
                               'store', 'services_sort',
                               'SORT', 2, 'name', 'alpha', 'value', 'num',
                               'FILTER', 2, 'name', 'Descr*1*', 'value', '*2*'))
        tab1 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        for i in range(0, 62):
            self.assertEqual(tab[60 + i], tab1[i])

    def testFilterTwoColsSortBadArity(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i), 'value', random.randint(0, 1000),
                     'name', 'Descr' + str(i))
            with self.assertResponseError():
                self.cmd('tabular.get', 'test', 0, 1000,
                         'store', 'services_sort',
                         'SORT', 1, 'name', 'alpha', 'value', 'num',
                         'FILTER', 2, 'name', 'Descr*1*', 'value', '*2*')

    def testFilterTwoColsFilterBadArity(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i), 'value', random.randint(0, 1000),
                     'name', 'Descr' + str(i))
            with self.assertResponseError():
                self.cmd('tabular.get', 'test', 0, 1000,
                         'store', 'services_sort',
                         'SORT', 2, 'name', 'alpha', 'value', 'num',
                         'FILTER', 1, 'name', 'Descr*1*', 'value', '*2*')

    def testFilterWithTwoColsOrderNoImpact(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i), 'value', random.randint(0, 1000),
                    'name', 'Descr' + str(i))
        self.assertOk(self.cmd('tabular.get', 'test', 30, 60,
                               'store', 'services_sort',
                               'FILTER', 2, 'name', 'Descr*1*', 'value', '*2*',
                               'SORT', 2, 'name', 'alpha', 'value', 'num'))
        tab0 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        self.assertOk(self.cmd('tabular.get', 'test', 30, 60,
                               'store', 'services_sort',
                               'FILTER', 2, 'value', '*2*', 'name', 'Descr*1*',
                               'SORT', 2, 'name', 'alpha', 'value', 'num'))
        tab1 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        self.assertOk(self.cmd('tabular.get', 'test', 30, 60,
                               'store', 'services_sort',
                               'SORT', 2, 'name', 'alpha', 'value', 'num',
                               'FILTER', 2, 'value', '*2*', 'name', 'Descr*1*'))
        tab2 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        self.assertOk(self.cmd('tabular.get', 'test', 30, 60,
                               'SORT', 2, 'name', 'alpha', 'value', 'num',
                               'FILTER', 2, 'value', '*2*', 'name', 'Descr*1*',
                               'store', 'services_sort'))
        tab3 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        for i in range(0, 62):
            self.assertEqual(tab0[i], tab1[i])
            self.assertEqual(tab1[i], tab2[i])
            self.assertEqual(tab2[i], tab3[i])

if __name__ == '__main__':
    unittest.main()
