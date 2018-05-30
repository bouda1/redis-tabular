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
            self.cmd('tabular.get', 'test', 1, 30, 'filter')
    def testParseArgvFilterNotFinished1(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'filter', 2)
    def testParseArgvFilterNotFinished2(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'filter', 1, 'name')
    def testParseArgvSortNotFinished(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'sort')
    def testParseArgvSortNotFinished1(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'sort', 2)
    def testParseArgvSortNotFinished2(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'sort', 1, 'name')
    def testParseArgvSortNotFinished3(self):
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30, 'sort', 1, 'name', 'foo')
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
        self.assertTrue(len(tab) == 0)

    def testGetWithHSetAsSet(self):
        for i in range(1, 10):
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        with self.assertResponseError():
            self.assertOk(self.cmd('tabular.get', 's1', 1000, 0, 'store', 'services_sort', 'sort', 1, 'value', 'revalpha'))

    def testGetWithASingleRevAlphaCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 1000, 0, 'store', 'services_sort', 'sort', 1, 'value', 'revalpha'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(tab[i] >= tab[i + 1]);

    def testGetWindowOutsideRange(self):
        for i in range(1, 30):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        tab = self.cmd('tabular.get', 'test', 1000, 1030, 'sort', 1, 'value', 'revalpha')
        self.assertTrue(len(tab) == 0)

    def testGetWindowWithoutStore(self):
        for i in range(1, 30):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        tab = self.cmd('tabular.get', 'test', 1, 1030, 'sort', 1, 'value', 'revalpha')
        self.assertTrue(len(tab) == 28)

    def testGetWithASingleRevNumericalCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'store', 'services_sort', 'sort', 1, 'value', 'revnum'))
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

    def testSortWithBadNum(self):
        for i in range(1, 30):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        tab = self.cmd('tabular.get', 'test', 1, 1030, 'sort', 1, 'name', 'num')
        self.assertTrue(len(tab) == 28)

    def testSortWithBadStr(self):
        for i in range(1, 30):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        tab = self.cmd('tabular.get', 'test', 1, 1030,
                       'sort', 1, 'name', 'alpha')
        self.assertTrue(len(tab) == 28)

if __name__ == '__main__':
    unittest.main()
