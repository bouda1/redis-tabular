#!/usr/bin/python

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
        with self.assertResponseError():
            self.cmd('tabular.get', 'test', 1, 30)

    def testSortWithASingleAlphaCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'sort', 'value', 'alpha'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(tab[i] <= tab[i + 1]);

    def testSortWithASingleNumericalCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'sort', 'value', 'num'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(int(tab[i]) <= int(tab[i + 1]));

    def testSortWithASingleRevAlphaCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'sort', 'value', 'revalpha'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(tab[i] >= tab[i + 1]);

    def testSortWithASingleRevNumericalCol(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'sort', 'value', 'revnum'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(int(tab[i]) >= int(tab[i + 1]));

    def testSortSimpleWithWindow(self):
        for i in range(1, 20):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', i)
        self.assertOk(self.cmd('tabular.get', 'test', 10, 15, 'SORT', 'value', 'num'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(int(tab[i]) == 11 + i)

    def testSortWithTwoColsAlphaNum(self):
        for i in range(1, 1000):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HMSET', 's' + str(i), 'value', random.randint(0, 999),
                    'name', 'Descr' + str(random.randint(0, 1000)))
        self.assertOk(self.cmd('tabular.get', 'test', 0, 1000, 'SORT', 'name', 'alpha', 'value', 'num'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
        for i in range(0, len(tab) - 2, 2):
            self.assertTrue(le(tab, 'an', i, i + 2))
        self.assertOk(self.cmd('tabular.get', 'test', 300, 330, 'SORT', 'name', 'alpha', 'value', 'num'))
#        tab1 = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->name', 'get', '*->value')
#        for i in range(0, 60):
#            self.assertEqual(tab[600 + i], tab1[i])

if __name__ == '__main__':
    unittest.main()
