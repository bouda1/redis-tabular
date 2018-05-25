#!/usr/bin/python

import random
import unittest
from rmtest import ModuleTestCase

class TestRedisTabular(ModuleTestCase('../build/redistabular.so')):
    def testSortWithoutWindowWithoutCol(self):
        with self.assertResponseError():
            self.cmd('tabular.sort', 'test')

    def testSortWithoutCol(self):
        with self.assertResponseError():
            self.cmd('tabular.sort', 'test', 1, 30)

    def testSortWithASingleAlphaCol(self):
        for i in range(1, 100):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.sort', 'test', 0, 100, 'value', 'alpha'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(tab[i] <= tab[i + 1]);

    def testSortWithASingleNumericalCol(self):
        for i in range(1, 100):
            self.cmd('SADD', 'test', 's' + str(i))
            self.cmd('HSET', 's' + str(i), 'value', random.randint(0, 999))
        self.assertOk(self.cmd('tabular.sort', 'test', 0, 100, 'value', 'num'))
        tab = self.cmd('sort', 'services_sort', 'by', 'nosort', 'get', '*->value')
        for i in range(0, len(tab) - 1):
            self.assertTrue(int(tab[i]) <= int(tab[i + 1]));

if __name__ == '__main__':
    unittest.main()
