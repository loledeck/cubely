# -*- coding: utf-8 -*-
import unittest
import cubely
from cubely.lang import *

class  StatusTestCase(unittest.TestCase):
    def setUp(self):
        cubely.db.open('unittest')

    def tearDown(self):
        cubely.db.close()

    def test_status(self):
        prod = cubely.D_PROD
        lmt(prod, to, all)
        self.assertTrue(len(prod.status) == 4, 'check statlen = status len 1')
        self.assertTrue(statlen(prod) == 4, 'check statlen = status len 2')
        lmt(prod, to, first, 1)
        self.assertTrue(statlen(prod) == 1, 'check statlen after limit first 1')
        tmpPos = prod.status[0]
        lmt(prod, to, last, 1)
        self.assertTrue(statlen(prod) == 1, 'check statlen after limit last 1')
        self.assertNotEqual(tmpPos, prod.status[0], 'check status has changed between 2 lmt calls')
        lmt(prod, add, first, 1)
        self.assertTrue(statlen(prod) == 2, 'check statlen after limit add 1')
        self.assertEqual(tmpPos, prod.status[1], 'check the correct position is added')
        lmt(prod, to, 'TOTPROD')
        self.assertTrue(statlen(prod) == 1, 'check statlen after limit to 1')
        lmt(prod, add, descendants, 'STD')
        self.assertTrue(statlen(prod) == 4, 'check statlen after limit add descendants 1')
        lmt(prod, remove, 'P1')
        self.assertTrue(statlen(prod) == 3, 'check statlen after limit remove 1')
        lmt(prod, complement)
        self.assertTrue(statlen(prod) == 1, 'check statlen after limit complement 1')
        self.assertEqual(prod.status[0].code, 'P1')
        lmt(prod, to, ancestors, 'STD')
        self.assertTrue(statlen(prod) == 1, 'check statlen after limit ancestors 1')
        self.assertEqual(prod.status[0].code, 'TOTPROD')
        lmt(prod, add, children, 'STD')
        self.assertTrue(statlen(prod) == 4, 'check statlen after limit children 1')
        lmt(prod, to, 'P1')
        lmt(prod, to, parents, 'STD')
        self.assertTrue(statlen(prod) == 1, 'check statlen after limit parents 1')
        self.assertEqual(prod.status[0].code, 'TOTPROD')
        lmt(prod, to, all)
        self.assertTrue(statlen(prod) == 4, 'check statlen after limit all 1')

if __name__ == '__main__':
    unittest.main()