import cubely.common
import cubely.errors
# -*- coding: utf-8 -*-
import unittest
import cubely
from cubely.lang import *
from cubely.errors import DimensionError

class  MiscTestCase(unittest.TestCase):
    def setUp(self):
        cubely.db.open('unittest')

    def tearDown(self):
        cubely.db.close()

    def test_1_isby(self):
        sales = cubely.V_SALES
        prod = cubely.D_PROD
        self.assertTrue(isby(sales, prod), 'check isby with correct dimension')
        self.assertTrue(isby(sales, 'GEOG'), 'check isby with correct code')
        channel = cubely.dim.create('CHANNEL', 'Channel dimension')
        self.assertFalse(isby(sales, channel), 'check isby with wrong dimension')
        self.assertFalse(isby(sales, 'CHANNEL'), 'check isby with wrong code')
        self.assertRaises(DimensionError, lambda: isby(sales, 'BOGUS'))

    def test_2_sort(self):
        prod = cubely.D_PROD
        lmt(prod, to, all)
        sorta(prod)
        self.assertEqual(prod.status[0].code, 'P1', 'check alphabetical sort')
        sorth(prod, 'STD')
        self.assertEqual(prod.status[0].code, 'TOTPROD', 'check hierarchical sort')

    def test_3_exists(self):
        self.assertFalse(exists('prod247'), 'check non existing object')
        self.assertTrue(exists('PROD'), 'check existing dim')
        self.assertTrue(exists('sales'), 'check existing cube')
        self.assertTrue(exists('SALESPLUS'), 'check existing formula')

if __name__ == '__main__':
    unittest.main()