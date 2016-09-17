# -*- coding: utf-8 -*-
import unittest
import cubely
from cubely.lang import *


class  FormulaTestCase(unittest.TestCase):
    def setUp(self):
        cubely.db.open('unittest')

    def tearDown(self):
        cubely.db.close()

    def test_1_create_and_test(self):
        sales = cubely.cube.create(['PROD', 'GEOG', 'TIME'], 'int', 'SALES', 'Sales figures')
        sales = cubely.V_SALES
        sales.set({'PROD': 'P1', 'GEOG': 'G1', 'TIME':'JAN'}, 10)
        sales.update()

        self.assertEqual(sales.get({'PROD': 'P1', 'GEOG': 'G1', 'TIME':'JAN'}), 10, 'check cube original value')
        form = cubely.formula.create('SALESPLUS', '@V_SALES + 2', 'test formula')
        self.assertEqual(form.get({'PROD': 'P1', 'GEOG': 'G1', 'TIME':'JAN'}), 12, 'check formula value')
        #self.assertEqual(sales.get({'PROD': 'P1', 'GEOG': 'G1', 'TIME':'JAN'}), 10, 'check cube original value again')

    def test_2_afterReopen(self):
        sales = cubely.V_SALES
        self.assertEqual(sales.get({'PROD': 'P1', 'GEOG': 'G1', 'TIME':'JAN'}), 10, 'check cube original value')
        form = cubely.F_SALESPLUS
        self.assertEqual(form.get({'PROD': 'P1', 'GEOG': 'G1', 'TIME':'JAN'}), 12, 'check formula value')
        self.assertEqual(sales.get({'PROD': 'P1', 'GEOG': 'G1', 'TIME':'JAN'}), 10, 'check cube original value again')
        #pass
    
if __name__ == '__main__':
    unittest.main()