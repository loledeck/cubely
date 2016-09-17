# -*- coding: utf-8 -*-
import cubely.lang
from cubely.lang import dyn_aggregate
import unittest
import cubely
import cubely.errors

class  CubeTestCase(unittest.TestCase):
    def setUp(self):
        cubely.db.open('unittest')

    def tearDown(self):
        cubely.db.close()

    def test_1_create(self):
        sales = cubely.cube.create(['PROD', 'GEOG', 'TIME'], 'int', 'SALES', 'Sales figures')
        self.assertEqual(sales.__class__, cubely.core.Cube, 'Check types')
        self.assertEqual(sales, cubely.CUBES['SALES'], 'Check book keeping 1')
        self.assertEqual(sales, cubely.V_SALES, 'Check book keeping 2')

    def test_2_setget(self):
        sales = cubely.V_SALES
        sales.set({'PROD': 'P1', 'GEOG': 'G1', 'TIME':'JAN'}, 10)
        self.assertEqual(sales.get({'PROD': 'P1', 'GEOG': 'G1', 'TIME':'JAN'}), 10, 'check get = set')
        sales.update()

    def test_3_getafeterupdate(self):
        sales = cubely.V_SALES
        self.assertEqual(sales.get({'PROD': 'P1', 'GEOG': 'G1', 'TIME':'JAN'}), 10, 'check get = set after update')

    def test_4_rollback(self):
        sales = cubely.V_SALES
        sales.set({'PROD': 'P1', 'GEOG': 'G1', 'TIME':'JAN'}, 3)
        self.assertEqual(len(sales.changedValues), 1, 'check changedValues modification')
        sales.rollback()
        self.assertEqual(len(sales.changedValues), 0, 'check changedValues modification')

    def test_5_aggregate(self):
        sales = cubely.V_SALES
        sales.set({'PROD': 'P2', 'GEOG': 'G1', 'TIME':'JAN'}, 20)
        sales.set({'PROD': 'P3', 'GEOG': 'G1', 'TIME':'JAN'}, 30)
        hier = cubely.hier.create('PROD','STD')
        hier.set('P1', 'TOTPROD')
        hier.set('P2', 'TOTPROD')
        hier.set('P3', 'TOTPROD')
        sales.update()
        cubely.lang.aggregate(sales)
        self.assertEqual(sales.get({'PROD': 'TOTPROD', 'GEOG': 'G1', 'TIME':'JAN'}), 60, 'check simple aggregation')
        rent = cubely.cube.create(['PROD', 'GEOG', 'TIME'], 'int', 'RENTALS', 'Rentals figures')
        rent.set({'PROD': 'P2', 'GEOG': 'G1', 'TIME':'JAN'}, 20)
        rent.set({'PROD': 'P3', 'GEOG': 'G1', 'TIME':'JAN'}, 30)
        rent_dyn_total = dyn_aggregate(rent, {'PROD': 'TOTPROD', 'GEOG': 'G1', 'TIME':'JAN'}, {'PROD':'STD'})
        self.assertEqual(rent_dyn_total, 50, 'check simple dynamic aggregation')


    def test_6_delete(self):
        cubely.cube.delete('SALES')
        self.assertTrue('SALES' not in cubely.CUBES.keys(), 'check book keeping before close 1')
        self.assertFalse(hasattr(cubely, 'V_SALES'), 'check book keeping before close 2')
        cubely.db.close()
        cubely.db.open('unittest')
        self.assertTrue('SALES' not in cubely.CUBES.keys(), 'check book keeping after close 1')
        self.assertFalse(hasattr(cubely, 'V_SALES'), 'check book keeping after close 2')


if __name__ == '__main__':
    unittest.main()