# -*- coding: utf-8 -*-
import unittest
import cubely
import cubely.errors

class  DimensionTestCase(unittest.TestCase):

    def setUp(self):
        cubely.db.open('unittest')

    def tearDown(self):
        try:
            cubely.dim.delete('TEST')
        except:
            pass
        cubely.db.close()

    def test_1_create(self):
        prod = cubely.dim.create('PROD', 'Product dimension')
        geog = cubely.dim.create('GEOG', 'Geography dimension')
        time = cubely.dim.create('TIME', 'Time dimension')
        self.assertTrue(prod.__class__ == cubely.core.Dimension, 'dimension class')
        self.assertEqual(prod, cubely.DIMS['PROD'], 'dimension coherence 1')
        #self.assertEqual(prod, cubely.D_PROD, 'dimension coherence 2')
        self.assertNotEqual(prod, geog, 'check dimensions copies 1')
        self.assertNotEqual(prod, time, 'check dimensions copies 2')
        self.assertRaises(cubely.errors.DimensionError, cubely.dim.create, 'PROD', 'another prod dimension')

    def test_2_delete(self):
        test = cubely.dim.create('TEST', 'Test dim')
        test.add_position('TESTPOS')
        cubely.dim.delete('TEST')
        self.assertFalse('TEST' in cubely.DIMS.keys(), 'dimension cleanup 1')
        self.assertFalse(hasattr(cubely, 'D_TEST'), 'dimension cleanup 2')


    def test_3_position(self):
        prod = cubely.D_PROD
        self.assertFalse(prod.has_position('TOTPROD'))
        self.assertFalse(prod.has_position('BOGUS'))
        prod.add_position('TOTPROD')
        prod.add_position('BOGUS')
        totProd = prod.get('TOTPROD')
        bogus = prod.get('BOGUS')
        self.assertTrue(totProd.__class__ == cubely.core.Position)
        self.assertTrue(bogus.__class__ == cubely.core.Position)
        prod.delete_position('BOGUS')
        self.assertFalse(prod.has_position('BOGUS'))
        geog = cubely.D_GEOG
        geog.add_position('G1')
        geog.add_position('G2')
        time = cubely.D_TIME
        time.add_position('JAN')

    def test_4_status(self):
        prod = cubely.D_PROD
        prod.status_set(['TOTPROD'])
        self.assertEqual(cubely.lang.statlen(prod), 1, 'dimension initial status')
        p1 = prod.add_position('P1')
        self.assertTrue(p1.__class__ == cubely.core.Position)
        self.assertEqual(cubely.lang.statlen(prod), 2, 'dimension after created position')
        p2 = prod.add_position('P2')
        p3 = prod.add_position('P3')
        prod.status_clear()
        self.assertEqual(cubely.lang.statlen(prod), 0, 'dimension status set to null')
        prod.status_add([p1, p2])
        self.assertEqual(cubely.lang.statlen(prod), 2, 'dimension status add 2 pos')
        prod.status_add(['TOTPROD'])
        self.assertEqual(cubely.lang.statlen(prod), 3, 'dimension status add code')
        prod.status_keep([p2])
        self.assertEqual(cubely.lang.statlen(prod), 1, 'dimension status keep pos')
        prod.status_set([p3])
        self.assertEqual(cubely.lang.statlen(prod), 1, 'dimension status set pos')
        prod.status_remove(['P3'])
        self.assertEqual(cubely.lang.statlen(prod), 0, 'dimension status remove code')
        prod.status_all()
        self.assertEqual(cubely.lang.statlen(prod), 4, 'allstat prod')
        cubely.D_GEOG.status_all()
        self.assertEqual(cubely.lang.statlen(cubely.D_GEOG), 2, 'allstat geog')
        cubely.DIMS['TIME'].status_all()
        self.assertEqual(cubely.lang.statlen(cubely.DIMS['TIME']), 1, 'allstat time')


if __name__ == '__main__':
    unittest.main()

