import cubely.core
# -*- coding: utf-8 -*-
import unittest
import cubely
import cubely.errors

class  HierarchyTestCase(unittest.TestCase):
    def setUp(self):
        cubely.db.open('unittest')

    def tearDown(self):
        cubely.db.close()

    def test_0_create(self):
        hier = cubely.hier.create('PROD','STD')
        self.assertTrue(hier.__class__ == cubely.core.Hierarchy, 'Type checking')
        self.assertEqual(hier.code, 'STD', 'Check code')
        self.assertEqual(hier, cubely.HIERS['PROD']['STD'], 'check book keeping')

    def test_1_set(self):
        hier = cubely.HIERS['PROD']['STD']
        hier.set('P1', 'P2')
        totp = cubely.D_PROD.get('TOTPROD')
        self.assertEqual('P2', hier.links['P1'], 'Check parent value')
        self.assertEqual('P2', hier.get('P1'), 'Check parent value 2')
        hier.set('P1', 'TOTPROD')
        self.assertEqual(totp.code, hier.links['P1'], 'Check change parent value')

    def test_2_unset(self):
        hier = cubely.HIERS['PROD']['STD']
        hier.unset('P1')
        self.assertFalse(hier.links.has_key('P1'), 'Check unset position in hier')
        self.assertRaises(cubely.errors.HierarchyError, hier.unset, 'P2')

    def test_3_delete(self):
        cubely.hier.delete('PROD', 'STD')
        self.assertFalse(cubely.HIERS['PROD'].has_key('STD'), 'Check that hier is deleted')



if __name__ == '__main__':
    unittest.main()