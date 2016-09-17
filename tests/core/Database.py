# -*- coding: utf-8 -*-
import unittest
import cubely

class DatabaseTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass
    
    def test_1_exists(self):
        self.assertTrue(cubely.db.exists('unittest'))

    def test_2_open_close(self):
        testDb = cubely.db.open('unittest')
        self.assertEqual(cubely.CURRENT_DB, 'unittest', 'test open 1')
        self.assertTrue(testDb.__class__ == cubely.core.Database, 'test open 2')
        cubely.db.close()
        self.assertTrue(cubely.CURRENT_DB == '', 'test close 1')
        self.assertTrue(cubely.DIMS == {}, 'test close 2')
        self.assertTrue(cubely.HIERS == {}, 'test close 3')
        self.assertTrue(cubely.DIMS_IN_USE == set(), 'test close 4')
        self.assertTrue(cubely.CUBES == {}, 'test close 5')
        self.assertTrue(cubely.FORMULAS == {}, 'test close 6')

    def test_3_collections(self):
        self.assertEquals(type(cubely.db.get_collections()), list)
        self.assertTrue('meta' in cubely.db.get_collections())

    def test_4_clean(self):
        cubely.db.open('unittest')
        cubely.db.clean()

    def test_5_update(self):
        cubely.db.update()

if __name__ == '__main__':
    unittest.main()
