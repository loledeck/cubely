# -*- coding: utf-8 -*-
import unittest
import cubely
import cubely.errors


class  PositionTestCase(unittest.TestCase):
    def setUp(self):
        cubely.db.open('unittest')

    def tearDown(self):
        cubely.db.close()

    def test_position(self):
        self.assertEqual(cubely.CURRENT_DB, 'unittest', 'check test db is available')
        prod = cubely.DIMS['PROD']
        self.assertEqual(prod.__class__, cubely.core.Dimension, 'type chekup 1')
        p1 = prod.add_position('unittest1', 'desc unittest1')
        self.assertEqual(p1.__class__, cubely.core.Position, 'type chekup 2')
        self.assertEqual(p1, prod.positions['unittest1'], 'newly created position in dim.positions')
        self.assertEqual(p1.dimension, 'PROD', 'check postion dimension')
        p1.delete()
        self.assertRaises(cubely.errors.DimensionError, prod.get, 'unittest1')


if __name__ == '__main__':
    unittest.main()