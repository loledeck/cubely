import unittest
import cubely
from cubely.tests.core.Database import DatabaseTestCase
from cubely.tests.core.Dimension import DimensionTestCase
from cubely.tests.core.Position import PositionTestCase
from cubely.tests.core.Hierarchy import HierarchyTestCase
from cubely.tests.core.Cube import CubeTestCase
from cubely.tests.core.Formula import FormulaTestCase
from cubely.tests.lang.status import StatusTestCase
from cubely.tests.lang.misc import MiscTestCase


def suite():
    suiteDb = unittest.TestLoader().loadTestsFromTestCase(DatabaseTestCase)
    suiteDim = unittest.TestLoader().loadTestsFromTestCase(DimensionTestCase)
    suitePos = unittest.TestLoader().loadTestsFromTestCase(PositionTestCase)
    suiteHie = unittest.TestLoader().loadTestsFromTestCase(HierarchyTestCase)
    suiteCub = unittest.TestLoader().loadTestsFromTestCase(CubeTestCase)
    suiteFrm = unittest.TestLoader().loadTestsFromTestCase(FormulaTestCase)
    suiteSta = unittest.TestLoader().loadTestsFromTestCase(StatusTestCase)
    suiteMsc = unittest.TestLoader().loadTestsFromTestCase(MiscTestCase)
    suite = unittest.TestSuite([
        suiteDb,
        suiteDim,
        suitePos,
        suiteHie,
        suiteCub,
        suiteFrm,
        suiteSta,
        suiteMsc
    ])
    return suite

try:
    cubely.db.drop('unittest')
except:
    pass

cubely.db.create('unittest', description='Temporary database for unit test')
cubely.db.close()

runner = unittest.TextTestRunner()
runner.run(suite())