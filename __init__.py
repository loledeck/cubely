# -*- coding: utf-8 -*-
"""cubely bootstrap file"""

import cubely
import cubely.lang

from pymongo import Connection

from cubely.core import Dimension
from cubely.core import Database
from cubely.core import Position
from cubely.core import Cube
from cubely.core import Formula
from cubely.core import Hierarchy
from cubely.core import Relation

from cubely.io import *
from cubely.demo import *
from cubely.lang import *

VERSION = '0.3'
CONNECTION = Connection()
COLLECTION = None

CURRENT_DB = ''   # name of the currently opened db

DIMS  = {}
HIERS = {}
CUBES = {}
FORMULAS = {}
DIMS_IN_USE = set()
MODIFIED_CUBES = set()

# *** Exposing shortcut objects for dml manipulations
db = Database()
dim = Dimension()
cube = Cube()
formula = Formula()
hier = Hierarchy()
relation = Relation()

# *** TEST RANGE *****
def build_sakila():
    cubely.demo.sakila.conf.build.sakila.run()

