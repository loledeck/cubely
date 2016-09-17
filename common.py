# -*- coding: utf-8 -*-
"""Common helper functions for cubely"""

import cubely.core
import cubely
import itertools
from time import asctime, localtime
from cubely.errors import CubeError

# *** Global functions
def reset_global_vars():
    for dim in cubely.DIMS.keys():
        temp = 'cubely.D_'+ dim
        exec('del('+temp+')')
    for cube in cubely.CUBES.keys():
        temp = 'cubely.V_'+ cube
        exec('del('+temp+')')
    for form in cubely.FORMULAS.keys():
        temp = 'cubely.F_'+ form
        exec('del('+temp+')')
    cubely.CURRENT_DB = ''
    cubely.DIMS = {}
    cubely.HIERS = {}
    cubely.DIMS_IN_USE = set()
    cubely.CUBES = {}
    cubely.FORMULAS = {}


def get_collection_name(objectType, parentCode = False, objectCode = False):
    salt = '_cubely_'
    collectionName = salt + objectType.lower()
    if parentCode:
        collectionName = collectionName + '_' + parentCode.lower()
    if objectCode:
        collectionName = collectionName + '_' + objectCode.lower()
    return collectionName


def is_numeric(s):
    try:
      i = float(s)
    except ValueError:
        return False
    except TypeError:
        return False
    else:
        return True


def delete_meta_simple(type, code):
    specs = cubely.db.metas.find_one({'code': type})
    index = 0
    for existing in specs['value']:
        if existing['code'] == code:
            break
        index += 1
    if specs['value'][index]:
        del specs['value'][index]
    cubely.db.metas.update({'code': type}, specs)


def delete_meta_double(type, criteria, criteriaValue, code):
    specs = cubely.db.metas.find_one({'code': type})
    index = 0
    for existing in specs['value']:
        if existing['code'] == code and existing[criteria] == criteriaValue:
            break
        index += 1
    if specs['value'][index]:
        del specs['value'][index]
    cubely.db.metas.update({'code': type}, specs)


def invert_dict(dic):
    return dict(map(lambda item: (item[1],item[0]), dic.items()))


def find_keys(dic, val):
    """return the key of dictionary dic given the value"""
    return [k for k, v in dic.iteritems() if v == val]

def get_dim_object(dim):
    if dim.__class__ == cubely.core.Dimension:
        dimObj = dim
    elif dim.__class__ in [str, unicode]:
        try:
            dimObj = cubely.DIMS[dim]
        except KeyError:
            raise cubely.errors.DimensionError(dim, 'Dimension does not exist')
    else:
        raise ValueError
    return dimObj

def get_hier_object(dim, hier):
    dimObj = get_dim_object(dim)
    if hier.__class__ == cubely.core.Hierarchy:
        hierObj = hier
    elif hier.__class__ in [str, unicode]:
        hierObj = cubely.HIERS[dimObj.code][hier]
    else:
        raise ValueError
    return hierObj

def check_dim_is_in_cube(obj, dim):
        if dim.code in obj.dimensions:
            return True
        else:
            return False

def cube_log(message, level=1, revertTabs=False):
    lvlMsg = ''
    if revertTabs:
        tabChar = '<'
    else:
        tabChar = '>'
    for i in range(0, level):
        lvlMsg = lvlMsg + tabChar
    print asctime(localtime()) + ' ' + lvlMsg + ' ' + message

def product(*args, **kwds):
    # product('ABCD', 'xy') --> Ax Ay Bx By Cx Cy Dx Dy
    # product(range(2), repeat=3) --> 000 001 010 011 100 101 110 111
    pools = map(tuple, args) * kwds.get('repeat', 1)
    result = [[]]
    for pool in pools:
        result = [x+[y] for x in result for y in pool]
    for prod in result:
        yield tuple(prod)

def get_status_nuplet(dimList):
    dimStatus = {}
    positionsToCompute = []
    queryList = []
    dimIndex = 0
    for dim in dimList:
        dimObj = cubely.common.get_dim_object(dim)
        dimStatus[dimObj.code] = [pos.code for pos in dimObj.status]
    try:
        # Python 2.6+ version
        [positionsToCompute.append(el) for el in itertools.product(*dimStatus.values())]
    except AttributeError:
        # Python 2.5 version
        [positionsToCompute.append(el) for el in cubely.common.product(*dimStatus.values())]
    #print positionsToCompute
    for nuplet in positionsToCompute:
        dimIndex = 0
        tmpDict = {}
        for element in nuplet:
            tmpDict[dimStatus.keys()[dimIndex]] = element
            dimIndex += 1
        queryList.append(tmpDict)
    return queryList

def get_position_tuple(posDict, silent_error=False):
    dimsTup = []
    for d in posDict.keys():
        if cubely.DIMS[d].positions.has_key(posDict[d]):
            dimsTup.append(posDict[d])
        else:
            if silent_error:
                pass
            else:
                raise CubeError(posDict[d], 'Invalid position')
    return tuple(dimsTup)