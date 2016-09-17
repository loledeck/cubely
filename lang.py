# -*- coding: utf-8 -*-
"""DML language for cubely"""
import cubely.common
import cubely.core

# *** Imports
import cubely
from cubely.core import *
from cubely.errors import *
from cubely.common import find_keys
from cubely.common import get_dim_object
from cubely.common import get_hier_object
from cubely.common import check_dim_is_in_cube
from cubely.common import cube_log

import threading
import copy
import time
import hashlib

# *** Constants
to = 'to'
add = 'add'
remove = 'remove'
complement = 'complement'
keep = 'keep'

# *** Module global vars
DIMS_TEMPORARY_STATUS = {}
UPDATE_INTERVAL = 500000

# *** Functions
def all(dim, method, hierCode = None):
    """
    Limit a dimension to all its positions by using the status_* funcs of the dim
    Generaly not used directly but called by the lmt function. Returns nothing.

    Keywords arguments:
    dim -- the dimension object to limit
    method -- a method function (add, to, keep, remove)
    hierCode -- reduce the scope of the limit to the designated hierarchy (default = None)

    """
    if method in [add, to]:
        if hierCode:
            hier = cubely.HIERS[dim.code][hierCode]
            linkVals = set(hier.links.values())
            linkKeys = set(hier.links.keys())
            dim.status_set(linkVals.union(linkKeys))
        else:
            dim.status_all()
    elif method == remove:
        dim.status_clear()
    elif method == keep:
        return
    else:
        raise ValueError


def _set_final_status_hier(dim, method, result, originalStatus):
    if method == to:
        dim.status_set([x for x in result])
    elif method == remove:
        result = originalStatus.difference(result)
        dim.status_set([x for x in result])
    elif method == keep:
        result = result.intersection(originalStatus)
        dim.status_set([x for x in result])
    elif method == add:
        dim.status_add([x for x in result])


def ancestors(dim, method, hierCode):
    """
    Limit a dimension to all the positions above in the hierarchy of the positions
    currently in status
    Generaly not used directly but called by the lmt function. Returns nothing.

    Keywords arguments:
    dim -- the dimension object to limit
    method -- a method function (add, to, keep, remove)
    hierCode -- code of the hierarchy to work with

    """
    result = set()
    originalStatus = set(dim.status)
    currentRoundResults = set('bogus_to_allow_first_iteration')
    currentStatus = dim.status
    try:
        hier = cubely.HIERS[dim.code][hierCode]
    except:
        raise HierarchyError(hierCode, 'Hierarchy does not exist')

    while len(currentRoundResults) > 0:
        currentRoundResults = set()
        for pos in currentStatus:
            try:
                currentRoundResults.add(hier.links[pos.code])
            except KeyError:
                pass
        currentStatus = [dim.positions[x] for x in currentRoundResults]
        for r in currentStatus:
            result.add(r)
            if r in originalStatus:
                del currentStatus[currentStatus.index(r)]
                continue

    _set_final_status_hier(dim, method, result, originalStatus)


def parents(dim, method, hierCode):
    """
    Limit a dimension to the positions immediatly above in the hierarchy of the
    positions currently in status
    Generaly not used directly but called by the lmt function. Returns nothing.

    Keywords arguments:
    dim -- the dimension object to limit
    method -- a method function (add, to, keep, remove)
    hierCode -- code of the hierarchy to work with

    """
    result = set()
    originalStatus = set(dim.status)
    currentStatus = dim.status
    try:
        hier = cubely.HIERS[dim.code][hierCode]
    except:
        raise HierarchyError(hierCode, 'Hierarchy does not exist')

    for pos in currentStatus:
        try:
            result.add(hier.links[pos.code])
        except KeyError:
            pass

    _set_final_status_hier(dim, method, result, originalStatus)


def children(dim, method, hierCode):
    """
    Limit a dimension to the positions immediatly below in the hierarchy of the
    positions currently in status
    Generaly not used directly but called by the lmt function. Returns nothing.

    Keywords arguments:
    dim -- the dimension object to limit
    method -- a method function (add, to, keep, remove)
    hierCode -- code of the hierarchy to work with

    """
    result = set()
    originalStatus = set(dim.status)
    currentStatus = dim.status
    try:
        hier = cubely.HIERS[dim.code][hierCode]
    except:
        raise HierarchyError(hierCode, 'Hierarchy does not exist')

    for pos in currentStatus:
        try:
            for x in find_keys(hier.links, pos.code):
                result.add(x)
        except KeyError:
            pass

    _set_final_status_hier(dim, method, result, originalStatus)


def descendants(dim, method, hierCode):
    """
    Limit a dimension to all the positions below in the hierarchy of the positions
    currently in status
    Generaly not used directly but called by the lmt function. Returns nothing.

    Keywords arguments:
    dim -- the dimension object to limit
    method -- a method function (add, to, keep, remove)
    hierCode -- code of the hierarchy to work with

    """
    result = set()
    originalStatus = set(dim.status)
    currentRoundResults = set('bogus_to_allow_first_iteration')
    currentStatus = dim.status
    try:
        hier = cubely.HIERS[dim.code][hierCode]
    except:
        raise HierarchyError(hierCode, 'Hierarchy does not exist')

    while len(currentRoundResults) > 0:
        currentRoundResults = set()
        for pos in currentStatus:
            try:
                for x in find_keys(hier.links, pos.code):
                    currentRoundResults.add(x)
            except KeyError:
                pass
        currentStatus = [dim.positions[x] for x in currentRoundResults]
        for r in currentStatus:
            result.add(r)
            if r in originalStatus:
                del currentStatus[currentStatus.index(r)]
                continue

    _set_final_status_hier(dim, method, result, originalStatus)


def last_descendants(dim, method, hierCode):
    result = set()
    originalStatus = set(dim.status)
    try:
        hier = cubely.HIERS[dim.code][hierCode]
    except:
        raise HierarchyError(hierCode, 'Hierarchy does not exist')

    lmt(dim, method, descendants, hierCode)
    ancestors = set(hier.links.values())
    for pos in dim:
        if pos.code not in ancestors:
            result.add(pos)

    _set_final_status_hier(dim, method, result, originalStatus)
    

#def first(dim, method, hierCode):
def first(dim, method, numItems):
    """
    Limit a dimension to the numItems first positions in status.
    Generaly not used directly but called by the lmt function. Returns nothing.

    Keywords arguments:
    dim -- the dimension object to limit
    method -- a method function (add, to, keep, remove)
    numItems -- the numbers of positions you want

    """
    #numItems = hierCode
    if method == to:
        result = dim.positions.values()[:numItems]
        dim.status_set(result)
    elif method == add:
        result = dim.positions.values()[:numItems]
        dim.status_add(result)
    elif method == remove:
        result = dim.status[:numItems]
        dim.status_remove(result)
    elif method == keep:
        result = dim.status[:numItems]
        dim.status_keep(result)


def last(dim, method, numItems):
    """
    Limit a dimension to the numItems last positions in status.
    Generaly not used directly but called by the lmt function. Returns nothing.

    Keywords arguments:
    dim -- the dimension object to limit
    method -- a method function (add, to, keep, remove)
    numItems -- the numbers of positions you want

    """
    numItems = -numItems
    if method == to:
        result = dim.positions.values()[numItems:]
        dim.status_set(result)
    elif method == add:
        result = dim.positions.values()[numItems:]
        dim.status_add(result)
    elif method == remove:
        result = dim.status[numItems:]
        dim.status_remove(result)
    elif method == keep:
        result = dim.status[numItems:]
        dim.status_keep(result)


def lmt(dim, method, fn = None, hierCode = None):
    """
    Set the status of a dimension. Return nothing.

    Keyword arguments:
    dim -- the dimension you want to set the status of
    method -- the method used (to, add, keep, remove, complement)
    fn -- either a function (all, ancestors, children, descendants, first, last,
          parents) or a list of positions or positions code
    hierCode -- hierarchy code to use for the "fn" functions that require it

    Examples:
    lmt(prod, to, first, 10) -- limit the prod dim to the 10 first posiotions
    lmt(prod, add, ancestors, 'STD') -- add all the positions that are above the
            posisitions currently in status in the hierarchy STD
    lmt(prod, complement) -- limit the dim to all thep positions that are not
            currently in status.

    """
    # Check arguments
    if method not in [to, add, remove, complement, keep]:
        raise ValueError

    # Get a handle on the dim object to manipulate
    if dim.__class__ is cubely.core.Dimension:
        dimObj = dim
    else:
        dimObj = cubely.DIMS[dim]

    # Special case method complement
    if method == complement:
        currentStatus = dimObj.status
        dimObj.status_all()
        dimObj.status_remove(currentStatus)
        return
        
    # General case
    if fn.__class__ is list:
        if method == add:
            dimObj.status_add(fn)
        elif method == remove:
            dimObj.status_remove(fn)
        elif method == to:
            dimObj.status_set(fn)
        elif method == keep:
            dimObj.status_keep(fn)
    elif fn.__class__ in [str, unicode]:
        pos = []
        pos.append(fn)
        if method == add:
            dimObj.status_add(pos)
        elif method == remove:
            dimObj.status_remove(pos)
        elif method == to:
            dimObj.status_set(pos)
        elif method == keep:
            dimObj.status_keep(pos)
    else:
        fn(dimObj, method, hierCode)
        


def rpr(obj, downDim=None, acrossDim=None, pageDims=None, title=None, debug=False):
    """
    Print a formated report on screen.

    Keywords:
    obj -- the object to report (dimension, cube, hierarchy, formula) (default = None)
    downDim -- the dim whose positions will appear as lines in the report (default = None)
    accrosDim -- the dim whose positions will appear as columns in the report (default = None)
    pageDims -- dims that will appear as pages in the report (default = None)
    title -- title of the report
    debug -- boolean, if True show a verbose output for debugging (default = False)

    """
    class BogusDim(object):
        code = ''
        iterationCount = 0
        def __init__(self, code):
            self.code = code
        def __iter__(self):
            self.iterationCount = 0
            return self
        def next(self):
            if self.iterationCount == 1:
                raise StopIteration
            else:
                self.iterationCount += 1
                return Position(self, 'None', 'desc')

    if obj.__class__ in [str, unicode] and obj in cubely.DIMS.keys():
        obj = cubely.DIMS[obj]

    if title:
            print '== ' + title

    if obj.__class__ == cubely.core.Dimension:
        for p in obj:
            print p

    if obj.__class__ == cubely.core.Hierarchy:
        print "%10s %10s" % ('POSITION', 'PARENT')
        print "%10s %10s" % ('--------', '------')
        for pos in cubely.DIMS[obj.dimension]:
            try:
                parent = obj.links[pos.code]
            except KeyError:
                parent = 'NA'
            print "%10s %10s" % (pos.code, parent)

    if obj.__class__ in [cubely.core.Cube, cubely.core.Formula]:
        pages = []
        objDims = list(obj.dimensions)
        if downDim:
            downDim = get_dim_object(downDim)
            if not check_dim_is_in_cube(obj, downDim):
                raise DimensionError(downDim.code, 'The dimension does not belong to the cube')
            objDims.remove(downDim.code)
        else:
            if len(objDims) >= 1:
                downDim = get_dim_object(objDims[0])
                del objDims[0]
            else:
                downDim = ['']
        if acrossDim:
            acrossDim = get_dim_object(acrossDim)
            if not check_dim_is_in_cube(obj, acrossDim):
                raise DimensionError(acrossDim.code, 'The dimension does not belong to the cube')
            objDims.remove(acrossDim.code)
        else:
            if len(objDims) >= 1:
                acrossDim = get_dim_object(objDims[0])
                del objDims[0]
            else:
                acrossDim = BogusDim('BOGUS')
        if pageDims:
            if pageDims.__class__ == list:
                for pageDim in pageDims:
                    pageDim = get_dim_object(pageDim)
            else:
                pageDim = get_dim_object(pageDims)
            if check_dim_is_in_cube(obj, pageDim):
                pages.append(pageDim)
            else:
                raise DimensionError(pageDim.code, 'The dimension does not belong to the cube')
        else:
            if len(objDims) >= 1:
                for d in objDims:
                    pages.append(get_dim_object(d))
            else:
                bogusPage = BogusDim('NONE')
                pages.append(bogusPage)

        # Construct format string for columns
        baseColumnFormat = '%-10s\t'
        formatCols = baseColumnFormat
        contentCols = [' ']
        cols = [' ']
        numCols = 0
        for col in acrossDim:
            cols.append(col)
            formatCols += baseColumnFormat
            try:
                contentCols.append(col.code)
            except AttributeError:
                pass
            numCols += 1
        contentCols = tuple(contentCols)

        for page in pages:
            for p in page:
                pageTitle = page.code + ': ' + p.code
                pageTitleLen = len(pageTitle)
                pageTitle2 = '-'*pageTitleLen
                pageTitleFormat2 = "%"+str(pageTitleLen)+"s"
                print ' '
                print "%s" % (pageTitle)
                print pageTitleFormat2 % (pageTitle2)
                print ' '
                lineIndex = 0
                colIndex = 0

                for line in downDim:
                    if lineIndex == 0:
                        print formatCols % contentCols

                    for col in cols:
                        if colIndex > 0:
                            coordinates = {}
                            if page.code != 'NONE':
                                coordinates[page.code] = p.code
                            if col.code != 'None':
                                coordinates[acrossDim.code] = col.code
                            coordinates[downDim.code] = line.code
                            if debug:
                                print coordinates
                            lineContent.append(obj.get(coordinates))
                        else:
                            lineContent = [line.code]

                        colIndex += 1
                        if colIndex > numCols:
                            print formatCols % tuple(lineContent)
                            colIndex = 0

                    lineIndex += 1
    print ' '

def aggregate(cube, parallel_aggreg=False, partial_aggreg=False):
    """
    Fully aggregate a cube, whatever the current statuses are.

    Warning: only the values already updated (via cube.update()) will be aggregated
    potentialy overwriting non commited data

    Keywords arguments:
    parallel_aggreg -- boolean, level of parallelism to be used (default=False, no parallelism)
    partial_aggreg -- list, list of dimensions to aggregate if not all (default=False)

    """
    # @TODO : multithreaded aggregation. multiprocessing.cpu_count() to get the number of cores
    timestamp = _get_timestamp_hash()
    for dimCode in cube.dimensions:
        _snapshot_status(dimCode, timestamp)
        lmt(cubely.DIMS[dimCode], to, all)
    try:
        cube_log('Starting aggregation of cube ' + cube.code, 1)
        for dimCode in cube.dimensions:
            if partial_aggreg and dimCode not in partial_aggreg:
                cube_log('Skipping dimension ' + dimCode, 2)
            else:
                try:
                    hiers = cubely.HIERS[dimCode]
                    for hierCode in hiers:
                        cube_log('['+cube.code+'] Rolling up dimension ' + dimCode + ' over hierarchy ' + hierCode, 2)
                        if parallel_aggreg:
                            rollup(cube, copy.copy(cubely.DIMS[dimCode]), copy.copy(cubely.HIERS[dimCode][hierCode]))
                        else:
                            rollup(cube, cubely.DIMS[dimCode], cubely.HIERS[dimCode][hierCode])
                        cube_log('['+cube.code+'] Done rolling up dimension ' + dimCode + ' over hierarchy ' + hierCode, 2, True)
                except KeyError:
                    pass
        cube_log('Done aggregating cube ' + cube.code, 1, True)
    finally:
        for dimCode in cube.dimensions:
            _restore_status(dimCode, timestamp)


def rollup(cube, dim, hier):
    """
    Aggregate a cube over 1 hierarchy. Status dependant.

    Keywords arguments:
    cube -- cube to aggregate
    dim -- dimension along which aggregate
    hier -- hierarchy name to use (must belong to dim)

    """
    # 1) lmt to last descendants IN STATUS
    # 2) for each one, add its own value to its parent
    # 3) lmt to parents
    # 4) "rinse and repeat" until only positions with no parents are in status
    # @TODO : test with recursion to see if it improves performance
    nextVal = None
    originalStatus = copy.copy(dim.status)
    try:
        potentials = set(hier.links.keys())
        ancestors = set(hier.links.values())
        lastDescendants = potentials.difference(ancestors)
        lmt(dim, to, list(lastDescendants))
        cellCounter = 0
        levelCounter = 0
        while dim.statlen() > 0:
            # For each position in status in the aggregated dim ...
            for el in dim:
                if cellCounter > UPDATE_INTERVAL:
                    cube_log('['+cube.code+'] Updating cube after ' + str(cellCounter) + ' records', 3)
                    cube.update()
                    cellCounter = 0
                # Retrieve the cells currently existing for this position
                cells = cube.cubeCollection.find({dim.code: el.code})
                for cell in cells:
                    dimsTup = {}
                    del(cell['_id'])
                    cellVal = cell['value']
                    del(cell['value'])
                    dimsTup = copy.copy(cell)
                    dimsTup[dim.code] = hier.links[el.code]
                    currentVal = cube.get(dimsTup)
                    if currentVal is None:
                        nextVal = cellVal
                    else:
                        nextVal = cellVal+currentVal

                if nextVal is not None:
                    '''if cube.code=='RENTALS':
                        print(dimsTup)
                        try:
                            print(nextVal)
                        except UnboundLocalError:
                            print(0)'''
                    cube.set(dimsTup, nextVal)
                cellCounter += 1

            lmt(dim, to, parents, hier.code)
            cube_log('['+cube.code+'] Done with level ' + str(levelCounter) + '. Parents to compute: ' + str(dim.statlen()), 3)
            levelCounter += 1
            # Warning: we must update the cube after dim otherwise the changes are not 
            # pushed back in the collection (used to define perimeter at the begining of the loop)
            cube.declare_aggregated_dim(dim.code)
            cube.update()

    finally:
        dim.status = originalStatus


def dyn_aggregate(cube, posTuple, hiers):
    result = 0
    tmpVal = cube.get(posTuple)
    if tmpVal != None:
        return cube.get(posTuple)

    toAggregateDims = cube.dimensions.difference(cube.aggregatedDims)
    notToAggregateDims = cube.dimensions.intersection(cube.aggregatedDims)
    timestamp = _get_timestamp_hash()

    for dim in notToAggregateDims:
        _snapshot_status(dim, timestamp)
        lmt(dim, to, posTuple[dim])

    for dim in toAggregateDims:
        _snapshot_status(dim, timestamp)
        lmt(dim, to, posTuple[dim])
        if hiers.has_key(dim):
            lmt(dim, to, last_descendants, hiers[dim])
    queryList = cubely.common.get_status_nuplet(cube.dimensions)

    for dim in cube.dimensions:
        _restore_status(dim, timestamp)
    for query in queryList:
        cellVal = cube.get(query)
        if type(cellVal) is NoneType:
            cellVal = 0
        result += cellVal
    cube.set(posTuple, result)
    return result


def _snapshot_status(dim, token=False):
    """
    Create a snapshot of the current status of a dimension. Thread safe. Internal use only.

    Keywords arguments:
    dim -- dimension to snapshot

    """
    dimObj = cubely.common.get_dim_object(dim)
    dimCode = dimObj.code

    if not token:
        token = 'default'

    try:
        # Python 2.6 and newer
        threadName = threading.current_thread().name
    except AttributeError:
        # Python 2.5 and before
        threadName = threading.currentThread()._Thread__name

    if not threadName in DIMS_TEMPORARY_STATUS.keys():
        DIMS_TEMPORARY_STATUS[threadName] = {}
    DIMS_TEMPORARY_STATUS[threadName][dimCode] = {}
    DIMS_TEMPORARY_STATUS[threadName][dimCode][token] = copy.copy(cubely.DIMS[dimCode].status)



def _restore_status(dim, token=False):
    """
    Restore a status snapshot for a dimension. Thread safe. Internal use only.

    Keywords arguments:
    dim -- dimension to restore

    """
    dimObj = cubely.common.get_dim_object(dim)
    dimCode = dimObj.code
    
    if not token:
        token = 'default'
    
    try:
        # Python 2.6 and newer
        threadName = threading.current_thread().name
    except AttributeError:
        # Python 2.5 and before
        threadName = threading.currentThread()._Thread__name
    cubely.DIMS[dimCode].status = DIMS_TEMPORARY_STATUS[threadName][dimCode][token]
    del DIMS_TEMPORARY_STATUS[threadName][dimCode][token]


def check_hier(hier):
    """
    Index a hierarchy for sorting

    Keywords arguments:
    hier -- cubely.Hierarchy object to use

    """
    dim = cubely.DIMS[hier.dimension]
    timestamp = _get_timestamp_hash()
    _snapshot_status(dim, timestamp)
    try:
        ancestors = set(hier.links.values())
        childs = set(hier.links.keys())
        topAncestors = ancestors.difference(childs)
        index = 0

        lmt(dim, to, list(topAncestors))

        while dim.statlen() > 0:
            for pos in dim.status:
                hier.hierCollection.update({'code': pos.code}, {"$set": {"index": index}})
                index += 1
            lmt(dim, to, children, hier.code)
    finally:
        _restore_status(dim, timestamp)


def sorth(dim, hier):
    """
    Sort a dimension based on the hierarchy order

    Keywords arguments:
    dim -- Dimension object or name to sort
    hier -- Hierarchy name to use for sorting

    """
    dimObj = get_dim_object(dim)
    hierObj = get_hier_object(dimObj, hier)
    def _sort(x, y):
        indexX = hierObj.hierCollection.find_one({'code': x.code})
        indexY = hierObj.hierCollection.find_one({'code': y.code})
        try:
            if indexX['index'] > indexY['index']:
                return 1
            elif indexX['index'] == indexY['index']:
                return 0
            else:  #x < y
                return -1
        except TypeError:
            if x.code in hierObj.links.values() and x.code not in hierObj.links.keys():
                return -1
            else:
                return 1
    dimObj.status.sort(_sort)


def sorta(dim):
    """
    Alphabetical sorting of a dimension.

    Keywords arguments:
    dim -- dimension to sort

    """
    dimObj = get_dim_object(dim)
    dimObj.status.sort(lambda x, y: cmp(x.code.lower(),y.code.lower()))


def total(cube, dims):
    inList = {}
    dimObjs = []
    resultsList = set()
    _total = 0

    for dim in dims:
        dimObj = get_dim_object(dim)
        dimObjs.append(dimObj)

    for dim in dimObjs:
        posList = []
        for pos in dim:
            posList.append(pos.code)
        inList[dim.code]=copy.copy(posList)

    for dim in dimObjs:
        inClause = {dim.code: {'$in': inList[dim.code]}}
        results = cube.cubeCollection.find(inClause)
        for res in results:
            resultsList.add(res)

    for res in resultsList:
        _total += res['value']

    return _total


def dsc(obj):
    """
    Describe an object.

    Keywords arguments:
    obj -- object to describe

    """
    print obj


def isby(obj, dim):
    """
    Is an object dimensioned by a given dimension. Return Boolean.

    Keywords arguments:
    obj -- Object or name to check
    dim -- Dimension object or name to use for the check

    """
    dimObj = get_dim_object(dim)

    if obj.__class__ == cubely.core.Dimension:
        if dim.code == dimObj.code:
            return True
        else:
            return False

    elif obj.__class__ == cubely.core.Hierarchy:
        if obj.dimension == dimObj.code:
            return True
        else:
            return False

    elif obj.__class__ == cubely.core.Cube:
        if dimObj.code in obj.dimensions:
            return True
        else:
            return False

    else:
        return False

def _get_timestamp_hash():
    return hashlib.md5(str(time.time())).hexdigest()

def push(dim):
    """
    Save the status of a dimension for future restoration.

    Keywords arguments:
    dim -- Dimension object or name to save

    """
    dimObj = cubely.common.get_dim_object(dim)
    if dimObj in cubely.DIMS.values():
        _snapshot_status(dimObj)
    else:
        raise DimensionError(dim, 'Dimension does not exist')

def pop(dim):
    """
    Restore a previously saved dimension status

    Keywords arguments:
    dim -- Dimension object or name to restore

    """
    dimObj = cubely.common.get_dim_object(dim)
    if dimObj in cubely.DIMS.values():
        _restore_status(dimObj)
    else:
        raise DimensionError(dim, 'Dimension does not exist')

def statlen(dim):
    """
    Calculate the number of positions in status for a given dimension. Return int.

    Keywords arguments:
    dim -- Dimension object or name

    """
    dimObj = cubely.common.get_dim_object(dim)
    return dimObj.statlen()

def exists(name):
    """
    Check if an object with the given name already exist. Return Boolean.

    Keywords arguments:
    name -- name to check

    """
    name = name.upper()
    if name in cubely.DIMS.keys():
        return True
    elif name in cubely.CUBES.keys():
        return True
    elif name in cubely.FORMULAS.keys():
        return True
    else:
        return False

def allstat():
    """
    Limit all dimensions to all positions
    """
    for dim in cubely.DIMS.values():
        lmt(dim, to, all)

def instat(dim, val):
    pass

def isvalue(dim, val):
    pass

def listnames(objectTypes):
    pass