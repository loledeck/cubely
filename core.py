# -*- coding: utf-8 -*-
"""Core cubely classes for multidimensional analysis"""

# *** Imports
from operator import truediv
from operator import neg
import copy
from threading import Lock
from types import NoneType
from types import IntType
from types import LongType
from types import FloatType

from pymongo import ASCENDING

import cubely
import cubely.core
import cubely.common

from cubely.errors import DatabaseError
from cubely.errors import DimensionError
from cubely.errors import PositionError
from cubely.errors import PositionAlreadyExistsError
from cubely.errors import CubeError
from cubely.errors import FormulaError
from cubely.errors import RelationError
from cubely.errors import HierarchyError

# *** Classes definition
class Database(object):
    """Cubely database. Each one correspond to one mongodb namespace. Singleton."""
    db = None
    name = u''
    description = u''
    metas = None

    """Clear the instance properties. Private."""
    def _clear(self):
        self.db = None
        self.name = u''
        self.description = u''
        self.metas = None

    """
    Create a new cubely database. Return nothing.

    Keywords arguments:
    name -- Name of the new db
    description -- Description of the new db (default = '')

    """
    def create(self, name, description=''):
        self._clear()
        self.db = cubely.CONNECTION[name]
        existingCols = self.db.collection_names()
        if len(existingCols) > 0:
            raise DatabaseError(name, 'Database ' + name + ' cannot be created. Already exists.')
        else:
            self.metas = self.db.meta
            self.metas.insert({'code': 'name', 'value': name})
            self.metas.insert({'code': 'description', 'value': description})
            self.metas.insert({'code': 'dims', 'value': []})
            self.metas.insert({'code': 'cubes', 'value': []})
            self.metas.insert({'code': 'hiers', 'value': []})
            self.metas.insert({'code': 'formulas', 'value': []})
            self.metas.insert({'code': 'relations', 'value': []})
            self.open(name)

    """
    Open a cubely database. Return the database instance.

    Keywords arguments:
    name -- Name of the db to open

    """
    def open(self, name):
        # connect to the database
        self.db = cubely.CONNECTION[name]
        existingCols = self.db.collection_names()
        # get meta information
        if u'meta' in existingCols:
            cubely.CURRENT_DB = name
            self.metas = self.db['meta']
            self.name = self.metas.find_one({'code': 'name'})[u'value']
            self.description = self.metas.find_one({'code': 'description'})
            # dims instanciation
            for dim in self.metas.find_one({'code': 'dims'})[u'value']:
                cubely.DIMS[dim['code']] = cubely.dim._get(dim['code'])
                tmpCode = 'cubely.D_' + dim['code'] + ' = cubely.DIMS[\'' + dim['code'] + '\']'
                exec(tmpCode)
            # cubes instanciation
            for var in self.metas.find_one({'code': 'cubes'})[u'value']:
                cubely.CUBES[var['code']] = cubely.cube._get(var['code'])
                tmpCode = 'cubely.V_' + var['code'] + ' = cubely.CUBES[\'' + var['code'] + '\']'
                exec(tmpCode)
            for var in cubely.CUBES.values():
                for dim in var.dimensions:
                    cubely.DIMS_IN_USE.add(dim)
            # Formulas instanciation
            for var in self.metas.find_one({'code': 'formulas'})[u'value']:
                #print var
                cubely.FORMULAS[var['code']] = cubely.formula._get(var['code'])
                tmpCode = 'cubely.F_' + var['code'] + ' = cubely.FORMULAS[\'' + var['code'] + '\']'
                exec(tmpCode)
            # hierarchies instanciation
            for hier in self.metas.find_one({'code': 'hiers'})[u'value']:
                if hier['dim'] not in cubely.HIERS.keys():
                    cubely.HIERS[hier['dim']] = {}
                cubely.HIERS[hier['dim']][hier['code']] = cubely.hier._get(hier['dim'], hier['code'])
                # ensure the hierarchies are indexed for hierarchical sorting
                cubely.lang.check_hier(cubely.HIERS[hier['dim']][hier['code']])
            # Relations instanciation
            for var in self.metas.find_one({'code': 'relations'})[u'value']:
                cubely.RELS[var['code']] = cubely.relation._get(var['code'])
                tmpCode = 'cubely.R_' + var['code'] + ' = cubely.RELS[\'' + var['code'] + '\']'
                exec(tmpCode)
            return self
        else:
            cubely.CURRENT_DB = None
            raise DatabaseError(name, 'Database ' + name + ' is not a valid cubely database')
            
    """Closes the current cubely database """
    def close(self):
        cubely.common.reset_global_vars()
        try:
            self.db.logout()
        except AttributeError:
            # If no db is open db is None
            pass

    """Delete a database. Return True or raise exception."""
    def drop(self, name):
        try:
            cubely.CONNECTION.drop_database(name)
            cubely.common.reset_global_vars()
            return True
        except TypeError:
            raise DatabaseError(name, 'Database ' + self.name + ' does not exist')

    def get_collections(self):
        return self.db.collection_names()

    """Check the coherence of the currently opened database"""
    def clean(self):
        dimList = []
        hierList = []
        cubeList = []
        # Clean meta of unused values
        print '# ** Checking dimensions'
        for dim in cubely.db.metas.find_one({'code': 'dims'})['value']:
            colName = cubely.common.get_collection_name('pos', dim['code'])
            dimList.append(colName)
            if colName not in cubely.db.db.collection_names():
                cubely.common.delete_meta_simple('dims', dim['code'])
                print '# ** * Meta entry for dim '+dim['code']+' dropped'
        # Drop unused collections
        predicat = '_cubely_pos'
        for col in cubely.db.db.collection_names():
            if col[0:len(predicat)] == predicat:
                if col not in dimList:
                    cubely.db.db.drop_collection(col)
                    print '# ** * Collection '+col+' dropped'
        print '# ** Dimensions done.'

        print '# ** Checking hierarchies'
        for hier in cubely.db.metas.find_one({'code': 'hiers'})['value']:
            colName = cubely.common.get_collection_name('hier', hier['dim'], hier['code'])
            hierList.append(hier['dim'].lower()+'_'+hier['code'].lower())
            if colName not in cubely.db.db.collection_names():
                cubely.common.delete_meta_double('hiers', 'dim', hier['dim'], hier['code'])
                print '# ** * Meta entry for hier '+colName+' dropped'
        # Drop unused collections
        predicat = '_cubely_hier'
        for col in cubely.db.db.collection_names():
            if col[0:len(predicat)] == predicat:
                nameEnd = col[len(predicat)+1:]
                if nameEnd not in hierList:
                    cubely.db.db.drop_collection(col)
                    print '# ** * Collection '+col+' dropped'
        print '# ** Hierarchies done.'

        print '# ** Checking cubes'
        for cube in cubely.db.metas.find_one({'code': 'cubes'})['value']:
            colName = cubely.common.get_collection_name('cube', cube['code'])
            cubeList.append(colName)
            if colName not in cubely.db.db.collection_names():
                cubely.common.delete_meta_simple('hiers', cube['code'])
                print '# ** * Meta entry for cube '+colName+' dropped'
        # Drop unused collections
        predicat = '_cubely_cube'
        for col in cubely.db.db.collection_names():
            if col[0:len(predicat)] == predicat:
                if col not in cubeList:
                    cubely.db.db.drop_collection(col)
                    print '# ** * Collection '+col+' dropped'
        print '# ** Cubes done.'

    """Check if the database dbNames exist. Return Boolean."""
    def exists(self, dbName):
        if dbName in cubely.CONNECTION.database_names(): return True
        else: return False

    """Save data changes in the currently active database."""
    def update(self):
        if cubely.CURRENT_DB == '':
            raise DatabaseError('','No open database to update')
        for cubeCode in cubely.CUBES:
            cubely.CUBES[cubeCode].update()


class Dimension(object):
    """Dimension. Axis of analysis, structuring the data"""
    code = ''
    abrev = ''
    description = u''
    positions = {}
    positionCollection = None
    hierarchies = {}
    status = []
    iterationCount = 0

    def __iter__(self):
        self.iterationCount = 0
        return self

    """Clear the instance properties. Private."""
    def _clear(self):
        self.code = ''
        self.abrev = ''
        self.description = u''
        self.positions = {}
        self.positionCollection = None
        self.hierarchies = {}
        self.status = []
        self.iterationCount = 0

    def __str__(self):
        return '(dimension ' + self.code + ') ' + self.description

    def next(self):
        if self.statlen() == 0:
            raise StopIteration
        elif self.iterationCount >= self.statlen():
            self.iterationCount = 0
            raise StopIteration
        else:
            self.iterationCount += 1
            return self.status[self.iterationCount - 1]

    """
    Create a new dimension. Singleton. Return the dimension object.

    Keywords arguments:
    code -- Code of the dimension. Must be unique in a given database.
    desc -- Description of the dimension

    """
    def create(self, code, desc):
        self._clear()
        code = code.upper()
        # Check if code already exists
        if code in cubely.DIMS.keys():
            raise DimensionError(code, code + ' dimension already exsits in this database')
        if cubely.CURRENT_DB == '':
            raise DimensionError(code, 'No active database to define dimension')
        self.code = code
        self.description = desc
        self.positionCollection = cubely.db.db[cubely.common.get_collection_name('pos', self.code)]
        dimObj = {
            'code': code,
            'desc': desc,
            'statlen': 0
        }
        cubely.db.metas.update({'code': 'dims'}, {'$push': {'value': dimObj}})
        dimCopy = copy.copy(self)
        cubely.DIMS[code] = dimCopy
        exec('cubely.D_'+code+' = cubely.DIMS[\''+code+'\']')
        return dimCopy

    """
    Delete anexisting dimension

    Keywords arguments:
    code -- Code of the dimension to delete

    """
    def delete(self, code):
        if code in cubely.DIMS_IN_USE:
            raise DimensionError(code, 'Dimension in use')
        if code in cubely.HIERS.keys():
            for hier in cubely.HIERS[code]:
                cubely.db.db.drop_collection(cubely.common.get_collection_name('hier', self.code, hier))
                cubely.common.delete_meta_double('hiers', 'dim', self.code, hier)
            del cubely.HIERS[code]
        if code in cubely.DIMS.keys():
            # Delete the positions first
            ## we must copy the positions list first cause its gonna be modified while we're iterating
            posList = copy.deepcopy(self.positions)
            for pos in posList:
                self.delete_position(pos)
            del(posList)
            # Delete the positions representation in mongo
            cubely.db.db.drop_collection(cubely.common.get_collection_name('pos', self.code))
            # Clean up memory
            del(cubely.DIMS[code])
            exec('del(cubely.D_'+code+')')
            cubely.common.delete_meta_simple('dims', code)
        else:
            raise DimensionError(code, code + ' dimension does not exist')

    """
    Add a new position to the dimension. Return the position object.

    Keywords arguments:
    code -- Code of the position to be created
    desc -- Description of the position (default None)

    """
    def add_position(self, code, desc=None):
        if self.positions.has_key(code):
            raise PositionAlreadyExistsError(code, 'Position already exists')
        else:
            self.positions[code] = 'PENDING'    # Special status to allow the creation of the position instance without infinite loop
            newPos = Position(self.code, code, desc)
            self.positions[code] = newPos
            if newPos.dimension != self.code:
                raise DimensionError(self, 'The position you want to add in status does not belong to the dimension')
            self.status.append(newPos)
            newPos.save()
            return newPos

    """
    Delete an existing position.

    Keywords arguments:
    code -- Code of the position to delete. Must belong to the dimension.

    """
    def delete_position(self, code):
        if self.positions.has_key(code):
            pos = self.positions[code]
            if pos in self.status:
                self.status.remove(pos)
            pos.requested_deletion = True
            pos.delete()
            del pos
        else:
            raise PositionError(code, 'Position does not exist')

    """
    The the existence of a position in a dimension. Return Boolean.

    Keywords arguments:
    code -- Code of the position to check

    """
    def has_position(self, code):
        if self.positions.has_key(code):
            return True
        else:
            return False

    """
    instanciate existing dimensions. Called from Database.open().
    Also instanciate the positions of the dimension. Return Dimension object.

    Keywords arguments:
    code -- Code of the dimension to instanciate

    """
    def _get(self, code):
        self._clear()
        self.code = code.upper()
        self.positions = {}
        colName = cubely.common.get_collection_name('pos', self.code)
        self.description = [item['desc'] for item in cubely.db.metas.find_one({'code': 'dims'})[u'value'] if item['code'] == self.code][0]
        self.positionCollection = cubely.db.db[colName]
        positions = cubely.db.db[colName].find()
        for pos in positions:
            self.positions[pos['code']] = Position(self.code, pos['code'], pos['desc'])
        return copy.copy(self)

    def statlen(self):
        """Return the number of positions currently in status for the dimension. Return Int."""
        return len(self.status)

    def status_clear(self):
        """Clear the status of the dimension"""
        self.status = []

    def _status_modify(self, positions, modification):
        """
        Modify the status of the dimension. Private.
        
        Keywords arguments:
        positions -- list of postions of the dimension
        modification -- [add|remove] the position list from the current status
        
        """
        tmpStatus = set(self.status)
        for position in positions:
            try:
                if position.has_key('code'):
                    {
                        'add': lambda x: tmpStatus.add(Position(self.code, x['code'], x['description'])),
                        'remove': lambda x: tmpStatus.remove(Position(self.code, x['code'], x['description']))
                    }[modification](position)

            # Exception generated by types without has_key function (str, unicode ...)
            except AttributeError:
                try:
                    if position.__class__ == Position:
                        {
                            'add': lambda x: tmpStatus.add(x),
                            'remove': lambda x: tmpStatus.remove(x)
                        }[modification](position)
                    elif position.__class__ in [str, unicode]:
                        instance = self.positionCollection.find_one({'code': position})
                        try:
                            {
                                'add': lambda x: tmpStatus.add(self.positions[x['code']]),
                                'remove': lambda x: tmpStatus.remove(self.positions[x['code']])
                            }[modification](instance)
                        except TypeError:
                            raise PositionError(position, 'Position code does not exist')
                    else:
                        raise PositionError(position, 'Invalid object class')
                # Exception if we try to remove a position not in status
                except KeyError:
                    # check if another position with the same code is  already in status
                    for pos in tmpStatus:
                        if position.__class__ == Position:
                            if pos.code == position.code:
                                tmpStatus.remove(pos)
                                break
                        else:
                            if pos.code == position:
                                tmpStatus.remove(pos)
                                break
        self.status = list(tmpStatus)
        self._check_positions_consistency()
        

    def status_add(self, positions):
        """Add the positions in the list to the current status"""
        self._status_modify(positions, 'add')

    def status_remove(self, positions):
        """Remove the positions in the list from the current status"""
        self._status_modify(positions, 'remove')

    def status_set(self, positions):
        """Set the current status to the positions list"""
        self.status_clear()
        self._status_modify(positions, 'add')

    def status_keep(self, positions):
        """Keep the positions in the list in status"""
        if len(positions) == 0:
            self.status_clear()
        if positions[0].__class__ in [str, unicode]:
            currentPos = [p.code for p in self.status]
        elif positions[0].__class__ == Position:
            currentPos = [p for p in self.status]
        currentPos = set(currentPos)
        posToSet = list(currentPos.intersection(set(positions)))
        self.status_set(posToSet)

    def status_all(self):
        """Set the status to all positions"""
        self.status = self.positions.values()
        self._check_positions_consistency()

    def get(self, code):
        """
        Return one position of the dimension.

        Keywords arguments:
        code -- code of the position to return

        """
        if code in self.positions.keys():
            return self.positions[code]
        else:
            raise DimensionError(code, 'Unknown position code')

    def _check_positions_consistency(self):
        """Check that the positions in status belong to the dimension. Private."""
        for p in self.status:
            if p.dimension != self.code:
                raise DimensionError(self, 'The position you want to add in status does not belong to the dimension')


class Position(object):
    """
    Position of a dimension
    Should not be maintained directly but through Dimension.add_position / Dimension.delete_position
    """
    dimension = None
    code=''
    description = u''
    requested_deletion = False

    def __init__(self, dim, code, desc=None):
        #Do not use cubely.common.get_dim_object here
        #It cannot work when the dimension is first created
        if dim.__class__ is cubely.core.Dimension:
            self.dimension = dim.code
        else:
            self.dimension = dim
        self.code = code
        if desc:
            self.description = desc
        else:
            self.description = code

    def __str__( self ):
        return self.code.encode('utf-8')

    def delete(self):
        """Delete the position."""
        # Delete cubes cells of the position
        for cube in cubely.CUBES.values():
            #print cube
            if self.dimension in cube.dimensions:
                cells = cube.cubeCollection.find({self.dimension: self.code}, snapshot=True)
                for cell in cells:
                    cube.cubeCollection.remove(cell)
        # Delete position from the dimension
        cubely.DIMS[self.dimension].positionCollection.remove({'code': self.code})
        if not self.requested_deletion:
            cubely.DIMS[self.dimension].delete_position(self.code)
        if self.code in cubely.DIMS[self.dimension].positions.keys():
            del cubely.DIMS[self.dimension].positions[self.code]
        # Delete self (sepuku)
        del self

    def save(self):
        """Save the position in mongodb"""
        cubely.DIMS[self.dimension].positionCollection.insert({'code': self.code, 'desc': self.description})


class Hierarchy(object):
    """Hold a hierarchy of position for one dimension. Singleton."""
    code = u''
    dimension = None
    links = {}
    hierCollection = None

    def _setup_collection(self):
        """Create the mongodb collection to store the hierarchy"""
        colName = cubely.common.get_collection_name('hier', self.dimension, self.code)
        self.hierCollection = cubely.db.db[colName]
        self.hierCollection.ensure_index(
            'code',
            unique=True,
            dropDups=True,
            background=True
        )

    def _clear(self):
        """Clear the instance properties. Private."""
        self.code = u''
        self.dimension = None
        self.links = {}
        self.hierCollection = None

    def create(self, dim, code):
        """
        Create a new hierarchy

        Keywords arguments:
        dim -- Dimension holding the hierarchy.
        code -- Code of the hierarchy. Must be unique in a dimension.

        """
        # Singleton clean-up
        self._clear()
        code = code.upper()
        # check if the dimension exists
        if dim.__class__ == cubely.core.Dimension:
            dimCodeToUse = dim.code
        else:
            dimCodeToUse = dim
        if dimCodeToUse in cubely.DIMS.keys():
            self.dimension = dimCodeToUse
        else:
            raise HierarchyError(dimCodeToUse, 'Dimension does not exist')
        self.code = code
        # Setup the collection object
        self._setup_collection()
        # Retrieve hierarchy definition
        hierSpecs = cubely.db.metas.find_one({'code': 'hiers'})
        flagExist = False
        if hierSpecs:
            for existingHier in hierSpecs['value']:
                if existingHier['code'] == code and existingHier['dim'] == self.dimension:
                    flagExist = True
                    break
            if not flagExist:
                cubely.db.metas.update({'code': 'hiers'}, {'$push': {'value': {'code': self.code, 'dim': dimCodeToUse}}})
        else:
            cubely.db.metas.insert({'code': 'hiers', 'value': [{'code': self.code, 'dim': dimCodeToUse}]})
        if dimCodeToUse not in cubely.HIERS.keys():
            cubely.HIERS[dimCodeToUse] = {}
        cubely.HIERS[dimCodeToUse][self.code] = copy.copy(self)
        return cubely.HIERS[dimCodeToUse][self.code]

    def delete(self, dim, code):
        """
        Delete a hierarchy.

        Keywords arguments:
        dim -- Dimension holding the hierarchy.
        code -- Code of the hierarchy to delete

        """
        if dim.__class__ == cubely.core.Dimension:
            dimCode = dim.code
        else:
            dimCode = dim
        hierSpecs = cubely.db.metas.find_one({'code': 'hiers'})
        hierIndex = 0
        for existingHier in hierSpecs['value']:
            if existingHier['code'] == code and existingHier['dim'] == dimCode:
                break
            hierIndex += 1
        if hierSpecs['value'][hierIndex]:
            del hierSpecs['value'][hierIndex]
        cubely.db.metas.update({'code': 'hiers'}, hierSpecs)
        cubely.db.db.drop_collection(cubely.common.get_collection_name('hier', dimCode, code))
        del cubely.HIERS[self.dimension][code]

    def _get(self, dim, code):
        """
        Instanciate the position. Private.

        Keywords arguments:
        dim -- Dimension of the hierarchy.
        code -- Code of the position

        """
        self._clear()
        self.code = code
        self.dimension = dim
        self.links = {}
        self._setup_collection()
        colName = cubely.common.get_collection_name('hier', self.dimension, self.code)
        if colName in cubely.db.get_collections():
            links = cubely.db.db[colName].find()
            for link in links:
                self.links[link['code']] = link['parent']
        return copy.copy(self)

    def set(self, pos, parent):
        """
        Set the parent of a position in the hierarchy

        Keywords arguments:
        pos -- Code of the child position
        parent -- Code of the parent position

        """
        dim = cubely.DIMS[self.dimension]
        if not dim.has_position(pos):
            raise HierarchyError(pos, 'Position does not exist')
        if not dim.has_position(parent):
            raise HierarchyError(parent, 'Position does not exist')
        if pos.__class__ == cubely.core.Position:
            posCode = pos.code
        else:
            posCode = pos
        if parent.__class__ == cubely.core.Position:
            parentCode = parent.code
        else:
            parentCode = parent
        # Check if the document needs to be inserted or updated
        doc = {'code': posCode}
        docObj = self.hierCollection.find_one(doc)
        if docObj:
            docObj['parent'] = parentCode
            self.hierCollection.save(docObj)
        else:
            doc['parent'] = parentCode
            self.hierCollection.insert(doc)
        self.links[posCode] = parentCode

    def unset(self, code):
        """
        Unset the parent of a position in the hierarchy

        Keywords arguments:
        code -- Code of the position whose parent you want to unset

        """
        if code in self.links.keys():
            self.hierCollection.remove({'code': code})
            del self.links[code]
        else:
            raise HierarchyError(code, 'Cannot unset a member that has not been already set')

    def get(self, code):
        """
        Return the code of the parent of a position in the hierarchy.

        Keywords arguments:
        code -- Code of the position

        """
        if code in self.links.keys():
            return self.links[code]
        else:
            return False

class Cube(object):
    """Object holding the data. Dimensioned by dimensions. Belong to a database. Singleton."""
    code = ''
    description = u''
    dimensions = set()
    allowedTypes = ['string', 'int', 'float', 'boolean']
    type = ''
    _values = {}
    collectionName = u''
    cubeCollection = None
    changedValues = set()
    aggregatedDims = set()

    def _clear(self):
        """Clear the instance properties. Private."""
        self.code = ''
        self.description = u''
        self.dimensions = set()
        self.allowedTypes = ['string', 'int', 'float', 'boolean']
        self.type = ''
        self._values = {}
        self.collectionName = u''
        self.cubeCollection = None
        self.aggregatedDims = set()
        self.changedValues = set()

    def __add__(self, other):
        return self._standard_operators_multiple('add', other)

    def __sub__(self, other):
        return self._standard_operators_multiple('sub', other)

    def __mul__(self, other):
        return self._standard_operators_multiple('mul', other)

    def __truediv__(self, other):
        return self._standard_operators_multiple('div', other)

    def __div__(self, other):
        return self._standard_operators_multiple('div', other)

    def __floordiv__(self, other):
        return self._standard_operators_multiple('fdv', other)

    def __mod__(self, other):
        return self._standard_operators_multiple('mod', other)

    def __pow__(self, other):
        return self._standard_operators_multiple('pow', other)

    def __abs__(self):
        return self._standard_operators_single(abs)

    def __pos__(self):
        ret = self._standard_operators_single(pos)
        return ret

    def __neg__(self):
        return self._standard_operators_single(neg)

    def _standard_operators_single(self, operator):
        """Private function to handle the single operators"""
        wk = copy.deepcopy(self)
        queryList = cubely.common.get_status_nuplet(wk.dimensions)
        for query in queryList:
            tmpval = wk.get(query)
            if type(tmpval) == NoneType:
                pass
            else:
                wk.set(query, operator(tmpval))
        return wk

    """
    @TODO : gérer le cas où other a des dimensions qui ne sont pas dans self.dimensions
    """
    def _standard_operators_multiple(self, operator, other):
        """Private function to handle multiple operators"""
        wk = copy.deepcopy(self)
        if other.__class__ == cubely.core.Cube:
            wk.dimensions = wk.dimensions.union(other.dimensions)
            queryList = cubely.common.get_status_nuplet(wk.dimensions)
            for query in queryList:
                querySelf = {}
                queryOther = {}
                for dim in query.keys():
                    if dim in wk.dimensions:
                        querySelf[dim] = query[dim]
                    if dim in other.dimensions:
                        queryOther[dim] = query[dim]
                cellSelf = wk.get(querySelf)
                cellOther = other.get(queryOther)
                if type(cellSelf) == NoneType: cellSelf = 0
                if type(cellOther) == NoneType: cellOther = 0
                cellVal = {
                    'add': cellSelf + cellOther,
                    'sub': cellSelf - cellOther,
                    'mul': cellSelf * cellOther,
                    'div': truediv(cellSelf, cellOther),
                    'fdv': cellSelf // cellOther,
                    'mod': cellSelf % cellOther,
                    'pow': pow(cellSelf, cellOther),
                }[operator]
                wk.set(querySelf, cellVal)
                
        elif type(other) is NoneType:
            return None
        elif type(other) in [IntType, LongType, FloatType]:
            queryList = cubely.common.get_status_nuplet(wk.dimensions)
            for query in queryList:
                cellVal = wk.get(query)
                if type(cellVal) is NoneType:
                    cellVal = 0
                cellVal = {
                    'add': cellVal + other,
                    'sub': cellVal - other,
                    'mul': cellVal * other,
                    'div': truediv(cellVal, other),
                    'fdv': cellVal // other,
                    'mod': cellVal % other,
                    'pow': pow(cellVal, other),
                }[operator]
                wk.set(query, cellVal)
        return wk

    def __deepcopy__(self, memo):
        newone = type(self)()
        newone._values = copy.deepcopy(self._values)
        newone.allowedTypes = self.allowedTypes
        newone.changedValues = copy.deepcopy(self.changedValues)
        newone.code = u'TMP_EXPRESSION'
        newone.collectionName = copy.copy(self.collectionName)
        newone.cubeCollection = self.cubeCollection
        newone.description = u''
        newone.dimensions = copy.deepcopy(self.dimensions)
        newone.type = self.type
        return newone

    def __str__(self):
        buffer = '(cube ' + self.code + ') <'
        for dim in self.dimensions:
            buffer += dim + ' '
        buffer = buffer[:-1] + '> ' + self.description
        return buffer

    def create(self, dims, type, code, desc):
        self._clear()
        code = code.upper()
        if type in self.allowedTypes:
            self.type = type
        else:
            raise CubeError(type, 'Invalid cube type')
        if code in cubely.CUBES.keys():
            raise CubeError(code, 'Cube code already exists')
        for dim in dims:
            if dim.__class__ == Dimension:
                self.dimensions.add(dim.code)
            else:
                self.dimensions.add(dim)
        self.code = code
        self.description = desc
        self.collectionName = cubely.common.get_collection_name('cube', False, self.code)
        self.cubeCollection = cubely.db.db[self.collectionName]
        cubely.DIMS_IN_USE.update(self.dimensions)
        cubeSpecs = cubely.db.metas.find_one({'code': 'cubes'})
        flagExist = False
        if cubeSpecs:
            for existingCube in cubeSpecs['value']:
                if existingCube['code'] == code:
                    flagExist = True
                    break
            if not flagExist:
                cubely.db.metas.update({'code': 'cubes'}, {'$push': {'value': {'code': self.code, 'desc': self.description, 'type': self.type, 'dims': list(self.dimensions), 'aggregated_dims':list(self.aggregatedDims)}}})
#                cubely.db.metas.update({'code': 'cubes'}, {'$push': {'value': {'code': self.code, 'desc': self.description, 'type': self.type, 'dims': list(self.dimensions)}}})
        else:
            cubely.db.metas.insert({'code': 'cubes', 'value': [{'code': self.code, 'desc': self.description, 'type': self.type, 'dims': list(self.dimensions), 'aggregated_dims':list(self.aggregatedDims)}]})
#            cubely.db.metas.insert({'code': 'cubes', 'value': [{'code': self.code, 'desc': self.description, 'type': self.type, 'dims': list(self.dimensions)}]})
        cubely.CUBES[self.code] = copy.copy(self)
        tmpCode = 'cubely.V_' + self.code + ' = cubely.CUBES[\'' + self.code + '\']'
        exec(tmpCode)
        return cubely.CUBES[self.code]

    def delete(self, code):
        code = code.upper()
        cubely.db.db.drop_collection(self.collectionName)
        cubely.common.delete_meta_simple('cubes', code)
        del cubely.CUBES[code]
        tmpCode = 'del cubely.V_' + code
        exec(tmpCode)

    def set(self, pos, val):
        dimsTup = cubely.common.get_position_tuple(pos)
        # Checking the value is of the correct type
        if {
            #'numeric': cubely.common.is_numeric(val),
            'int': cubely.common.is_numeric(val),
            'float': cubely.common.is_numeric(val),
            'boolean': True,
            'string': isinstance(val, basestring)
        }[self.type]:
            # set new value
            self._values[dimsTup] = {'int': int(float(val)), 'float': float(val), 'boolean': bool(val), 'string': val}[self.type]
            # flaging the changed values
            self.changedValues.add(dimsTup)
            cubely.MODIFIED_CUBES.add(self.code)
        else:
            raise TypeError

    def get(self, pos):
        dimsTup = []
        for d in self.dimensions:
            dimsTup.append(pos[d])
            if not pos.has_key(d):
                raise CubeError(d, 'Invalid dimension for cube')
            if not cubely.DIMS[d].has_position(pos[d]):
                raise PositionError(d, 'Position does not exist')
        try:
            return self._values[tuple(dimsTup)]
        except KeyError:
            # Check if the position is stored but not loaded in memory
            stored = self.cubeCollection.find_one(pos)
            if stored:
                self._values[tuple(dimsTup)] = stored['value']
                return self._values[tuple(dimsTup)]
            else:
                # it really does not exist
                return None

    def update(self):
        # ensure that the collection is created and indexed
        self.cubeCollection.ensure_index(
            [(dim, ASCENDING) for dim in self.dimensions],
            dropDups=True,
            unique=True,
            background=True
        )
        # write back values
        # we're using a copy of changedValues to allow // update
        lock = Lock()
        lock.acquire()
        immutableChangedValues = self.changedValues
        self.changedValues = set()
        lock.release()
        for coordTuples in immutableChangedValues:
            doc = {}
            dimIndex = 0
            for d in self.dimensions:
                doc[d] = coordTuples[dimIndex]
                dimIndex += 1
            # Check if the document needs to be inserted or updated
            self.cubeCollection.update(doc, {"$set": {"value": self._values[coordTuples]}}, upsert=True)
        # write back metadata changes
        cubeSpecs = cubely.db.metas.find_one({'code': 'cubes'})['value']
        cubeSpecIndex = None
        for cubeSpec in cubeSpecs:
            if cubeSpec['code'] == self.code:
                cubeSpec['desc'] = self.description
                cubeSpec['aggregated_dims'] = list(self.aggregatedDims)
                cubeSpecIndex = cubeSpecs.index(cubeSpec)
                cubeSpecs[cubeSpecIndex] = cubeSpec
                break
        cubely.db.metas.update({'code': 'cubes'}, {'$set': {'value': cubeSpecs}})

    def rollback(self):
        self.changedValues = set()

    def _get(self, code):
        self._clear()
        self.code = code
        self.collectionName = cubely.common.get_collection_name('cube', self.code)
        self.cubeCollection = cubely.db.db[self.collectionName]
        cubes = cubely.db.metas.find_one({'code': 'cubes'})['value']
        for cube in cubes:
            if cube['code'] == code:
                self.description = cube['desc']
                self.dimensions = set(cube['dims'])
                self.type = cube['type']
                self.aggregatedDims = set(cube['aggregated_dims'])
                break
        self._values = {}
        return copy.copy(self)

    def declare_aggregated_dim(self, dim):
        dimObj = cubely.common.get_dim_object(dim)
        self.aggregatedDims.add(dimObj.code)


class Relation(object):
    code = ''
    description = ''
    dimensions = set
    collectionName = None
    links = []
    
    def _clear(self):
        """Clear the instance properties. Private."""
        self.code = ''
        self.description = u''
        self.dimensions = set()
        self.links = []
    
    def create(self, dims, code, desc=None):
        self._clear()
        code = code.upper()
        if code in cubely.RELS.keys():
            raise CubeError(code, 'Relation code already exists')
        for dim in dims:
            if dim.__class__ == Dimension:
                self.dimensions.add(dim.code)
            else:
                self.dimensions.add(dim)
        self.code = code
        self.description = desc
        self.collectionName = cubely.common.get_collection_name('relation', False, self.code)
        #self.cubeCollection = cubely.db.db[self.collectionName]
        cubely.DIMS_IN_USE.update(self.dimensions)
        relSpecs = cubely.db.metas.find_one({'code': 'cubes'})
        flagExist = False
        if relSpecs:
            for existingRel in relSpecs['value']:
                if existingRel['code'] == code:
                    flagExist = True
                    break
            if not flagExist:
                cubely.db.metas.update({'code': 'relations'}, {'$push': {'value': {'code': self.code, 'desc': self.description, 'dims': list(self.dimensions)}}})
        else:
            cubely.db.metas.insert({'code': 'relations', 'value': [{'code': self.code, 'desc': self.description, 'dims': list(self.dimensions)}]})
        cubely.RELS[self.code] = copy.copy(self)
        tmpCode = 'cubely.R_' + self.code + ' = cubely.RELS[\'' + self.code + '\']'
        exec(tmpCode)
        return cubely.RELS[self.code]


class Formula(Cube):
    formula = u''
    _storage_cube = None

    def _clear(self):
        self.formula = u''
        self._storage_cube = None
        Cube._clear(self)

    def _get(self, code):
        self._clear()
        self.code = code.upper()
        formulas = cubely.db.metas.find_one({'code': 'formulas'})['value']
        for formula in formulas:
            if formula['code'] == self.code:
                self.description = formula['desc']
                self.formula = formula['formula']
                break
        """
        This exec does not generaly yeld data (the status is generally null
        when its executed).
        It's about getting the right dims for the formula.
        """
        exec('self._storage_cube = ' + self.formula)
        self.dimensions = self._storage_cube.dimensions
        return copy.copy(self)

    def __str__(self):
        buffer = '(formula ' + self.code + ') <'
        buffer += self.formula
        buffer = buffer[:-1] + '> ' + self.description
        return buffer

    def create(self, code, taintedFormula, desc = None):
        self._clear()
        if code in cubely.FORMULAS.keys():
            raise CubeError(code, 'Cube code already exists')
        self.code = code.upper()
        self.formula = taintedFormula.replace('@', 'cubely.')
        if desc:
            self.description = desc
        formSpecs = cubely.db.metas.find_one({'code': 'formulas'})
        flagExist = False
        if formSpecs:
            for existingFormula in formSpecs['value']:
                if existingFormula['code'] == self.code:
                    flagExist = True
                    break
            if not flagExist:
                cubely.db.metas.update({'code': 'formulas'}, {'$push': {'value': {'code': self.code, 'desc': self.description, 'formula': self.formula}}})
        else:
            cubely.db.metas.insert({'code': 'formulas', 'value': [{'code': self.code, 'desc': self.description, 'formula': self.formula}]})
        cubely.FORMULAS[self.code] = copy.copy(self)
        tmpCode = 'cubely.F_' + self.code + ' = cubely.FORMULAS[\'' + self.code + '\']'
        exec(tmpCode)
        return cubely.FORMULAS[self.code]

    def get(self, pos):
        for dim in pos.keys():
            cubely.lang.push(dim)
            cubely.lang.lmt(cubely.DIMS[dim], cubely.lang.to, pos[dim])
        dimsTup = cubely.common.get_position_tuple(pos)
        try:
            cellVal = self._values[dimsTup]
        except KeyError:
            exec('self._storage_cube = ' + self.formula)
            cellVal = self._storage_cube.get(pos)
            self._values[dimsTup] = cellVal
        finally:
            for dim in pos.keys():
                cubely.lang.pop(dim)
        return cellVal

    def update(self):
        pass

    def delete(self, code):
        cubely.common.delete_meta_simple('formulas', code)
        del cubely.FORMULAS[code]

    def set(self, pos, val):
        raise CubeError(self.code, 'You cannot set value to a VirtualCube')