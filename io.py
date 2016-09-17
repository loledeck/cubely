# -*- coding: utf-8 -*-
"""IO module for cubely"""

import cubely.lang
import cubely.io
import copy
import csv
import cubely
from cubely.common import cube_log
from cubely.errors import PositionAlreadyExistsError

from multiprocessing import cpu_count
from threading import Thread


class Build(object):
    db = {}
    dims = {}
    hiers = {}
    cubes = {}
    formulas = {}
    files = {}
    aggregates = []
    postloads = []
    
    parallel_load = False
    parallel_degree = 0

    def __init__(self, dbCode, dbDesc = u'', buildParams = {}):
        self.db['code'] = dbCode
        self.db['desc'] = dbDesc

        for param in buildParams.keys():
            setattr(self, param, buildParams[param])
        if self.parallel_degree == 0:
            self.parallel_degree = cpu_count()

    def run(self, type='full'):
        # Create structure
        # * database
        if type=='full':
            cubely.db.drop(self.db['code'])
            cubely.db.create(self.db['code'], self.db['desc'])
        # * Dimensions
            for dim in self.dims.keys():
                cubely.dim.create(dim, self.dims[dim])
        # * Hierarchies
            for dim in self.hiers.keys():
                for hier in self.hiers[dim]:
                    cubely.hier.create(dim, hier)
        # * Cubes
            for cube in self.cubes.keys():
                cubely.cube.create(self.cubes[cube]['dims'], self.cubes[cube]['type'], cube, self.cubes[cube]['desc'])

        # * Formulas
            for formula in self.formulas.keys():
                cubely.formula.create(formula, self.formulas[formula]['formula'], self.formulas[formula]['desc'])

        if type=='inc':
            cubely.db.open(self.db['code'])
            
        # * Files
        def _sequential_read(fileList):
            for fic in fileList:
                path = fic['path']
                ficCopy = copy.copy(fic)
                del ficCopy['path']
                cubely.io.fileread(path, ficCopy)

        def _parallel_read(fileList):
            waitingProcesses = []
            for fic in fileList:
                waitingProcesses.append(Thread(target=cubely.io.fileread, args=(fic['path'],copy.copy(fic))))
            cube_log('Nb of files to read: '+str(len(waitingProcesses)), 1, False)

            while len(waitingProcesses) > 0:
                runningProcesses = []
                while len(runningProcesses) < self.parallel_degree:
                    try:
                        p = waitingProcesses[0]
                    except:
                        break
                    runningProcesses.append(p)
                    p.start()
                    del(waitingProcesses[0])
                for p in runningProcesses:
                    p.join()

        cube_log('Starting Reading Files', 1)
        if self.parallel_load:
            _parallel_read(self.files['referential'])
            _parallel_read(self.files['data'])
        else:
            _sequential_read(self.files['referential'])
            _sequential_read(self.files['data'])

        cube_log('Done Reading Files', 1, True)

        # * Aggregations
        if self.parallel_load:
            waitingProcesses = {}

            for cubeCode in self.aggregates.keys():
                partialAggreg = False
                if self.aggregates[cubeCode] != 'all':
                    partialAggreg = self.aggregates[cubeCode]
                waitingProcesses[cubeCode] = Thread(target=cubely.lang.aggregate, args=(cubely.CUBES[cubeCode],), kwargs={'parallel_aggreg': True, 'partial_aggreg': partialAggreg})

            cube_log('Nb of cubes to aggregate: '+str(len(waitingProcesses)), 1, False)

            while len(waitingProcesses) > 0:
                runningProcesses = []
                # While there are cubes to aggregate
                while len(runningProcesses) < self.parallel_degree:
                    # Launch new threads while you don't reach the targetProcessesCount limit
                    try:
                        p = waitingProcesses[waitingProcesses.keys()[0]]
                    except:
                        # Exception when there is no cube left in the queue
                        break
                    runningProcesses.append(p)
                    #cube_log('Len waitingprocess: '+str(len(waitingProcesses)), 1, False)
                    p.start()
                    #cube_log('Spawned process for cube '+waitingProcesses.keys()[0], 1, False)
                    del(waitingProcesses[waitingProcesses.keys()[0]])
                for p in runningProcesses:
                    # Wait for the running threads to complete before launching new ones
                    p.join()
        else:
            for cubeCode in self.aggregates.keys():
                partialAggreg = False
                if self.aggregates[cubeCode] != 'all':
                    partialAggreg = self.aggregates[cubeCode]
                cubely.lang.aggregate(cubely.CUBES[cubeCode], partialAggreg)

        # * Postload
        for runnableCode in self.postloads:
            runnableCode()

        # * End
        cubely.db.close()
        cube_log('Database close. All done.', 1, True)


def _get_fixed_reader(fullFileName):
    reader = open(fullFileName, 'r')
    return reader


def _get_structured_reader(fullFileName, delimiter, quoteChar = None):
    reader = csv.reader(open(fullFileName), delimiter=delimiter, quotechar=quoteChar)
    return reader


def _get_field_value(type, line, index, field):
    if type == 'structured':
        return line[index].strip().decode('utf-8')
    elif type == 'fixed':
        return line[field['start']-1:field['stop']-1].strip().decode('utf-8')
    else:
        raise ValueError

cubesToUpdate = set()

def fileread(fullFileName, definition):
    # Getting a file handle
    if not definition.has_key('separator'):
        definition['separator'] = '\t'

    fic = {
        'fixed': _get_fixed_reader(fullFileName),
        'structured': _get_structured_reader(fullFileName, definition['separator'])
    }[definition['type']]

    operationalDefinition = copy.copy(definition)
    
    filters = []

    for defKey in operationalDefinition.keys():
        if defKey in ['type', 'separator', 'path']:
            del operationalDefinition[defKey]

        if defKey == 'filter':
            filters.append(operationalDefinition[defKey])
            del operationalDefinition[defKey]

    mayBeParentPositions = []
    checkHier = set()

    cube_log('Reading File: ' + fullFileName, 2)
    try:
        for line in fic:
            circuitBreaker = False
            if line[0] == '*':
                continue
            for filter in filters:
                filterIndex = filter['field']
                filterQuery = filter['keep']
                if filterQuery != _get_field_value(
                                    definition['type'],
                                    line,
                                    filterIndex,
                                    operationalDefinition[filterIndex]):
                    circuitBreaker = True
            if circuitBreaker:
                continue
            pendingPosition = {}
            currentPositionCode = u''
            cellToSet = {}
            for fieldKey in operationalDefinition.keys():
                field = operationalDefinition[fieldKey]

                if field['objectType'] == 'Dimension':
                    obj = cubely.DIMS[field['objectCode']]

                if field['objectType'] == 'Hierarchy':
                    obj = cubely.HIERS[field['dataNature']][field['objectCode']]
                    checkHier.add(obj)

                if field['objectType'] == 'Cube':
                    obj = cubely.CUBES[field['objectCode']]
                    # to delete ??
                    #obj.code = field['objectCode']
                    cubesToUpdate.add(obj.code)

                if 'expression' in field.keys():
                    field_value = field['expression']
                else:
                    field_value = _get_field_value(definition['type'], line, fieldKey, field)

                # Adding positions
                # ** Position code
                if field['objectType'] == 'Dimension' and field['dataNature'] == 'code':
                    currentPositionCode = field_value
                    if 'MATCH' in field['metas']:
                        if obj.has_position(currentPositionCode):
                            cellToSet[obj.code] = currentPositionCode
                        else:
                            print '** Invalid ' + obj.code + ' position code : ' + currentPositionCode
                            continue

                    if 'APPEND' in field['metas']:
                        if not obj.has_position(currentPositionCode):
                            if pendingPosition == {}:
                                pendingPosition['code'] = currentPositionCode
                            else:
                                obj.add_position(currentPositionCode, pendingPosition['desc'])
                # ** Position desc
                if field['objectType'] == 'Dimension' and field['dataNature'] == 'desc':
                    desc = field_value
                    if pendingPosition == {}:
                        pendingPosition['desc'] = desc
                    else:
                        try:
                            obj.add_position(pendingPosition['code'], desc)
                        except PositionAlreadyExistsError:
                            pass

                # Parentage informations
                if field['objectType'] == 'Hierarchy':
                    parentCode = field_value
                    if parentCode != u'':
                        try:
                            obj.set(currentPositionCode, parentCode)
                        except cubely.errors.HierarchyError:
                            mayBeParentPositions.append({'dim': obj.dimension, 'hier': obj.code, 'position':currentPositionCode, 'parent':parentCode})

                # Setting cube value
                if field['objectType'] == 'Cube':
                    cellVal = field_value
                    obj.set(cellToSet, cellVal)


        # Check the maybe positions
        if len(mayBeParentPositions) > 0:
            for pos in mayBeParentPositions:
                try:
                    cubely.HIERS[pos['dim']][pos['hier']].set(pos['position'], pos['parent'])
                except cubely.errors.HierarchyError:
                    print '* Unknown parent position ' + pos['parent']

        # Saving cubes that have been updated
        for cubeCode in cubesToUpdate:
            cube_log('Updating cube ' + cubeCode, 2)
            cubely.CUBES[cubeCode].update()

        # Checking hiers
        for hier in checkHier:
            cubely.lang.check_hier(hier)
    finally:
        if definition['type'] == 'fixed':
            fic.close()



