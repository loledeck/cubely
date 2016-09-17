import os

import cubely.io

build_params = {
    'parallel_load': False
    ,'parallel_degree': 2
}

sakila = cubely.io.Build('sakila', 'Demo database based on mysql sakila db', build_params)

dims = {
    'PROD': 'Products',
    'GEOG': 'Geography',
    'TIME': 'Time',
    'ACTOR': 'Actors'
}
hiers = {
    'PROD': ['STD'],
    'GEOG': ['STD'],
    'TIME': ['STD']
}
cubes = {
    'RENTALS': {'dims': ['PROD', 'GEOG', 'TIME'], 'type': 'int', 'desc': 'Rentals data'},
    'SALES': {'dims': ['PROD', 'GEOG', 'TIME'], 'type': 'float', 'desc': 'Sales data'},
    'INVENTORY': {'dims': ['PROD'], 'type': 'int', 'desc': 'Inventory data'}
}
formulas = {
	'REVENUE': {'desc': 'Global revenue', 'formula':'@V_RENTALS + @V_SALES'}
}
definitionProd = {
    'type': 'structured'
    ,'separator': '\t'
    ,'path': os.path.dirname(__file__)+'/../data/prods.txt'
    ,0: {'objectType': 'Dimension', 'objectCode': 'PROD', 'dataNature': 'code', 'metas': ['APPEND']}
    ,1: {'objectType': 'Dimension', 'objectCode': 'PROD', 'dataNature': 'desc'}
    ,2: {'objectType': 'Hierarchy', 'objectCode': 'STD', 'dataNature': 'PROD'}
}
definitionGeog = {
    'type': 'structured'
    ,'separator': '\t'
    ,'path': os.path.dirname(__file__)+'/../data/geography.txt'
    ,0: {'objectType': 'Dimension', 'objectCode': 'GEOG', 'dataNature': 'code', 'metas': ['APPEND']}
    ,1: {'objectType': 'Dimension', 'objectCode': 'GEOG', 'dataNature': 'desc'}
    ,2: {'objectType': 'Hierarchy', 'objectCode': 'STD', 'dataNature': 'GEOG'}
}
definitionTime = {
    'type': 'structured'
    ,'separator': '\t'
    ,'path': os.path.dirname(__file__)+'/../data/time.txt'
    ,0: {'objectType': 'Dimension', 'objectCode': 'TIME', 'dataNature': 'code', 'metas': ['APPEND']}
    ,1: {'objectType': 'Dimension', 'objectCode': 'TIME', 'dataNature': 'desc'}
    ,2: {'objectType': 'Hierarchy', 'objectCode': 'STD', 'dataNature': 'TIME'}
}
definitionRentals = {
    'type': 'structured'
    ,'separator': '\t'
    ,'path': os.path.dirname(__file__)+'/../data/rentals.txt'
    ,0: {'objectType': 'Dimension', 'objectCode': 'PROD', 'dataNature': 'code', 'metas': ['MATCH']}
    ,1: {'objectType': 'Dimension', 'objectCode': 'GEOG', 'dataNature': 'code', 'metas': ['MATCH']}
    ,2: {'objectType': 'Dimension', 'objectCode': 'TIME', 'dataNature': 'code', 'metas': ['MATCH']}
    ,3: {'objectType': 'Cube', 'objectCode': 'RENTALS'}
}
definitionSales = {
    'type': 'structured'
    ,'separator': '\t'
    ,'path': os.path.dirname(__file__)+'/../data/sales.txt'
    ,0: {'objectType': 'Dimension', 'objectCode': 'PROD', 'dataNature': 'code', 'metas': ['MATCH']}
    ,1: {'objectType': 'Dimension', 'objectCode': 'GEOG', 'dataNature': 'code', 'metas': ['MATCH']}
    ,2: {'objectType': 'Dimension', 'objectCode': 'TIME', 'dataNature': 'code', 'metas': ['MATCH']}
    ,3: {'objectType': 'Cube', 'objectCode': 'SALES'}
}
definitionInventory = {
    'type': 'structured'
    ,'separator': '\t'
    ,'path': os.path.dirname(__file__)+'/../data/inventory.txt'
    ,0: {'objectType': 'Dimension', 'objectCode': 'PROD', 'dataNature': 'code', 'metas': ['MATCH']}
    ,1: {'objectType': 'Cube', 'objectCode': 'INVENTORY'}
}

files = {
    'referential': [
        definitionGeog,
        definitionProd,
        definitionTime
    ],
    'data': [
        definitionRentals
        ,definitionSales
        ,definitionInventory
    ]
}

def postload():
    print 'My custom treatments go here !'

sakila.dims = dims
sakila.hiers = hiers
sakila.cubes = cubes
sakila.files = files
#sakila.aggregates = {'RENTALS':['GEOG'], 'SALES':'all', 'INVENTORY':'all'}
#sakila.aggregates = {'RENTALS':'all', 'SALES':'all', 'INVENTORY':'all'}
sakila.aggregates = {'RENTALS':'all'}
sakila.postloads = [postload]