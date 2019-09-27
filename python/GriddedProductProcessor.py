#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 15:26:36 2019

@author: jon
"""

import MalardClient as mc
import MalardHelpers as m

import netCDF4 as n

import pandas as pd

from datetime import datetime
from dateutil.relativedelta import relativedelta
from datetime import timedelta
from math import floor
 
import numpy as np

def bucket_wrapper( col, resolution = 500 ):
    def simple_bucket( row ):
        return resolution * floor( row[col] / resolution )   
    return simple_bucket

def averageElevation( xy, time, elevations ):
    res = {}
    x,y = xy    
    res['x'] = x
    res['y'] = y
    res['time'] = time
    res['elev'] = elevations.mean()
    
    return pd.DataFrame(res, index=['x'])        

def processGridCell(queryInfo, gridCellSize, startX, startY, resolution, fillValue = -2147483647):
    
    resultDf = m.getDataFrameFromNetCDF(queryInfo.resultFileName)

    resultDf['x_b'] = resultDf.apply( bucket_wrapper('x'), axis=1 )
    resultDf['y_b'] = resultDf.apply( bucket_wrapper('y'), axis=1 )

    gridded_product = [ averageElevation( k, v['time'].min(), v['elev']  ) for k,v in resultDf.groupby(['x_b','y_b'])  ]

    gridded_df = pd.concat(gridded_product,ignore_index=True)

    pointCount = len(resultDf)
         
    n = floor(gridCellSize / resolution)
    r = range(0, n)
    
    xcoords = [ i * resolution + resolution * 0.5 + startX for i in r]    
    ycoords = [ i * resolution + resolution * 0.5 + startY for i in r]

    def index( point, startX, resolution ):
        return int( ( point - startX ) / resolution  )  
    
    data = np.empty((n,n,1))
    data.fill(fillValue)
    
    print(type(data))
    
    for x,y,e in zip( gridded_df['x'], gridded_df['y'], gridded_df['elev'] ) :
        i = index(x, startX, resolution)
        j = index(y, startY, resolution)
        
        data[i][j][0] = e
    
    return (xcoords, ycoords, data, pointCount)

def writeGriddedProduct( bbox, xcoords, ycoords, data, pointCount, resolution ):
    
    dataset = n.Dataset("{}/GriddedProduct_{}_y{}_m{}_{}_{}.nc".format(output_path, bbox.dataSet.region, bbox.minT.year, bbox.minT.month, bbox.minX, bbox.minY ),'w',format='NETCDF4')
            
    x = dataset.createDimension('x', len(xcoords))
    y = dataset.createDimension('y', len(ycoords))
    time = dataset.createDimension('time', 1)
    nv = dataset.createDimension('nv', 2)
    
    # Create coordinate variables for 4-dimensions
    times = dataset.createVariable('time', np.int32, ('time',))
    xs = dataset.createVariable('x', np.float32, ('x',))
    ys = dataset.createVariable('y', np.float32, ('y',))
    nvs = dataset.createVariable('nv', np.int32, ('nv',))
    elevations = dataset.createVariable('elevation', np.float32, ('time','x','y'))
    
    x_bnds = dataset.createVariable('x_bnds', np.float32, ('x','nv') )
    y_bnds = dataset.createVariable('y_bnds', np.float32, ('y','nv') )
    
    def boundsArray( coords, resolution ):
        bounds = [ ( coord - 0.5 * resolution, coord + 0.5 * resolution ) for coord in coords ]
        boundsArray = np.empty((len(coords), 2))
        
        for i, x in enumerate(bounds):
            lower, upper = x
            boundsArray[i][0] = lower
            boundsArray[i][1] = upper
        
        return boundsArray

        
    times = [ bbox.minT ]
    indicatorVariables = [0,1]
    
    times[:] = np.array(times)
    xs[:] = xcoords
    x_bnds[:] = boundsArray(xcoords, resolution)
    ys[:] = ycoords
    y_bnds[:] = boundsArray(ycoords, resolution)
    
    nvs[:] = np.array(indicatorVariables)
    elevations[:,:,:] = data
            
    dataset.close()
    

environmentName = 'DEVv2'

output_path = '/home/jon/data'
gridCellSize = 100000
resolution = 500
fillValue = -2147483647

client = mc.MalardClient( environmentName )

dataSet = mc.DataSet( 'cryotempo', 'GRIS_BaseC_Q2', 'greenland')

bbox = client.boundingBox(dataSet)

print(str(bbox))

gridcells = client.gridCells(bbox)

processing_dates = []
dt = datetime( bbox.minT.year, bbox.minT.month, 1,0,0,0)

while dt < bbox.maxT :
    nextdt = dt + relativedelta(months=1)
    processing_dates.append( (dt, nextdt - timedelta(seconds=1))  )
    dt = nextdt

projections = ['x','y','time','elev']

gridcells = [gridcells[10]]
processing_dates = [(datetime(2011,1,1,0,0,0),datetime(2011,6,3,0,0,0))]

for gc in gridcells:
    for from_dt, to_dt in processing_dates:
        month_gc = mc.BoundingBox(gc.dataSet, gc.minX, gc.maxX, gc.minY, gc.maxY, from_dt, to_dt, 0)
        queryInfo = client.executeQuery(month_gc, projections)
        
        if queryInfo.status == "Success":
            xc, yc, d, count = processGridCell( queryInfo, gridCellSize, gc.minX, gc.minY, resolution )
            writeGriddedProduct( month_gc, xc, yc, d, count, resolution )
            
        else:
            print(queryInfo.message)
