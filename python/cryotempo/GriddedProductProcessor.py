#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 15:26:36 2019

@author: jon
"""
import netCDF4 as n

import pandas as pd

from datetime import datetime
from dateutil.relativedelta import relativedelta
from datetime import timedelta
from math import floor

from MalardClient.MalardClient import MalardClient
from MalardClient.DataSet import DataSet
from MalardClient.BoundingBox import BoundingBox

import sys
import os
 
import numpy as np

def bucket_series( series, resolution ):
    res = np.empty(len(series))
    for i, x in enumerate(series):
        res[i] =  resolution * floor( x / resolution ) + resolution * 0.5
    return res      

def statistics( x, y, t, process_time, g_count, p_count, inmask_count, masked_pcount ):
    res ={}        
    res['x'] = x
    res['y'] = y
    res['t'] = t
    res['process_time'] = process_time
    res['gridded_count'] = g_count
    res['point_count'] = p_count
    res['inmask_count'] = inmask_count
    res['masked_pcount'] = masked_pcount
    
    return pd.DataFrame(res, index=['x'])

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def processGridCell(client, queryInfo, gridCellSize, startX, startY, resolution, mask, fillValue = -2147483647):
    
    resultDf = queryInfo.to_df
    client.releaseCacheHandle( queryInfo.resultFileName )
    
    resultDf['x_b'] = bucket_series( resultDf['x'], resolution )
    resultDf['y_b'] = bucket_series( resultDf['y'], resolution )
    
    pointCount = len(resultDf)
    maskedCount = 0
         
    n = floor(gridCellSize / resolution)
    r = range(0, n)
    
    xcoords = [ i * resolution + resolution * 0.5 + startX for i in r]    
    ycoords = [ i * resolution + resolution * 0.5 + startY for i in r]

    def index( point, startX, resolution ):
        return int( ( point - startX ) / resolution  )  
    
    data = np.empty((n,n,1))
    data.fill(fillValue)
    
    dataN = np.empty((n,n,1))
    dataN.fill(fillValue)
    
    griddedCount = 0 
    
    for x,y,e in zip( resultDf['x_b'], resultDf['y_b'], resultDf['elev'] ) :
        i = index(x, startX, resolution)
        j = index(y, startY, resolution)
        
        p = (x,y)
        if p in mask:
            maskedCount = maskedCount + 1
            if data[i][j][0] == fillValue:
                griddedCount = griddedCount + 1
                data[i][j][0] = e
                dataN[i][j][0] = 1
            else:
                data[i][j][0] = data[i][j][0] + e
                dataN[i][j][0] = dataN[i][j][0] + 1
            
    for i,x in enumerate( xcoords ):
        for j,y in enumerate( ycoords ):
            if data[i][j][0] != fillValue:
                data[i][j][0] = data[i][j][0] / dataN[i][j][0] 
    
    return (xcoords, ycoords, data, griddedCount, pointCount, maskedCount)

def writeGriddedProduct(output_path, dataSet, bbox, xcoords, ycoords, data, resolution ):
    
    fullPath = "{}/GriddedProduct_{}_y{}_m{}_{}_{}.nc".format(output_path, dataSet.region, bbox.minT.year, bbox.minT.month, bbox.minX, bbox.minY )
    
    ensure_dir(fullPath)
    
    dataset = n.Dataset(fullPath,'w',format='NETCDF4')
            
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

def loadMasks( client, dataSet, gridCell,resolution):
    import json
    
    maskTypes = ["LRM_{}m".format(resolution),"SARIN_{}m".format(resolution),"ICE_{}m".format(resolution)]
    
    mask_dataframes = [  pd.read_csv(json.loads(client.query.getGridCellMask(dataSet.parentDataSet, dataSet.dataSet, maskType, dataSet.region, gridCell.minX, gridCell.minY, gridCell.maxX - gridCell.minX ))["fileName"]  ) for maskType in maskTypes] 
        
    inmask = {}

    for x, y, lrm, sarin, ice in zip(  mask_dataframes[0]["x"], mask_dataframes[0]["y"], mask_dataframes[0]["within_LRM"], mask_dataframes[1]["within_SARIN"], mask_dataframes[2]["within_ICE"]):
        if lrm == 0 and sarin == 1 and ice == 1:
            inmask[(x,y)] = 1
            
    return inmask

def main( argv ):
    
    argv = argv[1:]
    
    year = 2011
    
    environmentName = 'DEVv2'
    ndays = int(argv[0])
    resolution = int(argv[1])
    print(resolution)
    interval = "{}days".format(ndays)
    
    output_path = '/home/jon/data/{}/y{}'.format(interval, year)
    gridCellSize = 100000
    fillValue = -2147483647
    
    client = MalardClient( environmentName, False )
    
    dataSet = DataSet( 'cryotempo', 'GRIS_BaselineC_Q2', 'greenland')
    
    bbox = client.boundingBox(dataSet)
    
    print(str(bbox))
    
    gridcells = client.gridCells(dataSet, bbox)
    
    processing_dates = []
    publication_dt = datetime( year, 6, 30,23,59,59)
    
    end_dt = datetime( year, 6 , 30, 23,59,59)
    
    while publication_dt <=  end_dt :
        next_publication_dt = publication_dt + relativedelta(months=1)
        processing_dates.append( ( publication_dt - relativedelta(days=ndays) , publication_dt ) )
        publication_dt = next_publication_dt - timedelta(seconds=1)
    
    print(processing_dates)
    projections = ['x','y','time','elev']
    #maskTypes = ["ICE_{}m".format(resolution),"SARIN_{}m".format(resolution),"Glacier_{}m".format(resolution)]
    
    stats = []
    total = len(gridcells)
    
    for i, gc in enumerate(gridcells):
        gc_start = datetime.now()
        for from_dt, to_dt in processing_dates:
            month_gc = BoundingBox(gc.minX, gc.maxX, gc.minY, gc.maxY, from_dt, to_dt)
            queryInfo = client.executeQuery(dataSet, month_gc, projections)
            
            if queryInfo.status == "Success":
                start_time = datetime.now()
                mask_dict = loadMasks(client, dataSet, month_gc, resolution  ) 
                xc, yc, d, g_count, i_count, m_count = processGridCell(client, queryInfo, gridCellSize, gc.minX, gc.minY, resolution, mask_dict, fillValue )
                writeGriddedProduct(output_path, dataSet, month_gc, xc, yc, d, resolution )
                end_time = datetime.now()
                
                inmask_count = len(mask_dict.keys())
                elapsed_time = (end_time - start_time).total_seconds()
                stats.append( statistics(gc.minX, gc.minY, from_dt, elapsed_time, g_count, i_count, inmask_count, m_count ) )
            else:
                stats.append( statistics(gc.minX, gc.minY, from_dt, 0, 0, 0, 0, 0 ) )
        gc_elapsed = ( datetime.now() - gc_start).total_seconds() 
        print('Processed [{}] grid cells. Total=[{}] Took=[{}]s'.format(i+1, total, gc_elapsed ))
        
    stats_df = pd.concat( stats, ignore_index=True )
    stats_path = '/home/jon/data/stats/{}/{}_{}_{}m.csv'.format(interval,dataSet.region,year,resolution)
    
    ensure_dir(stats_path)
    
    stats_df.to_csv( stats_path )

if __name__ == "__main__":
    main(sys.argv)
                