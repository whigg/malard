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
from MalardClient.MaskFilter import MaskFilter

import ShapeFileIndex as s

import sys
import os

import Header as h
 
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

def processGridCell(client, resultDf, gridCellSize, startX, startY, resolution, mask, fillValue = -2147483647):
    
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
            elif (xcoords[i],ycoords[j]) in mask:
                data[i][j][0] = -1000
    
    return (xcoords, ycoords, data, griddedCount, pointCount, maskedCount)

def writeGriddedProduct(output_path, dataSet, bbox, xcoords, ycoords, data, resolution, proj4, pub_date, index, file_mappings, fill_value ):
    
    pubdatestr = "{}15".format(pub_date.strftime("%Y%m"))
    fileNameExt = ".nc"
    filetype = "THEM_GRID_"
    fileName = "CS_TEST_{}_{}_{}_{}_{}_V1".format( filetype, dataSet.region, pubdatestr, bbox.minX, bbox.minY )
    productPath = "y{}/m{}/cell_{}_{}/{}{}".format(pub_date.year, pub_date.month, bbox.minX, bbox.minY, fileName, fileNameExt)
    fullPath = "{}/{}".format(output_path, productPath)
    headerPath = "{}/y{}/m{}/cell_{}_{}/{}.HDR".format(output_path, pub_date.year, pub_date.month, bbox.minX, bbox.minY, fileName)

    fileDescription = "L3 Gridded thematic product containing interpolated swath data that is generated from CryoSat2 SARIN data."    

    ensure_dir(fullPath)
    ensure_dir(headerPath)
    
    index.addGridCell( bbox, productPath   )
    
    dataset = n.Dataset(fullPath,'w',format='NETCDF4')
    
    minX = bbox.minX
    maxX = bbox.maxX
    minY = bbox.minY 
    maxY = bbox.maxY
    minT = bbox.minT.isoformat()
    maxT = bbox.maxT.isoformat()
    
    dataset.cdm_data_type = "Gridded"                 
    dataset.Conventions = "CF-1.7"
    dataset.Metadata_Conventions = "Unidata Dataset Discovery v1.0"                
    dataset.comment = "Gridded file containing elevation estimates on a regular grid"                 
    dataset.contact = "cryotempo@earthwave.co.uk"               
    dataset.creator_email = "cryotempo@earthwave.co.uk"                 
    dataset.creator_url = "http://www.earthwave.co.uk"                  
    dataset.date_created = datetime.now().isoformat()                                
    dataset.date_modified = datetime.now().isoformat() 
    #dataset.external_dem = "DEM"        
    dataset.geospatial_y_min = minY                 
    dataset.geospatial_y_max = maxY         
    dataset.geospatial_x_min = minX                
    dataset.geospatial_x_max = maxX                  
    dataset.geospatial_y_units = "metres" 
    dataset.geospatial_x_units = "metres"
    dataset.geospatial_projection = proj4   
    dataset.geospatial_resolution = 2000
    dataset.geospatial_resolution_units = "metres"
    dataset.institution = "ESA, UoE, Earthwave, isardSAT"                 
    dataset.keywords = "Land Ice > Gridded > Elevation Model  > Elevation Points > Swath Processing > CryoSat2"                 
    dataset.keywords_vocabulary = "NetCDF COARDS Climate and Forecast Standard Names"                 
    dataset.platform = " Cryosat-2"
    dataset.processing_level = "L3"
    dataset.product_version = "1.0"
    dataset.project = "CryoTEMPO which is an evolution of CryoSat+ CryoTop"                 
    dataset.references = "http://www.cryotempo.org"
    dataset.source = "Gridded Swath data generated from Cryo-Sat2 SARIN data."
    dataset.version = 1
    dataset.summary = "Land Ice Elevation Thematic Gridded Product" 
    dataset.time_coverage_duration = "P1M"
    dataset.time_coverage_start = minT 
    dataset.time_coverage_end = maxT
    dataset.title = "Land Ice Elevation Thematic Gridded Product"

    x = dataset.createDimension('x', len(xcoords))
    y = dataset.createDimension('y', len(ycoords))
    time = dataset.createDimension('time', 1)
    nv = dataset.createDimension('nv', 2)
    
    # Create coordinate variables for 4-dimensions
    times = dataset.createVariable('time', np.int32, ('time',))
    times.long_name = "Measurement of time"
    times.units = "Seconds from 1970 in the UTC timezone."
    times.short_name = "time"
    xs = dataset.createVariable('x', np.float32, ('x',))
    xs.long_name = "Distance in horizontal direction on the Earth’s surface."
    xs.units = "metres"
    xs.standard_name = "x"
    ys = dataset.createVariable('y', np.float32, ('y',))
    ys.long_name = "Distance in vertical direction on the Earth’s surface."
    ys.units = "metres"
    ys.standard_name = "y"
    elevations = dataset.createVariable('elevation', np.float32, ('time','x','y'), fill_value = fill_value)
    elevations.units = "metres"
    elevations.long_name = "Elevation estimate for a point in space at the pixel centre and time" 
    elevations.coordinates = "x y"

    nvs = dataset.createVariable('nv', np.int32, ('nv',))
    nvs.long_name = "Vertex"
    nvs.comment = "Vertex with values 0 or 1, where 0 is westerly or southerly and 1 is northerly or easterly"
    nvs.units = "Binary: 0 or 1"
    nvs.short_name = "nv"
    x_bnds = dataset.createVariable('x_bnds', np.float32, ('x','nv') )
    x_bnds.long_name = "x minimum and maximum bounds"
    x_bnds.comment = "x values at the west and east boundary of each pixel."                
    x_bnds.units = "metres" 
    x_bnds.short_name = "x_bnds"
    y_bnds = dataset.createVariable('y_bnds', np.float32, ('y','nv') )
    y_bnds.long_name = "y minimum and maximum bounds"
    y_bnds.comment = "y values at the north and south boundary of each pixel."                
    y_bnds.units = "metres" 
    y_bnds.short_name = "y_bnds"
    
    def boundsArray( coords, resolution ):
        bounds = [ ( coord - 0.5 * resolution, coord + 0.5 * resolution ) for coord in coords ]
        boundsArray = np.empty((len(coords), 2))
        
        for i, x in enumerate(bounds):
            lower, upper = x
            boundsArray[i][0] = lower
            boundsArray[i][1] = upper
        
        return boundsArray

        
    timesArray = np.zeros(1) 
    timesArray.fill( int(bbox.minT.timestamp() ) )
    
    indicatorVariables = [0,1]
    
    times[:] = timesArray
    xs[:] = xcoords
    x_bnds[:] = boundsArray(xcoords, resolution)
    ys[:] = ycoords
    y_bnds[:] = boundsArray(ycoords, resolution)
    
    nvs[:] = np.array(indicatorVariables)
    elevations[:,:,:] = data
            
    dataset.close()
    
    size = os.stat(fullPath).st_size
    attributes = { "File_Description" : fileDescription, "File_Type": "THEM_GRID_", "File_Name" : fileName, "Validity_Start" : minT, "Validity_Stop" : maxT, "Min_X" : bbox.minX, "Max_X" : bbox.maxX, "Min_Y" : bbox.minY, "Max_Y" : bbox.maxY, "Creator" : "Earthwave", "Creator_Version" : 0.1, "Tot_size" : size,"Projection": proj4, "Start_X":minX, "Stop_X":maxX,"Start_Y":minY,"Stop_Y":maxY,"Grid_Pixel_Width" : 2000, "Grid_Pixel_Height" : 2000  } 

    with open( headerPath, "wt", encoding="utf8"  ) as f:
        f.write( h.createHeader( attributes, gridded=True, source_files = file_mappings ))

def loadMasks( client, gridCell, maskFilters, resolution):
    
    inmask = {}
    
    df = client.filterGriddedPoints( gridCell.minX, gridCell.maxX, gridCell.minY, gridCell.maxY, maskFilters , resolution )
    
    for x,y,b in zip(df['x'],df['y'],df['inFilter']):
        if b == True:
            inmask[(x,y)] = 1
            
    return inmask

def main( argv ):
    
    #argv = argv[1:]
    
    year = 2011
    
    environmentName = 'DEVv2'
    #ndays = int(argv[0])
    resolution = 2000 #int(argv[1])
    print(resolution)
    interval = "3months" #"{}days".format(ndays)
    
    ## TODO: These need to be stored in Malard by DataSet and Type.    
    maskFilterIce = MaskFilter( p_shapeFile="/data/puma1/scratch/cryotempo/masks/icesheets.shp"  ) 
    maskFilterLRM = MaskFilter( p_shapeFile="/data/puma1/scratch/cryotempo/sarinmasks/LRM_Greenland.shp" , p_includeWithin=False ) 

    maskFilters = [ maskFilterIce, maskFilterLRM ]
      
    output_path = "/home/jon/data/grid"
    gridCellSize = 100000
    fillValue = -2147483647
    
    client = MalardClient( environmentName, True )
    
    dataSet = DataSet( 'cryotempo', 'GRIS_BaselineC_Q2', 'greenland')
    
    proj4 = client.getProjection(dataSet).proj4
    
    bbox = client.boundingBox(dataSet)
    
    print(str(bbox))
    
   
    gridcells = client.gridCells(dataSet, bbox)
    
    start_date = datetime( year, 3, 15,0,0,0)
    last_date = datetime( year, 4 , 15, 0, 0, 0)
    
    window = []
    
    while start_date < last_date:
        window_start = start_date - relativedelta(days=start_date.day) + relativedelta(days=1) - relativedelta(months=1)
        window_end = window_start + relativedelta(months=3) - timedelta(seconds=1)
        window.append( (window_start, window_end, start_date) )
        start_date = start_date + relativedelta( months=1 )
    
    projections = ['x','y','time','elev','swathFileId']
    #maskTypes = ["ICE_{}m".format(resolution),"SARIN_{}m".format(resolution),"Glacier_{}m".format(resolution)]
    
    stats = []
    total = len(gridcells)
    
    for from_dt, to_dt, pub_date in window:
        index = s.ShapeFileIndex(output_path, "THEM_GRID_", proj4, dataSet.region, pub_date ) 
        for i, gc in enumerate(gridcells):
            gc_start = datetime.now()        
            month_gc = BoundingBox(gc.minX, gc.maxX, gc.minY, gc.maxY, from_dt, to_dt)
            queryInfo = client.executeQuery(dataSet, month_gc, projections)
            
            if queryInfo.status == "Success":
                data = queryInfo.to_df
                client.releaseCacheHandle( queryInfo.resultFileName )
                
                file_ids = data['swathFileId'].unique()
                file_mappings = client.getSwathNamesFromIds( dataSet, file_ids )
                    
                start_time = datetime.now()
                mask_dict = loadMasks(client, month_gc, maskFilters, resolution  ) 
                xc, yc, d, g_count, i_count, m_count = processGridCell(client, data, gridCellSize, gc.minX, gc.minY, resolution, mask_dict, fillValue)
                writeGriddedProduct(output_path, dataSet, month_gc, xc, yc, d, resolution, proj4, pub_date,index, file_mappings, fillValue )
                end_time = datetime.now()
                
                inmask_count = len(mask_dict.keys())
                elapsed_time = (end_time - start_time).total_seconds()
                stats.append( statistics(gc.minX, gc.minY, from_dt, elapsed_time, g_count, i_count, inmask_count, m_count ) )
            else:
                stats.append( statistics(gc.minX, gc.minY, from_dt, 0, 0, 0, 0, 0 ) )
        index.close()
        gc_elapsed = ( datetime.now() - gc_start).total_seconds() 
        print('Processed [{}] grid cells. Total=[{}] Took=[{}]s'.format(i+1, total, gc_elapsed ))
        
    stats_df = pd.concat( stats, ignore_index=True )
    stats_path = '/home/jon/data/stats/{}/{}_{}_{}m.csv'.format(interval,dataSet.region,year,resolution)
    
    ensure_dir(stats_path)
    
    stats_df.to_csv( stats_path )

if __name__ == "__main__":
    main(sys.argv)
                