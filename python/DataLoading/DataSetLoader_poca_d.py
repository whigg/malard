# -*- coding: utf-8 -*-

import sys
from os import listdir
from datetime import datetime
import MalardClient.AsyncDataSetQuery as aq
import pandas as pd
import numpy as np
import re
import netCDF4 
import os
import MalardClient.MalardClient as mc
import DataSetLoader as dsl
from osgeo import gdal

from dateutil.relativedelta import relativedelta
from datetime import timedelta

import Timeseries as t

def getPixel(x, y, gt):
    '''
    :param x: pandas df
    :param y: pandas df
    :param gt: GeoTransform
    :return:
    ''' 
    py = ((x-gt[0])/gt[1]).astype('int64')
    px = ((gt[3]-y)/-gt[5]).astype('int64')
    
    return px.values, py.values

def getArray( dem ):
    ad = gdal.Open(dem, gdal.GA_ReadOnly)
    adArray = ad.GetRasterBand(1).ReadAsArray()
    adGt = ad.GetGeoTransform()
    
    return ( adArray, adGt )

def createTempFiles( swaths, src_dir, tempDir, columnMappings ):
    
    for s,d in swaths:
        print("Creating temp poca file {}".format(s))
        nc = netCDF4.Dataset(os.path.join(src_dir, s))
        data = {}

        for v in columnMappings.keys():
            d = nc.variables[v]
            srs = pd.Series(d[:])
            data[v] = srs

        df = pd.DataFrame(data)
        nc.close()
        
        ds = netCDF4.Dataset(os.path.join(tempDir, s),'w',format='NETCDF4')
        ds.createDimension( 'row', None )
        
        for column, dtype in zip(df.columns, df.dtypes):
            col = ds.createVariable(columnMappings[column], dtype, ('row',))
            col[:] = np.array(df[column])
        
        wf = ds.createVariable( "wf_number", np.int32, ("row", ))
        wf[:] = range(1, len(df)+1 )
           
        ds.close()

def createTempFile(tempDir, fileName, df ):
    ds = netCDF4.Dataset(os.path.join(tempDir, fileName),'w',format='NETCDF4')
    ds.createDimension( 'row', None )
    
    for column, dtype in zip(df.columns, df.dtypes):
        col = ds.createVariable(column, dtype, ('row',))
        col[:] = np.array(df[column])
    
    ds.close()
   
def cleanUpTempFiles( swaths, tempDir ):
    for s,d in swaths:
        os.remove( os.path.join(tempDir, s) )
        
def main(month, year):
    # My code here
    # Get the arguments from the command-line except the filename   
    parentDataSet = 'test'
    dataSet = 'poca_d_nw_esa'
    region = 'greenland'
    swathdir = '/data/snail/scratch/rawdata/poca_d'
    tempdir = '/data/puma/scratch/malard/tempnetcdfs'
    gris_dem = '/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/combined/GrIS_dem.tif'
    
    environmentName = 'MALARD-PROD'    
    
    client = mc.MalardClient(environmentName)
    
    columnFilters = []
    pocaColumns = {'height_1_20_ku' : 'elev', 'lat_poca_20_ku' : 'lat','lon_poca_20_ku' : 'lon' }
    
    includeColumns = []
    
    ice_file = "/data/puma/scratch/cryotempo/masks/greenland/icesheets.shp"
    maskFilterIce = mc.MaskFilter( p_shapeFile=ice_file)
    maskFilterLRM = mc.MaskFilter( p_shapeFile="/data/puma/scratch/cryotempo/masks/greenland/LRM_Greenland.shp" , p_includeWithin=False )
    maskFilters = [maskFilterIce, maskFilterLRM] 
	         
    gridCellSize = 100000
        
    swathfiles = [(f,dateFromFileName(f)) for f in listdir(swathdir) if  f.endswith(".nc") and isyearandmonth( f, year, month )]
            
    createTempFiles( swathfiles, swathdir, tempdir, pocaColumns  )
            
    message = 'Processing Year Month %d-%d. Num Swaths %d' % (year,month,len(swathfiles) )
    print(message)
    
    if len(swathfiles) > 0:
        dsl.publishData( environmentName, swathfiles, parentDataSet, dataSet, region, tempdir, columnFilters, includeColumns, gridCellSize, maskFilters )
        
    cleanUpTempFiles( swathfiles, tempdir )
    
    #load the results. Need to add the demDiff
    if len(swathfiles) > 0:
        ds = mc.DataSet( parentDataSet, dataSet, region )
        
        minT = datetime( year, month, 1, 0,0,0 )
        maxT = minT + relativedelta( months=1 ) - timedelta( seconds=1)
        
        demArray, demGt = getArray(gris_dem)
        extent = t.getExtent(gris_dem)
        print(extent) 
               
        bb = mc.BoundingBox(extent[0], extent[1], extent[2], extent[3], minT, maxT)
        gcs = client.gridCells(ds,bb)
        print("Number of gridCells found {}".format(len(gcs)))        
        for gc in gcs:
            gc_bb = mc.BoundingBox(gc.minX, gc.maxX, gc.minY, gc.maxY, minT, maxT)
       
            info = client.executeQuery(ds, gc_bb)
                
            if info.status == "Success" and not info.resultFileName.startswith("Error"):
                df = info.to_df
                if len(df) > 0:
                    
                    print("MinX {} MaxX {} MinY {} MaxY {}".format(df.x.min(), df.x.max(), df.y.min(), df.y.max() ))
                    demElev = demArray[getPixel(df.x, df.y, demGt)]
                    esaElev = df['elev']
                    demDiff = np.empty(len(demElev))
                    demDiff.fill(np.nan)
                    
                    for i in range(0,len(demElev)):
                        if demElev[i] > -9999:
                            demDiff[i] = esaElev[i] - demElev[i]
                            
                    df["demDiff"] = demDiff
                
                    file = "PocaDD_{}_{}_{}_{}_{}_{}.nc".format(parentDataSet, dataSet, region, gc_bb.minX, gc_bb.minY, gc_bb.minT )
                    createTempFile(tempdir, file , df)
                
                    projection = client.getProjection(ds).shortName
                    client.asyncQuery.publishGridCellPoints(parentDataSet, "{}_demDiff".format(dataSet), region, gc_bb.minX, gc_bb.minY, gc_bb.minT.timestamp(), gridCellSize, os.path.join(tempdir, file), projection)
                
                    os.remove(os.path.join(tempdir,file) )
                

def dateFromFileName( file ):
    matchObj = re.findall(r'SIN_2__(\d+T\d+)', file)
    dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
    return dataTime
    
def isyearandmonth(file, year, month ):
    matchObj = re.findall(r'2__(\d+T\d+)', file)
    dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
    
    if dataTime.year == year and dataTime.month == month:
        return True
    else:
        return False
    
if __name__ == "__main__":
    
    args = sys.argv[1:]
    month = int(args[0])
    year = int(args[1])
    
    main(month, year)
