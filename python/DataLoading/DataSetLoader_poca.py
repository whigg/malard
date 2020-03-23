"""Loads poca data into Malard from the swath files 

Usage:
    python3 DataSetLoader_poca.py <year>    
"""

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

import ProcessingRequest as pr

"""Takes a list of swath files and column mappings, e.g. pocaH -> elev and creates a NetCDF with the mapped column names.

Args:
    swaths: a list of file name date pairs
    src_dir: path to read the files from
    tempDir: directory that the temporary files will be written to
    columnMappings: a dictionary of column name mappings to the standard malard names, eg. pocaLat to lat, pocaLon to lon
    region: used to check whether the swath covers the input region

Returns:
    List of file name date pairs.    


"""
def createTempFiles( swaths, src_dir, tempDir, columnMappings, region ):
    
    output_swaths = []
    
    for s,dt in swaths:
        print("Creating temp poca file {}".format(s))
        nc = netCDF4.Dataset(os.path.join(src_dir, s))
        data = {}
        
        if "lat" not in nc.variables:
            print( "Skipping file because lat column is missing {}".format(s) )
            continue
        
        lats = nc.variables["lat"]
        
        if region == "greenland":
            if np.max(lats) < 52:
                print( "Skipping file. Not in greenland {}".format(s))
                continue
        elif region == "antarctic":
            if np.max(lats) > -50:
                print( "Skipping file. Not in antarctic {}".format(s))
                continue
            
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
            
        ds.close()
        output_swaths.append( (s, dt ) )
    
    return output_swaths
        
        
def cleanUpTempFiles( swaths, tempDir ):
    for s,d in swaths:
        os.remove( os.path.join(tempDir, s) )
        
def main(month, year, loaderConfig):
    
    parentDataSet = loaderConfig["parentDataSet"]
    dataSet = loaderConfig["dataSetPoca"]
    region = loaderConfig["region"]
    swathdir = loaderConfig["swathDir"]
    environmentName = loaderConfig["MalardEnvironment"]

    tempdir = '/data/puma1/scratch/v2/malard/tempnetcdfs'
    #year = int(argv[0])
    #month = int(argv[1])
    columnFilters = []#[{'column':'coh','op':'gte','threshold':0.3},{'column':'power','op':'gte','threshold':100.0}]
    pocaColumns = {'pocaH' : 'elev', 'pocaLat' : 'lat','pocaLon' : 'lon', 'pocaWf' : 'wf_number' }
    
    includeColumns = []
    
    ice_file = "/data/puma/scratch/cryotempo/masks/greenland/icesheets.shp" if region == "greenland" else "/data/puma1/scratch/cryotempo/sarinmasks/AntarcticaReprojected.shp"
    maskFilterIce = mc.MaskFilter( p_shapeFile=ice_file)
    lrm_file = "/data/puma/scratch/cryotempo/masks/greenland/LRM_Greenland.shp" if region == "greenland" else "/data/puma1/scratch/cryotempo/sarinmasks/LRM_Antarctica.shp"
    maskFilterLRM = mc.MaskFilter( p_shapeFile= lrm_file, p_includeWithin=False )
    maskFilters = [maskFilterIce, maskFilterLRM]
    
    gridCellSize = 100000
    
    years = [year]
    months = [month]
    
    for year in years:
        log_file = open( "log/{}_{}_{}_{}_swath_loading_log.csv".format(year, parentDataSet, dataSet, region),"w" )
        for month in months:
            swathfiles = [(f,dateFromFileName(f)) for f in listdir(swathdir) if  f.endswith(".nc") and isyearandmonth( f, year, month )]
            
            filtered_swaths = createTempFiles( swathfiles, swathdir, tempdir, pocaColumns, region )
            
            message = 'Processing Year Month %d-%d. Num Swaths %d' % (year,month,len(filtered_swaths) )
            print(message)
            log_file.write( message + "\n")
            if len(filtered_swaths) > 0:
                publishData(log_file, environmentName, filtered_swaths, parentDataSet, dataSet, region, tempdir, columnFilters, includeColumns, gridCellSize, maskFilters )
            
            cleanUpTempFiles( filtered_swaths, tempdir )

def dateFromFileName( file ):
    matchObj = re.findall(r'2S_(\d+T\d+)', file)
    dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
    return dataTime
        
def publishData(log_file, environmentName, swathfiles, parentDataSet, dataSet, region, swathdir, columnFilters, includeColumns, gridCellSize, regionMask = None, xCol='lon', yCol='lat' ):
    
    query = aq.AsyncDataSetQuery( 'ws://localhost:9000',environmentName,False)
    i = 0 
    results = []    
    for file,dataTime in swathfiles:
        i = i + 1
        print("Processing {} Count {}".format(file,i) )
        result = query.publishSwathToGridCells( parentDataSet, dataSet, region, file, swathdir, dataTime, columnFilters, includeColumns, gridCellSize, xCol, yCol, maskFilters = regionMask )
        if result.status == 'Success':
            log_file.write("Processed file, {}, Succcessfully\n".format(file))
            results.append(result.swathDetails)
        else:
            message = 'File, %s ,Result Status ,%s, Message ,%s\n' % ( file, result.status, result.message ) 
            print(message)
            log_file.write(message)
        
    data = []    
    for result in results:
        swathDetails = result
        gridCells = pd.DataFrame(swathDetails['gridCells'])
        gridCells['swathName'] = swathDetails['swathName']
        gridCells['swathId'] = swathDetails['swathId']
        gridCells['swathPointCount'] = swathDetails['swathPointCount']
        gridCells['filteredSwathPointCount'] = swathDetails['filteredSwathPointCount']
        data.append(gridCells)

    df = pd.concat(data, ignore_index=True)

    groupBy = df.groupby(['x','y','t','projection'])

    for k,v in groupBy:
        x,y,t,projection = k
        files = list(v['fileName'])
        result = query.publishGridCellPoints( parentDataSet, dataSet, region, x, y, t, gridCellSize, files, projection)
        print('Result status %s' %(result.message))
        query.releaseCache(files)
    
def isyearandmonth(file, year, month ):
    print(file)
    matchObj = re.findall(r'2S_(\d+T\d+)', file)
    dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
    
    if dataTime.year == year and dataTime.month == month:
        return True
    else:
        return False

    
def isyear( file, year ):
    matchObj = re.findall(r'2S_(\d+T\d+)', file)
    dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
    
    if dataTime.year == year:
        return True
    else:
        return False

def ismonth( file, month ):
    matchObj = re.findall(r'2S_(\d+T\d+)', file)
    dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
    
    if dataTime.month == month:
        return True
    else:
        return False

if __name__ == "__main__":
    args = sys.argv[1:]
    
    month = args[0]
    year = args[1]
    configPath = args[2]
        
    processingRequest = pr.ProcessingRequest.fetchRequest(configPath)
    loadConfig = processingRequest.getConfig
    
    print("Running for month=[{month}] and year=[{year}]".format(month=month, year=year))
    
    main(int(month),int(year), loadConfig)
    
