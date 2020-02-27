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
        
def cleanUpTempFiles( swaths, tempDir ):
    for s,d in swaths:
        os.remove( os.path.join(tempDir, s) )
        
def main(argv):
    # My code here
    # Get the arguments from the command-line except the filename
    argv = sys.argv[1:]
    
    parentDataSet = 'cryotempo'
    dataSet = 'poca_d'
    region = 'greenland'
    swathdir = '/data/eagle/project-cryo-tempo/data/greenland/poca_d'
    tempdir = '/data/puma1/scratch/v2/malard/tempnetcdfs'
    year = int(argv[0])
    #month = int(argv[1])
    columnFilters = []#[{'column':'coh','op':'gte','threshold':0.3},{'column':'power','op':'gte','threshold':100.0}]
    pocaColumns = {'height_1_20_ku' : 'elev', 'lat_poca_20_ku' : 'lat','lon_poca_20_ku' : 'lon' }
    
    includeColumns = []
    
    
    gridCellSize = 100000
    environmentName = 'DEV_EXT'
    
    log_file = open( "/data/puma1/scratch/v2/malard/logs/{}_{}_{}_{}_swath_loading_log.csv".format(year, parentDataSet, dataSet, region),"w" )
    
    years = [year]
    months = [5]
    
    for year in years:
        for month in months:
            swathfiles = [(f,dateFromFileName(f)) for f in listdir(swathdir) if  f.endswith(".nc") and isyearandmonth( f, year, month )]
            
            createTempFiles( swathfiles, swathdir, tempdir, pocaColumns  )
            
            message = 'Processing Year Month %d-%d. Num Swaths %d' % (year,month,len(swathfiles) )
            print(message)
            log_file.write( message + "\n")
            if len(swathfiles) > 0:
                publishData(log_file, environmentName, swathfiles, parentDataSet, dataSet, region, tempdir, columnFilters, includeColumns, gridCellSize )
            
            cleanUpTempFiles( swathfiles, tempdir )

def dateFromFileName( file ):
    matchObj = re.findall(r'SIN_2__(\d+T\d+)', file)
    dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
    return dataTime
        
def publishData(log_file, environmentName, swathfiles, parentDataSet, dataSet, region, swathdir, columnFilters, includeColumns, gridCellSize, xCol='lon', yCol='lat' ):
    
    query = aq.AsyncDataSetQuery( 'ws://localhost:9000',environmentName,False)
    i = 0 
    results = []    
    for file,dataTime in swathfiles:
        i = i + 1
        print("Processing {} Count {}".format(file,i) )
        result = query.publishSwathToGridCells( parentDataSet, dataSet, region, file, swathdir, dataTime, columnFilters, includeColumns, gridCellSize, xCol, yCol )
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
    matchObj = re.findall(r'2__(\d+T\d+)', file)
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
    main(sys.argv)
