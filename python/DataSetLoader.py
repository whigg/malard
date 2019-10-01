# -*- coding: utf-8 -*-

import sys
from os import listdir
from os.path import isfile, join
from datetime import datetime
import re
import json
import pandas as pd
import AsyncDataSetQuery
import DataSetQuery


def main(argv):
    # My code here
    # Get the arguments from the command-line except the filename
    argv = sys.argv[1:]
    
    parentDataSet = 'cryotempo'
    dataSet = 'AIS_BaselineC_FilteredPC'
    region = 'antarctic'
    swathdir = '/data/slug1/store/rawdata/initialSwath/AntarcticForAnna/'
    year = int(argv[0])
    month = int(argv[1])
    columnFilters = {'coh':0.3,'power':100.0}
    includeColumns =[]
    gridCellSize = 100000
    environmentName = 'DEVv2'

    swathfiles = [(f,dateFromFileName(f)) for f in listdir(swathdir) if isyearandmonth( f, year, month )]
    
    print('Processing Year Month %d-%d. Num Swaths %d' % (year,month,len(swathfiles) ))
    
    if len(swathfiles) > 0:
        publishData(environmentName, swathfiles, parentDataSet, dataSet, region, swathdir, columnFilters, includeColumns, gridCellSize )
           
def dateFromFileName( file ):
    matchObj = re.findall(r'2S_(\d+T\d+)', file)
    dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
    return dataTime
        
def publishData(environmentName, swathfiles, parentDataSet, dataSet, region, swathdir, columnFilters, includeColumns, gridCellSize ):
    
    query = AsyncDataSetQuery.AsyncDataSetQuery( 'ws://localhost:9000',environmentName,False)
     
    results = []    
    for file,dataTime in swathfiles:
        print(file)
        result = query.publishSwathToGridCells( parentDataSet, dataSet, region, file, swathdir, dataTime, columnFilters, includeColumns, gridCellSize )
        if result.status == 'Success':
            results.append(result.swathDetails)
        else:
            print('File %s Result Status %s Message %s' % ( file, result.status, result.message ))
        
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
    main(sys.argv)