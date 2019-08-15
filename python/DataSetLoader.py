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
    
    print('Args {} are these.'.format(argv[0]))
    
    parentDataSet = 'mtngla'
    dataSet = 'tdx3'
    region = 'himalayas'
    swathdir = 'C:\\Earthwave\\MountainGlaciers\\f2010\\'
    year = int(argv[0])
    columnFilters = {'coh':0.3,'power':100.0}
    includeColumns =[]
    gridCellSize = 100000
    environmentName = 'JALOCALv3'

    swathfiles = [f for f in listdir(swathdir) if isyear( f, year )]
    
    print(len(swathfiles))
    
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    
    for month in months:
        filesByMonth = [f for f in swathfiles if ismonth( f, month )]
        print('Processing Year Month %d-%d. Num Swaths %d' % (year,month,len(filesByMonth) ))
        if len(filesByMonth) > 0: 
            publishData(environmentName, filesByMonth, parentDataSet, dataSet, region, swathdir, columnFilters, includeColumns, gridCellSize )
           

def publishData(environmentName, swathfiles, parentDataSet, dataSet, region, swathdir, columnFilters, includeColumns, gridCellSize ):
    
    query = AsyncDataSetQuery.AsyncDataSetQuery( 'ws://localhost:9000',environmentName,False)
     
    results = []    
    for file,dataTime in swathfiles:
        print(file)
        #matchObj = re.findall(r'2S_(\d+T\d+)', file)
        #dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
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