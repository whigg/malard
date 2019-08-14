# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 18:56:00 2019

@author: s_j_a
"""

import sys
import re
from datetime import datetime

import pandas as pd
import numpy as np
import netCDF4
import json

import AsyncDataSetQuery

def main(argv):

    file =  'ILATM2_20160419_132625_smooth_nadir3seg_50pt.csv'
    filePath = 'C:\\Users\\s_j_a\\Dropbox\\Earthwave\\jon\\'
    environmentName = 'JALOCALv3'
  
    parentDataSet = 'cryotempo'
    dataSet = 'oib'
    region = 'greenland'
    gridCellSize = 100000
    
    matchObj = re.findall(r'ILATM2_(\d+_\d+)_', file)
    dataTime = datetime.strptime(matchObj[0], '%Y%m%d_%H%M%S')
    
    df = pd.read_csv(filePath+file, skiprows=9)
    
    df = df.rename(columns=lambda x: x.strip())    
    df['lat'] =df['Latitude(deg)']
    df['lon'] =df['Longitude(deg)']
    
    tempfilepath = 'C:\\Earthwave\\malard\\data\\'
    tempfilename = 'icebridge12.nc'
    
    ds = netCDF4.Dataset(tempfilepath + tempfilename,'w',format='NETCDF4')
    ds.createDimension( 'row', None )
    
    for column, dtype in zip(df.columns, df.dtypes):
        columnStr = column.replace('# ','',)                    
        col = ds.createVariable(columnStr, dtype, ('row',))
        col[:] = np.array(df[column])
    
    ds.close()
    
    query = AsyncDataSetQuery.AsyncDataSetQuery( 'ws://localhost:9000',environmentName,True)
    
    results = query.publishSwathToGridCells( parentDataSet, dataSet, region, tempfilename, tempfilepath, dataTime, {}, [], 100000 ) 

    if results.status == 'Success':     
        data = []
        swathDetails = results.swathDetails
        gridCells = pd.DataFrame(swathDetails['gridCells'])
        gridCells['swathName'] = swathDetails['swathName']
        gridCells['swathId'] = swathDetails['swathId']
        gridCells['swathPointCount'] = swathDetails['swathPointCount']
        gridCells['filteredSwathPointCount'] = swathDetails['filteredSwathPointCount']
        data.append(gridCells)

        griddf = pd.concat(data, ignore_index=True)

        groupBy = griddf.groupby(['x','y','t','projection'])

        for k,v in groupBy:
            x,y,t,projection = k
            files = list(v['fileName'])
            results = query.publishGridCellPoints( parentDataSet, dataSet, region, x, y, t, gridCellSize, files, projection)
            print(results.message)
            query.releaseCache(files)
    else:
        print( 'Swath File [%s] failed to load with status [%s] and message [%s]' % (tempfilename, results.status, results.message ) )
        

if __name__ == "__main__":
    main(sys.argv)