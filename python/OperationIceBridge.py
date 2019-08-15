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
from os import listdir
import AsyncDataSetQuery
import DataSetLoader

def main(argv):

    argv = sys.argv[1:]
    
    filePath = '/data/slug1/store/rawdata/oib/' + argv[0] +'/'
    print(filePath)
    environmentName = 'DEVv2'
  
    parentDataSet = 'cryotempo'
    dataSet = 'oib'
    gridCellSize = 100000
    
    swathfiles = [f for f in listdir(filePath)]
    
    greenland = []
    antarctic = []
    
    for file in swathfiles:
        matchObj = re.findall(r'ILATM2_(\d+_\d+)_', file)
        dataTime = datetime.strptime(matchObj[0], '%Y%m%d_%H%M%S')
    
        df = pd.read_csv(filePath+file, skiprows=9)
    
        df = df.rename(columns=lambda x: x.strip())    
        df['lat'] =df['Latitude(deg)']
        df['lon'] =df['Longitude(deg)']
        
        
        tempfilepath = '/data/puma1/scratch/v2/malard/tempnetcdfs/'
        tempfilename = file.replace('.csv','.nc')
        
        if float(df['lat'][0:1]) >= 52.0:
            greenland.append((tempfilename,dataTime))
        elif float(df['lat'][0:1]) < -60.0:
            antarctic.append((tempfilename,dataTime))
        else:
            println('File %d will not be processed. Unexpected location.'%(tempfilename))
            
        ds = netCDF4.Dataset(tempfilepath + tempfilename,'w',format='NETCDF4')
        ds.createDimension( 'row', None )
    
        for column, dtype in zip(df.columns, df.dtypes):
            columnStr = column.replace('# ','',)                    
            col = ds.createVariable(columnStr, dtype, ('row',))
            col[:] = np.array(df[column])
    
        ds.close()
 
    print('Found Greenland files[%d] Antarctic Files [%d]' %(len(greenland), len(antarctic)))

    if len(greenland) > 0:
        DataSetLoader.publishData(environmentName, greenland, parentDataSet, dataSet, 'greenland', tempfilepath, {}, [], gridCellSize )
        
    if len(antarctic) > 0:    
        DataSetLoader.publishData(environmentName, antarctic, parentDataSet, dataSet, 'antarctic', tempfilepath, {}, [], gridCellSize )

if __name__ == "__main__":
    main(sys.argv)