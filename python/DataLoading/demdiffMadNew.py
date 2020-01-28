#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 28 11:29:59 2019

@author: jon
"""

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

from os import listdir
import DataSetLoader as d


import MalardClient.MalardHelpers as h

def main(year):

    filePath = '/media/earthwave/MalardExt/Malard/swath/himalayas/HMA_v3'
    environmentName = 'DEV_EXT'
  
    region = 'himalayas'
    parentDataSet = 'mtngla'
    dataSet = 'tdx_mad_v3'
    gridCellSize = 100000
    
    minCoh = 0.6
    minPower = 10000.0
    
    years = [year]
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    
    tempfilepath = '/data/puma1/scratch/v2/malard/tempnetcdfs/'
    
    for year in years:
        for month in months:
            swathfiles = [f for f in listdir(filePath) if d.isyearandmonth( f, year, month )]
    
            print('Processing Year Month %d-%d. Num Swaths %d' % (year,month,len(swathfiles) ))

            tempFiles = []
            for file in swathfiles:
                print("Starting processing {}".format(file)) 
                matchObj = re.findall(r'2S_(\d+T\d+)', file)
                dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
        
                df = h.getDataFrameFromNetCDF( "{}/{}".format(filePath, file)  )
                
                dfFilteredCoh = df[df["coh"] >= minCoh]
                dfFilteredPower = dfFilteredCoh[dfFilteredCoh["powerScaled"] >= minPower]
                
                df = pd.DataFrame(dfFilteredPower)
                df['demDiffMadNew'] = df['demDiff'].groupby(df['wf_number']).transform('mad')
                            
                tempFiles.append((file, dataTime))
                
                ds = netCDF4.Dataset(tempfilepath + file,'w',format='NETCDF4')
                ds.createDimension( 'row', None )
                
                for column, dtype in zip(df.columns, df.dtypes):
                    columnStr = column.replace('# ','',)                    
                    col = ds.createVariable(columnStr, dtype, ('row',))
                    col[:] = np.array(df[column])
                
                ds.close()
         
            print('Found files[%d]' %(len(tempFiles)))
            
            if len(tempFiles) > 0:
                d.publishData(environmentName, tempFiles, parentDataSet, dataSet, region, tempfilepath, [], [], gridCellSize, regionMask=[] )
                
if __name__ == "__main__":
    argv = sys.argv[1:]
    year = int( argv[0] )
    
    main(year)