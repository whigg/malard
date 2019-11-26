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
import h5py
from os import listdir
import DataSetLoader

def main(argv):

    #argv = sys.argv[1:]
    
    filePath = '/data/slug1/holding/ICESat'
    print(filePath)
    environmentName = 'DEVv2'
  
    parentDataSet = 'cryotempo'
    dataSet = 'icesat2'
    gridCellSize = 100000
    
    columnFilters = [{'column':'elev','op':'lt','threshold':100000.0}]
    
    swathfiles = [f for f in listdir(filePath) if f.endswith("h5")]
    
    greenland = []
    antarctic = []
    
    for file in swathfiles:
        print("Starting processing {}".format(file)) 
        matchObj = re.findall(r'ATL06_(\d+)', file)
        dataTime = datetime.strptime(matchObj[0], '%Y%m%d%H%M%S')

        hf = h5py.File("{}/{}".format(filePath, file), 'r')        
        
        tracks = [  t for t in hf.keys() if t.startswith("gt")  ]

        for t in tracks:
            df = pd.DataFrame()
            df['elev'] = hf[t]['land_ice_segments']['h_li']
            df['uncertainty'] =  hf[t]['land_ice_segments']['h_li_sigma']
            df['lat'] = hf[t]['land_ice_segments']['latitude']
            df['lon'] = hf[t]['land_ice_segments']['longitude']
            tracks = np.array(range(0,len(df)),"S4")
            tracks.fill(t)
            df['tracks'] = tracks
        
            tempfilepath = '/data/puma1/scratch/v2/malard/tempnetcdfs/'
            tempfilename = file.replace('.h5','{}.nc'.format(t))
            
            if float(df['lat'].min()) >= 52.0:
                greenland.append((tempfilename,dataTime))
            elif float(df['lat'][0:1]) < -60.0:
                antarctic.append((tempfilename,dataTime))
            else:
                print('File {} will not be processed. Unexpected location.'.format(tempfilename))
                
            ds = netCDF4.Dataset(tempfilepath + tempfilename,'w',format='NETCDF4')
            ds.createDimension( 'row', None )
        
            for column, dtype in zip(df.columns, df.dtypes):
                columnStr = column.replace('# ','',)                    
                col = ds.createVariable(columnStr, dtype, ('row',))
                col[:] = np.array(df[column])
        
            ds.close()
 
    print('Found Greenland files[%d] Antarctic Files [%d]' %(len(greenland), len(antarctic)))

    if len(greenland) > 0:
        DataSetLoader.publishData(environmentName, greenland, parentDataSet, dataSet, 'greenland', tempfilepath, columnFilters, [], gridCellSize )
        
    if len(antarctic) > 0:    
        DataSetLoader.publishData(environmentName, antarctic, parentDataSet, dataSet, 'antarctic', tempfilepath, columnFilters, [], gridCellSize )

if __name__ == "__main__":
    main(sys.argv)