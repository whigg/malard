#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 08:24:27 2019

@author: jon
"""

import pandas as pd
import netCDF4
import numpy as np
import sys
import re

from os import listdir
from datetime import datetime

import DataSetLoader


def main(argv):

    filePath = '/data/slug1/store/projects/cryomlt/data/poca'
    #filePath = "/data/mouse1/team/jon/poca"
    environmentName = 'DEVv2'
      
    parentDataSet = 'cryotempo'
    dataSet = 'poca'
    gridCellSize = 100000
    
    columnFilters = [{'column':'elev','op':'lt','threshold':100000.0}]
    
    years = listdir(filePath)
    for year in years:    
        greenland = []
        antarctic = []
        
        stores = [(pd.HDFStore( "{}/{}/{}".format(filePath, year, f),mode='r',complevel=9, complib='blosc'),f) for f in listdir( "{}/{}".format(filePath, year) )]
        
        for s,f in stores:
            data = s["data"]
            
            d = pd.DataFrame()
            
            d['lon'] = data['lon']
            d['lat'] = data['lat']
            d['elev'] = data['elev']
            d['startTime'] = data['startTime']
            d['wf_number'] = data['wf_number']
            
            matchObj = re.findall(r'poca_(\d+-\d+)', f)
            dataTime = datetime.strptime(matchObj[0], '%Y%m%d-%H%M%S')
            
            tempfilepath = '/data/puma1/scratch/v2/malard/tempnetcdfs/'
            tempfilename = f.replace('.h5','.nc')
           
            if float(d['lat'].min()) >= 52.0:
                greenland.append((tempfilename,dataTime))
            elif float(d['lat'][0:1]) < -60.0:
                antarctic.append((tempfilename,dataTime))
            else:
                print('File {} will not be processed. Unexpected location.'.format(tempfilename))
                    
            ds = netCDF4.Dataset(tempfilepath + tempfilename,'w',format='NETCDF4')
            ds.createDimension( 'row', None )
            
            for column, dtype in zip(d.columns, d.dtypes):
                columnStr = column.replace('# ','',)                    
                col = ds.createVariable(columnStr, dtype, ('row',))
                col[:] = np.array(d[column])
            
            ds.close()
            s.close()
            
        if len(greenland) > 0:
            DataSetLoader.publishData(environmentName, greenland, parentDataSet, dataSet, 'greenland', tempfilepath, columnFilters, [], gridCellSize )
            
        if len(antarctic) > 0:    
            DataSetLoader.publishData(environmentName, antarctic, parentDataSet, dataSet, 'antarctic', tempfilepath, columnFilters, [], gridCellSize )

if __name__ == "__main__":
    main(sys.argv)    