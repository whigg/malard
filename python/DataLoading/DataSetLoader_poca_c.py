#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 07:52:29 2020

@author: jon
"""

import MalardClient.MalardClient as mc
import DataSetLoader as d
import re

from os import listdir
from datetime import datetime
import sys

def main(month, year):
    # My code here
    # Get the arguments from the command-line except the filename
    parentDataSet = 'test'
    dataSet = 'ThwaitesBergPOCA'
    region = 'antarctic'
    swathdir = '/data/snail1/scratch/rawdata/ThwaitesBergPOCA'
    
    columnFilters = []# [{'column':'coh','op':'gte','threshold':0.3},{'column':'powerScaled','op':'gte','threshold':100.0}]
    includeColumns = []# ['lon', 'lat', 'elev', 'heading', 'demDiff', 'demDiffMad', 'demDiffMad2','phaseAmb', 'meanDiffSpread', 'wf_number', 'sampleNb', 'powerScaled','powerdB', 'phase', 'phaseS', 'phaseSSegment', 'phaseConfidence', 'coh']
    gridCellSize = 100000
    environmentName = 'DEVv2'
    
    years = [year]
    months = [month]
    
    ice_file = "/data/puma/scratch/cryotempo/masks/greenland/icesheets.shp" if region == "greenland" else "/data/puma1/scratch/cryotempo/sarinmasks/AntarcticaReprojected.shp"
    maskFilterIce = mc.MaskFilter( p_shapeFile=ice_file)
    lrm_file = "/data/puma/scratch/cryotempo/masks/greenland/LRM_Greenland.shp" if region == "greenland" else "/data/puma1/scratch/cryotempo/sarinmasks/LRM_Antarctica.shp"
    maskFilterLRM = mc.MaskFilter( p_shapeFile= lrm_file, p_includeWithin=False )
    maskFilters = [maskFilterIce, maskFilterLRM]
    
    
    for year in years:
        for month in months:
            swathfiles = [(f,dateFromFileName(f)) for f in listdir(swathdir) if  f.endswith(".nc") and isyearandmonth( f, year, month ) ]
    
            print('Processing Year Month %d-%d. Num Swaths %d' % (year,month,len(swathfiles) ))
    
            if len(swathfiles) > 0:
                d.publishData(environmentName, swathfiles, parentDataSet, dataSet, region, swathdir, columnFilters, includeColumns,gridCellSize, maskFilters )

def dateFromFileName( file ):
    print(file)
    matchObj = re.findall(r'2P_(\d+T\d+)', file)
    dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
    return dataTime

def isyearandmonth(file, year, month ):
    
    dataTime = dateFromFileName(file)    
    
    if dataTime.year == year and dataTime.month == month:
        return True
    else:
        return False

if __name__ == "__main__":
    args = sys.argv[1:]
    
    month = int(args[0])
    year = int(args[1])
    
    main( month, year )
