# -*- coding: utf-8 -*-

import sys
from os import listdir
from datetime import datetime
import re
import pandas as pd
import MalardClient.AsyncDataSetQuery as aq
import MalardClient.MalardClient as mc



def main(argv):
    # My code here
    # Get the arguments from the command-line except the filename
    parentDataSet = 'cryotempo'
    dataSet = 'swath_c_nw_mask'
    region = 'greenland'
    swathdir = '/media/earthwave/MalardExt/Malard/swath/greenland/GrIS_NW_bC'
    year = int(argv[0])
    #month = int(argv[1])

    columnFilters = [{'column':'coh','op':'gte','threshold':0.3},{'column':'powerScaled','op':'gte','threshold':100.0}]
    includeColumns =['lon', 'lat', 'elev', 'heading', 'demDiff', 'demDiffMad', 'demDiffMad2','phaseAmb', 'meanDiffSpread', 'wf_number', 'sampleNb', 'powerScaled','powerdB', 'phase', 'phaseS', 'phaseSSegment', 'phaseConfidence', 'coh']
    gridCellSize = 100000
    environmentName = 'DEV_EXT'
    
    years = [year]
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    
    ice_file = "/data/puma1/scratch/cryotempo/masks/icesheets.shp"
    maskFilterIce = mc.MaskFilter( p_shapeFile=ice_file)
    maskFilterLRM = mc.MaskFilter( p_shapeFile="/data/puma1/scratch/cryotempo/sarinmasks/LRM_Greenland.shp" , p_includeWithin=False )
    maskFilters = [maskFilterIce, maskFilterLRM]
    
    for year in years:
        for month in months:
            swathfiles = [(f,dateFromFileName(f)) for f in listdir(swathdir) if isyearandmonth( f, year, month )]
    
            print('Processing Year Month %d-%d. Num Swaths %d' % (year,month,len(swathfiles) ))
    
            if len(swathfiles) > 0:
                publishData(environmentName, swathfiles, parentDataSet, dataSet, region, swathdir, columnFilters, includeColumns,gridCellSize, maskFilters )

def dateFromFileName( file ):
    matchObj = re.findall(r'2S_(\d+T\d+)', file)
    dataTime = datetime.strptime(matchObj[0], '%Y%m%dT%H%M%S')
    return dataTime
        
def publishData(environmentName, swathfiles, parentDataSet, dataSet, region, swathdir, columnFilters, includeColumns, gridCellSize, regionMask ):
    
    query = aq.AsyncDataSetQuery( 'ws://localhost:9000',environmentName,False)
    i = 0 
    results = []    
    for file,dataTime in swathfiles:
        i = i + 1
        print("Processing {} Count {}".format(file,i) )
        result = query.publishSwathToGridCells( parentDataSet, dataSet, region, file, swathdir, dataTime, columnFilters, includeColumns, gridCellSize, maskFilters = regionMask )
        print(result.message)
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
    main(sys.argv[1:])
