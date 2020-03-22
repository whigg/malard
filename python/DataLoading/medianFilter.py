<<<<<<< HEAD
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 16:22:16 2020

@author: jon
"""


import math
import numpy as np
import matplotlib.pyplot as plt
import os

import Timeseries as t

from osgeo import gdal

def getArray( file ):
    dem = gdal.Open(file, gdal.GA_ReadOnly)
    demArray = dem.GetRasterBand(1).ReadAsArray()
    return demArray



''' nanmedfilt2 computes the median across a sliding window

    Args: 
        a: data matrix to filter
        m: width of sliding window
        n: height of sliding window
    Returns:
        b: matrix of medians for windows that dont contain all nans
    Raises:
        ValueError: m and n need to be odd.
'''
def nanmedfilt2(a,m,n):
    
    if m%2 == 0 or n%2 == 0:
        raise ValueError( "Both m and n must be odd integers." )
    
    b = np.empty(a.shape)
    b.fill(np.nan)
    
    coverage = np.zeros(a.shape)
    
    NRows, NCols = a.shape
    
    mhalf = math.floor(m/2)
    nhalf = math.floor(n/2)
    
    rows = range(mhalf, NRows - mhalf )
    cols = range(nhalf, NCols - nhalf )
    
    for i in rows:
        for j in cols:
            tmp = np.empty((m,n))
            tmp[:] = a[i-mhalf:i+mhalf+1, j-mhalf:j+mhalf+1]
            
            if not np.all(np.isnan(tmp)):
                b[i,j] = np.nanmedian(tmp)
                #check the corners all have data
                if( not np.isnan(tmp[0,0]) and not np.isnan(tmp[0,mhalf]) and not np.isnan(tmp[mhalf,mhalf]) and not np.isnan(tmp[mhalf,0])):
                    coverage[i,j] = 1
            else:
                b[i,j] = np.nan
    return (b,coverage)

def medClean(mat, demArray, m, n , iterations):
    
    rows, cols = mat.shape
    
    
    for i in range(rows):
        for j in range(cols):
            mat[i,j] = np.nan if mat[i,j] == -32768 else mat[i,j]
    
    outMat = np.empty(mat.shape)
    outMat.fill(np.nan)
    
    adjustment = np.empty(mat.shape)
    adjustment.fill( np.nan )
    
    for it in range(iterations):
        
        matF, coverage = nanmedfilt2( mat, m, n )
        
        print( np.nanmax(mat) )
        print( np.nanmin(mat) )
        
        print( np.nanmax(matF) )
        print( np.nanmin(matF) )
        diff = matF - mat
        
        mad = np.nanstd( diff )
        print(mad)
        
        absDiff = np.abs( diff )
        print(np.nanmedian(absDiff))
        
        
        for i in range(rows):
            for j in range(cols):
                if absDiff[i,j] < 3*mad and not np.isnan(absDiff[i,j]): 
                    outMat[i,j] = mat[i,j]
                elif coverage[i,j] == 1:
                    adjustment[i,j] = diff[i,j]
                    outMat[i,j] = matF[i,j]
                else:
                    outMat[i,j] = np.nan
        
        mat = outMat
    
    mask = np.zeros(outMat.shape)
    
    for i in range(rows):
        for j in range(cols):
            mask[i,j] = 1 if not np.isnan(outMat[i,j]) or not np.isnan(adjustment[i,j]) else 0
            
            if mask[i,j] == 1:
                if not np.isnan(adjustment[i,j]):
                    demArray[i,j] = demArray[i,j] + adjustment[i,j] 
            else:
                demArray[i,j] = -32768
    
    return (mask, adjustment)

def applyMedianFilter( demFilePath, diffFilePath, numIterations ):
    fullPath = diffFilePath

    array = getArray( fullPath )        
    demArray = getArray( demFilePath )
    
    mask, adjustment = medClean( array, demArray, 5,5, numIterations  )

    driver = gdal.GetDriverByName('GTiff')
    
    maskFileName = fullPath.replace("_diff","_medianmask_{}".format(numIterations))
    result = driver.CreateCopy(maskFileName, gdal.Open(fullPath))
    result.GetRasterBand(1).WriteArray(mask)
    result = None   
    
    demFileName = fullPath.replace(".tif","_masked.tif")
    result = driver.CreateCopy(demFileName, gdal.Open(fullPath))
    result.GetRasterBand(1).WriteArray(demArray)
    result = None
    
    return demFileName

if __name__ == "__main__":
    
    
    arcticDem = '/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/combined/GrIS_dem_2km.tif'
    filePath =  "/data/eagle/team/shared/Work/CryoTEMPO/PM5/DemDiffMadRatio/GrIS_NWFW115_pocaDemDiff100_Unc_7_MinP_1_MaxPix_8_DemDiffMad_6_Resolution_2000"
    diffFile = "greenland_2011_2_diff.tif"
    dem = "greenland_2011_2.tif"
    numIterations = 4
    
    demFilePath = os.path.join( filePath, dem )
    diffFilePath = os.path.join(filePath, diffFile )
    
    demFileName = applyMedianFilter(demFilePath, diffFilePath, numIterations)
    ## add the code to do the diff
    fig, ax = plt.subplots(1,2, figsize=(6,12))
    t.nicePlot(demFileName,fig,ax[0],"Feb2011 MaskedDem","Elevation (m)",nodata=-32768)
    t.nicePlot(demFilePath,fig,ax[1],"Feb2011 Dem","Elevation (m)",nodata=-32768)
    
    plot = demFileName.replace(".tif",".png")
    print("About to save figure {}".format(plot) )
    fig.savefig( plot )
    
=======
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 16:22:16 2020

@author: jon
"""


import math
import numpy as np
import matplotlib.pyplot as plt
import os

import Timeseries as t

from osgeo import gdal

def getArray( file ):
    dem = gdal.Open(file, gdal.GA_ReadOnly)
    demArray = dem.GetRasterBand(1).ReadAsArray()
    return demArray



''' nanmedfilt2 computes the median across a sliding window

    Args: 
        a: data matrix to filter
        m: width of sliding window
        n: height of sliding window
    Returns:
        b: matrix of medians for windows that dont contain all nans
    Raises:
        ValueError: m and n need to be odd.
'''
def nanmedfilt2(a,m,n):
    
    if m%2 == 0 or n%2 == 0:
        raise ValueError( "Both m and n must be odd integers." )
    
    b = np.empty(a.shape)
    b.fill(np.nan)
    
    coverage = np.zeros(a.shape)
    
    NRows, NCols = a.shape
    
    mhalf = math.floor(m/2)
    nhalf = math.floor(n/2)
    
    rows = range(mhalf, NRows - mhalf )
    cols = range(nhalf, NCols - nhalf )
    
    for i in rows:
        for j in cols:
            tmp = np.empty((m,n))
            tmp[:] = a[i-mhalf:i+mhalf+1, j-mhalf:j+mhalf+1]
            
            if not np.all(np.isnan(tmp)):
                b[i,j] = np.nanmedian(tmp)
                #check the corners all have data
                if( not np.isnan(tmp[0,0]) and not np.isnan(tmp[0,mhalf]) and not np.isnan(tmp[mhalf,mhalf]) and not np.isnan(tmp[mhalf,0])):
                    coverage[i,j] = 1
            else:
                b[i,j] = np.nan
    return (b,coverage)

def medClean(mat, demArray, m, n , iterations):
    
    rows, cols = mat.shape
    
    
    for i in range(rows):
        for j in range(cols):
            mat[i,j] = np.nan if mat[i,j] == -32768 else mat[i,j]
    
    outMat = np.empty(mat.shape)
    outMat.fill(np.nan)
    
    adjustment = np.empty(mat.shape)
    adjustment.fill( np.nan )
    
    for it in range(iterations):
        
        matF, coverage = nanmedfilt2( mat, m, n )
        
        print( np.nanmax(mat) )
        print( np.nanmin(mat) )
        
        print( np.nanmax(matF) )
        print( np.nanmin(matF) )
        diff = matF - mat
        
        mad = np.nanstd( diff )
        print(mad)
        
        absDiff = np.abs( diff )
        print(np.nanmedian(absDiff))
        
        
        for i in range(rows):
            for j in range(cols):
                if absDiff[i,j] < 3*mad and not np.isnan(absDiff[i,j]): 
                    outMat[i,j] = mat[i,j]
                elif coverage[i,j] == 1:
                    adjustment[i,j] = diff[i,j]
                    outMat[i,j] = matF[i,j]
                else:
                    outMat[i,j] = np.nan
        
        mat = outMat
    
    mask = np.zeros(outMat.shape)
    
    for i in range(rows):
        for j in range(cols):
            mask[i,j] = 1 if not np.isnan(outMat[i,j]) or not np.isnan(adjustment[i,j]) else 0
            
            if mask[i,j] == 1:
                if not np.isnan(adjustment[i,j]):
                    demArray[i,j] = demArray[i,j] + adjustment[i,j] 
            else:
                demArray[i,j] = -32768
    
    return (mask, adjustment)

def applyMedianFilter( demFilePath, diffFilePath, numIterations ):
    fullPath = diffFilePath

    array = getArray( fullPath )        
    demArray = getArray( demFilePath )
    
    mask, adjustment = medClean( array, demArray, 5,5, numIterations  )

    driver = gdal.GetDriverByName('GTiff')
    
    maskFileName = fullPath.replace("_diff","_medianmask_{}".format(numIterations))
    result = driver.CreateCopy(maskFileName, gdal.Open(fullPath))
    result.GetRasterBand(1).WriteArray(mask)
    result = None   
    
    demFileName = demFilePath.replace(".tif","_masked_dem.tif")
    result = driver.CreateCopy(demFileName, gdal.Open(fullPath))
    result.GetRasterBand(1).WriteArray(demArray)
    result = None
    
    return demFileName

if __name__ == "__main__":
    
    
    arcticDem = '/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/combined/GrIS_dem_2km.tif'
    filePath =  "/data/eagle/team/shared/Work/CryoTEMPO/PM5/DemDiffMadRatio/GrIS_NWFW115_pocaDemDiff100_Unc_7_MinP_1_MaxPix_8_DemDiffMad_6_Resolution_2000"
    diffFile = "greenland_2011_2_diff.tif"
    dem = "greenland_2011_2.tif"
    numIterations = 4
    
    demFilePath = os.path.join( filePath, dem )
    diffFilePath = os.path.join(filePath, diffFile )
    
    demFileName = applyMedianFilter(demFilePath, diffFilePath, numIterations)
    ## add the code to do the diff
    fig, ax = plt.subplots(1,2, figsize=(12,20))
    t.nicePlot(demFileName,fig,ax[0],"Feb2011 MaskedDem","Elevation (m)",nodata=-32768)
    t.nicePlot(demFilePath,fig,ax[1],"Feb2011 Dem","Elevation (m)",nodata=-32768)
    
    plot = demFileName.replace(".tif",".png")
    print("About to save figure {}".format(plot) )
    fig.savefig( plot )
    
>>>>>>> e7b63a4d1efa78f63850d44b0c077fad44904196
     