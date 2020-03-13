#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 16:22:16 2020

@author: jon
"""


import math
import numpy as np
import os

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
            else:
                b[i,j] = np.nan
    return b

def medClean(mat, m, n , iterations):
    
    rows, cols = mat.shape
    
    for i in range(rows):
        for j in range(cols):
            mat[i,j] = np.nan if mat[i,j] == -32768 else mat[i,j]
    
    outMatrices = []
    for it in range(iterations):
        
        matF = nanmedfilt2( mat, m, n )
        
        
        print( np.nanmax(mat) )
        print( np.nanmin(mat) )
        
        print( np.nanmax(matF) )
        print( np.nanmin(matF) )
        diff = mat - matF
        
        mad = np.nanstd( diff )
        print(mad)
        
        absDiff = np.abs( diff )
        print(np.nanmedian(absDiff))
        
        outMat = np.empty(mat.shape)
        outMat.fill(np.nan)
        for i in range(rows):
            for j in range(cols):
                outMat[i,j] = mat[i,j] if absDiff[i,j] < 3*mad and not np.isnan(absDiff[i,j]) else np.nan
        
        outMatrices.append( outMat )
        print(type(outMat))
        mat = outMat
    
    mask = np.zeros(outMat.shape)
    
    for i in range(rows):
        for j in range(cols):
            outMat[i,j] = -32768 if np.isnan(outMat[i,j])  else outMat[i,j]
            mask[i,j] = 1 if outMat[i,j] > -32768 else 0
    
    return (outMat, mask)

def applyMedianFilter( diffFilePath, numIterations ):
    fullPath = diffFilePath

    array = getArray( fullPath )        
    
    outMat, mask = medClean( array, 5,5, numIterations  )

    driver = gdal.GetDriverByName('GTiff')
    diffMaskFileName = fullPath.replace("_diff","_diff_medianmask_{}".format(numIterations))
    result = driver.CreateCopy(diffMaskFileName, gdal.Open(fullPath))
    result.GetRasterBand(1).WriteArray(outMat)
    result = None
    
    maskFileName = fullPath.replace("_diff","_medianmask_{}".format(numIterations))
    result = driver.CreateCopy(maskFileName, gdal.Open(fullPath))
    result.GetRasterBand(1).WriteArray(mask)
    result = None
    
    return maskFileName

if __name__ == "__main__":
    
    filePath =  "/data/puma/scratch/cryotempo/processeddata/greenland_nw_adjust/GrIS_NWFW115_pocaDemDiff100_Unc_7_MinP_1_MaxPix_8_DemDiffMad_6_Resolution_2000"
    diffFile = "greenland_2011_2_diff.tif"
    dem = "greenland_2011_2.tif"
    numIterations = 4
    
    applyMedianFilter(filePath, dem, diffFile, numIterations)

