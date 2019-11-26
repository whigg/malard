#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 20:00:16 2019

@author: jon
"""

import cupy as cp
import numpy as np

def bucket( x , y, minX, maxX, minY, maxY, nrOfBucketsInRow ):
    xInterval = ( maxX - minX ) / nrOfBucketsInRow
    yInterval = ( maxY - minY ) / nrOfBucketsInRow
    
    xPos = int(( x - minX ) / xInterval) + 1
    yPos = int(( y - minY ) / yInterval)
    
    return min(yPos * nrOfBucketsInRow + xPos, nrOfBucketsInRow* nrOfBucketsInRow)

def bucket_s( x , y, minX, maxX, minY, maxY, nrOfBucketsInRow ):
    return 1

def bucketData( xArray, yArray, tArray, minX, maxX, minY, maxY, nrOfBucketsInRow ):    
    
    minXArray = cp.empty(len(xArray))
    minXArray.fill(minX)
    maxXArray = cp.empty(len(xArray))
    maxXArray.fill(maxX)

    minYArray = cp.empty(len(yArray))
    minYArray.fill(minY)
    maxYArray = cp.empty(len(yArray))
    maxYArray.fill(maxY)
    
    bucketsArray = cp.empty(len(xArray))
    bucketsArray.fill(nrOfBucketsInRow)

    bucketKernel = cp.ElementwiseKernel(
        'float64 x, float64 y, float64 minX, float64 maxX, float64 minY, float64 maxY, float64 numRows',
        'float64 bucketNumber',
        'bucketNumber = min(floor((y - minY)/((maxY - minY)/numRows))*numRows + floor((x - minX)/((maxX - minX)/numRows)) + 1, (numRows * numRows) )',
        'bucket'
        )

    bucketSrs = bucketKernel(cp.array(xArray),cp.array(yArray),minXArray,maxXArray, minYArray, maxYArray, bucketsArray  )
    bucketSrs = cp.asnumpy(bucketSrs)
    
    buckets = range(1,nrOfBucketsInRow * nrOfBucketsInRow +1)
    
    bucketData = {}
    for b in buckets:
        bucketData[b] = ([],[],[])
    
    for i in range(0,len(xArray)):
        b = bucketSrs[i]
        x = xArray[i]
        y = yArray[i]
        t = tArray[i]
        
        dataX, dataY, dataT = bucketData[b]
        
        dataX.append(x)
        dataY.append(y)
        dataT.append(t)
        
        bucketData[b] = (dataX,dataY,dataT)
    
    for b,(x,y,t) in bucketData.items():
        bucketData[b] = ( cp.array(x),cp.array(y),cp.array(t, dtype=np.int64) )
        print( 'Bucket=[%d] has [%d] points.' % (b, len(x)))
    
    return bucketData
