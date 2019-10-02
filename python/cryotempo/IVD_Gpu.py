
import MalardHelpers as m
import numpy as np
import cupy as cp
from datetime import datetime

from math import floor

inputPath = '/data/mouse1/team/jon/fuse/'

largeFile = 'cryotempo_GRIS_BaseC_Q2.nc'
smallFile = 'cryotempo_oib.nc'

def bucket( x , y, minX, maxX, minY, maxY, nrOfBucketsInRow ):
    xInterval = ( maxX - minX ) / nrOfBucketsInRow
    yInterval = ( maxY - minY ) / nrOfBucketsInRow
    
    xPos = int(( x - minX ) / xInterval) + 1
    yPos = int(( y - minY ) / yInterval)
    
    return min(yPos * nrOfBucketsInRow + xPos, nrOfBucketsInRow* nrOfBucketsInRow)

def bucketData( x, y, bucketSrs ):
    
    buckets = range(1,numBucketsPerRow * numBucketsPerRow +1)
    
    bucketData = {}
    for b in buckets:
        xyb = zip(x,y,bucketSrs)
        xy = [(x,y) for x,y,d in xyb if d == b ]
        xs,ys = zip(*xy)
        
        print( 'Bucket=[%d] Length=[%d]' % (b, len(xs)) )             
        if len(xs) > 0:
            bucketData[b] = (cp.array(xs), cp.array(ys))
        else:
            bucketData[b] =(None, None )
    return bucketData

def bucketData2( xArray, yArray, bucketSrs ):    
    buckets = range(1,numBucketsPerRow * numBucketsPerRow +1)
    
    bucketData = {}
    for b in buckets:
        bucketData[b] = ([],[])
    
    for i in range(0,len(xArray)):
        b = bucketSrs[i]
        x = xArray[i]
        y = yArray[i]
        
        dataX, dataY = bucketData[b]
        
        dataX.append(x)
        dataY.append(y)
        
        bucketData[b] = (dataX,dataY)
    
    for b,(x,y) in bucketData.items():
        bucketData[b] = ( cp.array(x),cp.array(y) )
        print( 'Bucket=[%d] has [%d] points.' % (b, len(x)))
    
    return bucketData

startTime = datetime.now()

dfLarge = m.getDataFrameFromNetCDF(inputPath + largeFile)
dfSmall = m.getDataFrameFromNetCDF(inputPath + smallFile)

dfTime = datetime.now()

print( 'Dataframes took: %s' % ( dfTime - startTime ))

largeX = np.array(dfLarge['x'])
smallX = np.array(dfSmall['x'])

largeY = np.array(dfLarge['y'])
smallY = np.array(dfSmall['y'])


bucketStartTime = datetime.now()

minX = floor(np.min(largeX))
maxX = floor(np.max(largeX))
minY = floor(np.min(largeY))
maxY = floor(np.max(largeY))

minXArray = cp.array(np.empty(len(largeX)))
minXArray.fill(minX)
maxXArray = cp.array(np.empty(len(largeX)))
maxXArray.fill(maxX)

minYArray = cp.array(np.empty(len(largeY)))
minYArray.fill(minY)
maxYArray = cp.array(np.empty(len(largeY)))
maxYArray.fill(maxY)

numBucketsPerRow = 5

bucketsArray = cp.array(np.empty(len(largeX)))
bucketsArray.fill(numBucketsPerRow)

bucketKernel = cp.ElementwiseKernel(
        'float64 x, float64 y, float64 minX, float64 maxX, float64 minY, float64 maxY, float64 numRows',
        'float64 bucketNumber',
        'bucketNumber = min(floor((y - minY)/((maxY - minY)/numRows))*numRows + floor((x - minX)/((maxX - minX)/numRows)) + 1, (numRows * numRows) )',
        'bucket'
        )

bucketSrs = bucketKernel(cp.array(largeX),cp.array(largeY),minXArray,maxXArray, minYArray, maxYArray, bucketsArray  )
bucketSrs = cp.asnumpy(bucketSrs)

bucketEndTime = datetime.now()

print('Bucketing took: %s' % ( bucketEndTime - bucketStartTime ))

bdata = bucketData2( largeX, largeY, bucketSrs  )

arraysTime = datetime.now()
print('Building bucketed arrays took=[%s]' % (arraysTime - bucketEndTime ) )        
print('MinX=[%f] MaxX=[%f] MinY=[%f] MaxY=[%f] '%( minX, maxX, minY, maxY ) )

results = np.zeros(len(smallX))

elementKernel = cp.ElementwiseKernel(
     'float64 x, float64 y',  # input params
     'float64 z',  # output params
     'z = sqrt(x * x + y * y)',  # map
     'distance'  # kernel name
 )


for i in range(0, len(smallX)):
    x, y = bdata[bucket(smallX[i], smallY[i], minX, maxX, minY, maxY, numBucketsPerRow )]
    
    if x is not None:
        minusX1 = cp.subtract(x, smallX[i] )
        minusY1 = cp.subtract(y, smallY[i] )

        results[i] = cp.min(elementKernel(minusX1,minusY1))
    else:
        results[i] = np.nan
        
print('Closest %f furthest %f' % (min(results), max(results)))

endTime = datetime.now()

print( endTime - startTime )




