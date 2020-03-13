import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
import numpy as np
#from sklearn.linear_model import LinearRegression
from sklearn import preprocessing
import matplotlib.pyplot as plt
from scipy.stats import binned_statistic
pd.set_option('display.max_columns', 100)
from osgeo import gdal
import time
import sys

def getPixel(x, y, gt):
    '''
    :param x: pandas df
    :param y: pandas df
    :param gt: GeoTransform
    :return:
    '''
    py = ((x-gt[0])/gt[1]).astype('int64')
    px = ((y-gt[3])/gt[5]).astype('int64')
    return px.values, py.values

#Load DEM
def loadDem(bb = None):    
    
    def filename( name ):
        return "/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/{name}_tiled/GrIS_{name}_{minX}_{minY}_to_{maxX}_{maxY}.tif".format( name=name, minX=int(bb.minX/1000), maxX=int(bb.maxX/1000), minY=int(bb.minY/1000), maxY=int(bb.maxY/1000) )
    
    demFile = '/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/combined/GrIS_dem.tif' if bb is None else filename("dem")
    roughnessFile = '/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_roughness/combined/GrIS_roughness.tif' if bb is None else filename("roughness")
    
    dem = gdal.Open(demFile, gdal.GA_ReadOnly)
    demArray = dem.GetRasterBand(1).ReadAsArray()
    demGt = dem.GetGeoTransform()
    roughness = gdal.Open(roughnessFile, gdal.GA_ReadOnly)
    roughArray = roughness.GetRasterBand(1).ReadAsArray()
    roughGt = roughness.GetGeoTransform()
    
    print("Dem file loaded {}".format(demFile))
    print("Roughness file loaded {}".format(roughnessFile))
    
    return ( demGt, demArray, roughGt, roughArray)



####### Get Coordinates of along and across slope #######

acrossDist = 1600
alongDist = 400

test = False

def updateStats( name, value, stats ):
     if name in stats:
        val = stats[name]
        stats[name] = val + value
     else:
        stats[name] = value

def calcSlopeAndRoughNess( data, stats, bb ):
    
    
    demGt, demArray, roughGt, roughArray = loadDem(bb)
    
    s = time.time() 
    
    wfData = createSwathSlope( data, acrossDist, alongDist, test, stats )
    
    slope_end = time.time()
    
    updateStats( "Slope_t", slope_end - s, stats )
        
    
    wfData = calcDemSlopeAndRoughness( wfData, demGt, demArray, roughGt, roughArray )
    
    dem_t = time.time() - slope_end
    
    updateStats( "Dem_t", dem_t, stats )
    
    return wfData


def createSwathSlope(data, acrossDist, alongDist, test, stats):
    
    swath_st = time.time() 
    wfDataOut = run( data, acrossDist, alongDist, test, stats)
    updateStats("SlopeRun_t", time.time() - swath_st, stats)
    
    return pd.concat( wfDataOut, ignore_index = True )


def run(data,acrossDist,alongDist,test, stats):
    #firstIter = True
    batchTime = time.time()
    ii = 1
    skipCount=0
    wfNums = data.wfUnique.unique()
    wfCount = len(wfNums)
    dataWithSlopeCoords = []
    #dataWithSlopeCoords = pd.DataFrame()
    for wf in wfNums:
        
        if (ii)%500 == 0:
            taken = time.time()-batchTime
            print("Num: {} of {} - wf: {}, BatchTime={:.4f}".format(ii,wfCount,wf,taken))
            batchTime = time.time()
        
        s = time.time()
        wfData = data[data['wfUnique']==wf].reset_index()
        updateStats("wf_filter", time.time() - s, stats )        
        
        if len(wfData)<=1:
            #print("Less than 2 records\n")
            ii=ii+1
            skipCount=skipCount+1
            #This os an additional quality metric
            continue
        
        s = time.time()
        wfEquation = smf.ols('y ~ x', data=wfData).fit()
        ols_end = time.time()
        updateStats("Slope_OLS", ols_end - s, stats )
        
        wfSlope = wfEquation.params['x']
        hypLength = np.sqrt(wfSlope**2 + 1)
        yAcrossDist = wfSlope/hypLength * acrossDist/2
        xAcrossDist = 1/hypLength * acrossDist/2
    
        alongSlope = -1/wfSlope #Perpendicular slopes are opposite reciprocals
        alongHypLength = np.sqrt(alongSlope**2 + 1)    
        if wfSlope == 0.0:
            yAlongDist = alongDist/2
            xAlongDist = 0.0
        else:
            yAlongDist = alongSlope/alongHypLength * alongDist/2
            xAlongDist = 1/alongHypLength * alongDist/2
        
        wfData['wfSlope'] = wfSlope
        wfData['alongSlope'] = alongSlope 
        wfData['alongHypLength'] = alongHypLength
        wfData['yAlongDist'] = yAlongDist
        wfData['xAlongDist'] = xAlongDist
        
        if xAcrossDist>0:
            swap = 1
        else:
            swap = -1
        wfData['xLeftAcross'] =-swap*xAcrossDist+wfData['x']
        wfData['xRightAcross'] = swap*xAcrossDist+wfData['x']
        wfData['yLeftAcross'] = -swap*yAcrossDist+wfData['y']
        wfData['yRightAcross'] = swap*yAcrossDist+wfData['y']
        
        if yAlongDist>0:
            swap = 1
        else:
            swap = -1
        wfData['xDownAlong'] = -swap*xAlongDist+wfData['x']
        wfData['xUpAlong'] = swap*xAlongDist+wfData['x']
        wfData['yDownAlong'] = -swap*yAlongDist+wfData['y']
        wfData['yUpAlong'] = swap*yAlongDist+wfData['y']   
        
        updateStats( "SwathSlope", time.time() - ols_end, stats )
    #startThis = time.time()
        dataWithSlopeCoords.append(wfData)
    #print("Time Taken: {}".format(time.time()-startThis))  
      
#        if firstIter:
#            dataWithSlopeCoords.append(wfData)
#            firstIter = False
#        else:
#            #startThis = time.time()
#            #dataWithSlopeCoords = dataWithSlopeCoords.append(wfData, ignore_index = True)
#            dataWithSlopeCoords.append(wfData)
#            #print("Time Taken: {}".format(time.time()-startThis))
            
        ii=ii+1
        if test:
            if ii>1000:
                print("Number Skipped: {}".format(skipCount))   
                return dataWithSlopeCoords
            
    print("Number Skipped: {}".format(skipCount))        
    return dataWithSlopeCoords

def calcDemSlopeAndRoughness( wfData, demGt, demArray, roughGt, roughArray ):
    wfData['demElevLeftAcross'] = demArray[getPixel(wfData['xLeftAcross'],wfData['yLeftAcross'],demGt)]
    wfData['demElevRightAcross'] = demArray[getPixel(wfData['xRightAcross'],wfData['yRightAcross'],demGt)]
    wfData['demElevDownAlong'] = demArray[getPixel(wfData['xDownAlong'],wfData['yDownAlong'],demGt)]
    wfData['demElevUpAlong'] = demArray[getPixel(wfData['xUpAlong'],wfData['yUpAlong'],demGt)]
    wfData['roughness'] = roughArray[getPixel(wfData['x'],wfData['y'],roughGt)]
    wfData['demElev'] = demArray[getPixel(wfData['x'],wfData['y'],demGt)]
    
    #Add north south direction - heading North = 1, which means rising y is going forwardings on Greenland
    #Need to put in special condition for Antarctica as only true if y<0, else reverse
    wfData['NS'] = wfData['heading'].apply(lambda x: 1 if x <90 else -1)
    
    wfData['slopeAlong'] = (wfData['demElevUpAlong'] - wfData['demElevDownAlong'])*wfData['NS']/alongDist 
    wfData['slopeAcross'] = (wfData['demElevRightAcross'] - wfData['demElevLeftAcross'])*wfData['NS']/acrossDist
        
    return wfData

##### Run #######
def main( years, areaLabel ):
    for year in years:
    
        start = time.time()
        
        print("Start {}".format(year))
        
        #Load Malard
        loadName = 'Joined-{}-Mar-May-{}'.format(areaLabel,year)
        data = pd.read_hdf('/data/puma1/scratch/cryotempo/uncertainty/processeddata/{}.h5'.format(loadName))
        
        demGt, demArray, roughGt, roughArray = loadDem()
        
        wfData = createSwathSlope(data,acrossDist,alongDist,test)
        
        ##### End of Coordinate code #########
        
        ##### Get values from DEM #######
        
        print("Before DEM Time Taken: {}".format(time.time()-start))
        
        
        wfData.to_hdf('/data/puma1/scratch/cryotempo/uncertainty/processeddata/{}_beforeDem.h5'.format(loadName),'data') 
        
        wfData = calcDemSlopeAndRoughness(wfData, demGt, demArray, roughGt, roughArray)
        
        wfData.to_hdf('/data/puma1/scratch/cryotempo/uncertainty/processeddata/{}_withDem.h5'.format(loadName),'data') 
        
        print("After DEM Time Taken: {}".format(time.time()-start))
        
        del data, wfData

if __name__ == "__main__":

    #years=[2012,2013,2014,2015,2016]
    years=[2012]
    areaLabel = 'Jak'  # or 'jak', 'str', 'nwgrnld'

    main(years, areaLabel)
