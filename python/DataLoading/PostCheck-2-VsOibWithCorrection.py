import MalardClient.MalardClient as c

import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from osgeo import gdal

import Timeseries as t

from datetime import datetime

def getPixel(x, y, gt):
    '''
    :param x: pandas df
    :param y: pandas df
    :param gt: GeoTransform
    :return:
    ''' 
    py = ((x-gt[0])/gt[1]).astype('int64')
    px = ((gt[3]-y)/-gt[5]).astype('int64')
    
    return px.values, py.values

def printresult(data, year, count, df, coverage, limit, maxpixel,res, demName):
    
    print("Nr elements before dem filter {}".format(len(data)))
    filtered = data[data["demSwathElev"] > 0]
    print("Nr elements after dem filter {}".format(len(filtered)))
    filtered = filtered[filtered["adCentElev"] > -9999]
    print("Nr elements after adCentElev filter {}".format(len(filtered)))
    filtered = filtered[filtered["adPtElev"] > -9999]
    print("Nr elements after adPtElev filter {}".format(len(filtered)))
    
    minElevComp = filtered['elevComp'].min()
    maxElevComp = filtered['elevComp'].max()
    mean = filtered["elevComp"].mean()
    std = filtered["elevComp"].std()
    mad = filtered["elevComp"].mad()
    median = filtered["elevComp"].median()
    rec = pd.DataFrame({"DemName": demName, "Year":[year],"Limit":[limit],"MaxPix":[maxpixel],"Res":[res],"Mean":[mean],"Std":[std],"Count":count,"Mad":mad,"Median":median,"MinElevComp":minElevComp,"MaxElevComp":maxElevComp,"Coverage":coverage})
    
    return df.append(rec)
    

def computeOibError(year, month, demFolder, unc):
    
    
    #     
    client = c.MalardClient("MALARD-PROD")
    ""
    results = pd.DataFrame(columns=["Year","Limit","MaxPix","Res","Mean","Std","Count","Mad","Median","MinElevComp","MaxElevComp","Coverage"])
    
    #print("start {}".format(year))
    
    minT = datetime(year,3,1,0,0,0)
    maxT = datetime(year,5,31,23,59,59)
    
    #Load Swath
    dsOib = c.DataSet("cryotempo","oib","greenland")
    
    #Joined-Jak-Mar-May-2016_Grid_elev_bins6_limit3.tif
    tifPath = demFolder
    
    demName = "swath_c_GrIS_OIB_unc_{}_masked_8.tif".format(datetime(year,month,15).strftime("%Y%m%d"))
    demFullPath = os.path.join(tifPath, demName)
        
    dem = gdal.Open(demFullPath, gdal.GA_ReadOnly)
    demArray = dem.GetRasterBand(1).ReadAsArray()
    demGt = dem.GetGeoTransform()
    
    M,N = demArray.shape
    count = 0
    
    
    for i in range(0,M):
        for j in range(0,N):
            count += 1 if demArray[i,j] > -32768 else 0 
    
    print("Number of points in DEM {}".format(count))
    
    #[-700000.0, -2300000.0, 200000.0, -900000.0]
    extent = t.getExtent(demFullPath)
    bb = c.BoundingBox( extent[0], extent[1], extent[2], extent[3], minT, maxT )
   
    length = len(demArray[demArray>0])
    
    ad = gdal.Open('/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/combined/GrIS_dem.tif', gdal.GA_ReadOnly)
    adArray = ad.GetRasterBand(1).ReadAsArray()
    adGt = ad.GetGeoTransform()
    
    ad = gdal.Open('/data/puma/scratch/cryotempo/masks_raster/GrIS_noLRM_2000.tif', gdal.GA_ReadOnly)
    adCoverageArray = ad.GetRasterBand(1).ReadAsArray()
    
    totalPoints = np.sum(adCoverageArray)
    print("TotalPoints {}".format(totalPoints))
    coverage = count/totalPoints
    print("Coverage {:.2%}".format( coverage ))
    oib_filter = [{"column":"inRegionMask", "op":"eq", "threshold":1}]
    
    oibResult = client.executeQuery(dsOib, bb, filters = oib_filter)
    oib = oibResult.to_df
    #oib = oib[oib.x<-108000.000]
    #print("Oib length is: {}".format(len(oib)))
    
    #print( "minX={} maxX={} minY={} maxY={}".format(oib["x"].min(), oib["x"].max(), oib["y"].min(), oib["y"].max()) )
    
    resolution = 2000
    x_b = resolution * ( np.floor( oib["x"] / resolution  ) + 0.5)
    y_b = resolution * ( np.floor( oib["y"] / resolution  ) + 0.5)
    
    oib['adPtElev'] = adArray[getPixel(oib['x'],oib['y'],adGt)]
    oib['adCentElev'] = adArray[getPixel(x_b,y_b,adGt)]
    
    oib['adjCentre'] = oib['adPtElev'] - oib['adCentElev']
    
    oib['demSwathElev'] = demArray[getPixel(x_b,y_b,demGt)]
    
    oib['elevComp'] = oib['demSwathElev'] - oib['WGS84_Ellipsoid_Height(m)'] + oib['adPtElev'] - oib['adCentElev']
    
    results = printresult(oib, year, length, results, coverage, limit=unc, maxpixel=8,res=2000,demName=demName)
        
    return results
        
if __name__ == "__main__":
    years = [2011,2012,2013,2014,2015,2016]
    unc= 6
    
    demFolder = "/data/puma/scratch/cryotempo/processeddata/greenland_oib/GrIS_OIB_PDD_100_PwrdB_-160_Coh_0.6_Unc_{}_MaxPix_8_DemDiffMad_6_Res_2000".format(unc)
    
    results = [ computeOibError(y,4, demFolder,unc) for y in years]
    
    df = pd.concat(results)
    print(df)
    df.to_csv(os.path.join(demFolder, "OIBCompare_u{}_withMaskAndAdj_8.csv".format(unc)))
    
        
