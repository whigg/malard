import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 100)
pd.set_option('display.expand_frame_repr',False)
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib as mpl
from MalardClient.MalardClient import MalardClient
from MalardClient.DataSet import DataSet
from MalardClient.BoundingBox import BoundingBox
import geopandas as gp
from shapely.geometry import Polygon, Point


var = 'Q_uStd'

resolution = 200 # Prod = 200 ### Note that this is inner interp and should be 200
minPoints = 1 # Prod = 1
power = 2 # Prod = 2

binList=[6] # Prod = 6

#years = [2011,2012,2013,2014,2015,2016]
years = [2015]

#limits = [3,5,7,10,15]
limits=[7] # Prod = 7

areaLabel = 'Jak'# 'pet' # or 'Jak', 'str'

def toGeoDataPoint(data, crs):
    geometry = [Point(xy) for xy in zip(data['x'], data['y'])]
    gData = gp.GeoDataFrame(data, crs=crs, geometry=geometry)
    return gData
 
 
def griddata(df,param,resolution,maxCentreDist,minCount):
    x = df.x
    y = df.y
    e = df[param]
    
    #_b = mid point of pixel
    x_b = resolution * ( np.floor( df["x"] / resolution  ) + 0.5)
    y_b = resolution * ( np.floor( df["y"] / resolution  ) + 0.5)
    
    # Calc x and y distance to centre
    x_dist = x - x_b
    y_dist = y - y_b 
    
    #Get weights with respect to distance to centre of pixel
    dist = np.sqrt( (x_dist)**2 + (y_dist)**2 )
    inv_dist = 1 / ( np.power(dist, power ) )
    wgt_param = inv_dist * e
    
    df["x_mid"] = x_b
    df["y_mid"] = y_b
    df["inv_dist"] = inv_dist
    df["wgt_param"] = wgt_param
    df['x_dist'] = x_dist
    df['y_dist'] = y_dist
    
    group_df = df.groupby(by=["x_mid","y_mid"]).agg(['sum','mean','count']).reset_index()
      
    return_df = pd.DataFrame()
    return_df['x'] = group_df['x_mid']
    return_df['y'] = group_df['y_mid']
    return_df[param] = group_df['wgt_param']['sum'] / group_df['inv_dist']['sum']
    return_df['x_dist'] = group_df['x_dist']['mean']
    return_df['y_dist'] = group_df['y_dist']['mean']
    return_df['count'] = group_df['y_dist']['count']
    
    return_df = return_df[(abs(return_df['x_dist'])<maxCentreDist) & (abs(return_df['y_dist'])<maxCentreDist)]
    
    return_df = return_df[return_df['count']>=minCount]
    
    return_df = return_df.drop(['x_dist','y_dist','count'],axis=1)
    
    return return_df
 
def printgrid(group_df,param,title,paramPretty, crs):
    
    fig, ax = plt.subplots(figsize=(6,12))
    cm = 'viridis'
    dfPoints = toGeoDataPoint(group_df, crs)
    msize = 4
    vmin=group_df[param].min()
    vmax=group_df[param].max()
    
    print("min {} max {}".format(vmin,vmax))
    
    dfPoints.plot(ax=ax, column=param, s=msize, cmap=cm, vmin=vmin, vmax=vmax)
    bar = fig.colorbar(mappable=mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=vmin, vmax=vmax), cmap=cm))
    bar.set_label(paramPretty)
    
    # add plot title
    plt.title(title)
    ax.set_xlabel("X Coordinate (km)")
    ax.set_ylabel("Y Coordinate (km)")
    def format_func(value, tick_number):
        return "{}".format(int(value/1000))
    ax.xaxis.set_major_formatter(plt.FuncFormatter(format_func))
    ax.yaxis.set_major_formatter(plt.FuncFormatter(format_func))
    
    #plt.savefig('/data/puma1/scratch/cryotempo/uncertainty/images/GriddedPlots/{}_{}.png'.format(loadName,title))
    
    plt.show()    
    
 
def setNoData(df,param,res,noDataValue):
    xRange = np.arange(df.x.min(),df.x.max()+res,res)
    yRange = np.arange(df.y.min(),df.y.max()+res,res)
    xDF = pd.DataFrame({'join':1,'x':xRange})
    yDF = pd.DataFrame({'join':1,'y':yRange})
    xyDF = pd.merge(xDF,yDF,on=['join'],how='inner').drop('join',axis=1)
    
    padDF = pd.merge(xyDF,df,on=['x','y'],how='left')
    
    padDF.fillna(value=noDataValue,inplace=True)
    
    return padDF
     
def saveCSV(df,numBins,limit,param, loadName, outputpath):
    a = pd.DataFrame()
    a['x'] = df['x']
    a['y'] = df['y']
    a['z'] = df[param]
    
    a.sort_values(['y','x'],ascending=[False, True], inplace=True)
    #a.sort_values(['y','x'], inplace=True)
    a.to_csv(outputpath, index=False)      

def main():
    
    for year in years:
        for numBins in binList:
            print("Start {} with {} bins".format(year,numBins))
            #Load data
            loadName = 'Joined-{}-Mar-May-{}'.format(areaLabel,year)
            df = pd.read_hdf('/data/puma1/scratch/cryotempo/uncertainty/processeddata/{}_with_uncert_{}bins.h5'.format(loadName,numBins))
            
            #df_old = df[(df['coh']>minCoh) & (df['power']>minPower) & (abs(df['demDiff'])<maxDemDiff) & (abs(df['demDiffMad'])<maxDemDiffMad)]
            
            print("loaded {}".format( len(df)) )
            ##
            start_t = datetime.now()
          
            for limit in limits:
                df_Full = df[df[var]<=limit]
                df_Full = df_Full[['x','y','elev']]
    
                df_idw = griddata(df_Full,'elev',resolution,resolution,minPoints)
                df_grid = setNoData(df_idw,'elev',resolution,-999)
                saveCSV(df_grid,numBins,limit,'elev', loadName)
                
       
            end_t = datetime.now()
                
            print("{}s".format((end_t - start_t).total_seconds()) )    

if __name__ == "__main__":
    main()
        
        

