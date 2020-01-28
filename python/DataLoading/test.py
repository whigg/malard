#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test program for loading data to see where it is geo-located in a region of interest
"""

import MalardClient.MalardClient as mc
import geopandas as gp
from shapely.geometry import Point
import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime

def toGeoDataPoint(data, crs):
    geometry = [Point(xy) for xy in zip(data['x'], data['y'])]
    gData = gp.GeoDataFrame(data, crs=crs, geometry=geometry)
    return gData


def simplePlot(title, df_swath,bb, background_map):
## plot with no filters
    fig, ax = plt.subplots(figsize=(12,10))
    
    #ax.set_xlim([bb.minX,bb.maxX])
    #ax.set_ylim([bb.minY,bb.maxY])
    
    background_map.plot(ax=ax, edgecolor='grey', color='gainsboro', linewidth=0.5)
 
    df_swath.plot(ax=ax, color="lightblue",alpha=0.1,markersize=1.0)
   
     # add plot title
    plt.title(title)
    ax.set_xlabel("X Coordinate (km)")
    ax.set_ylabel("Y Coordinate (km)")
    def format_func(value, tick_number):
        return "{}".format(int(value/1000))
    ax.xaxis.set_major_formatter(plt.FuncFormatter(format_func))
    ax.yaxis.set_major_formatter(plt.FuncFormatter(format_func))
   
    
    return fig

def main():
    client = mc.MalardClient()

    ds = mc.DataSet("cryotempo","swath_c_nw","greenland")

    bb = client.boundingBox( ds )    
    
    proj4 = client.getProjection( ds ).proj4

    minT = datetime(2011,3,1,0,0,0 )
    maxT = datetime(2011,4,1,0,0,0 )
    
    ice_file = "/data/puma1/scratch/cryotempo/masks/icesheets.shp"
    maskFilterIce = mc.MaskFilter( p_shapeFile=ice_file)
    maskFilterLRM = mc.MaskFilter( p_shapeFile="/data/puma1/scratch/cryotempo/sarinmasks/LRM_Greenland.shp" , p_includeWithin=False )
    maskFilters = [maskFilterIce, maskFilterLRM]
    
    gcs = client.gridCellsWithinPolygon(ds, minT, maxT, maskFilterIce, maskFilters=maskFilters)
    nrgc = len(gcs)
    
    minCoh = 0.5
    minPowerdB = -160
    maxDemDiff = 100
    maxDemDiffMad = 20
    minPower = 100
    #{"column":"inRegionMask","op":"eq","threshold":1},
    filters = [{"column":"powerScaled","op":"gte","threshold":minPower},{"column":"powerdB","op":"gte","threshold":minPowerdB},{"column":"coh","op":"gte","threshold":minCoh},{"column":"demDiffMad","op":"lt","threshold":maxDemDiffMad},{"column":"demDiff","op":"lte","threshold":maxDemDiff},{"column":"demDiff","op":"gte","threshold":-maxDemDiff}]   #demDiff<100, demDiff>-100, 
  
    st = datetime.now()
    all_df = []    
    for i, gc in enumerate(gcs):
        
        print("Processing i=[{0}] out of [{1}]".format( i+1, nrgc ) )
        
        bb = mc.BoundingBox( gc.minX, gc.maxX, gc.minY, gc.maxY, minT, maxT )
        
        results = client.executeQuery(ds, bb, maskFilters = maskFilters, filters=filters)
    
        all_df.append(results.to_df)
        client.releaseCacheHandle(results.resultFileName)
    
    print("execute query finished.")
    end = datetime.now()
    
    print("Took {} seconds to load data".format((end - st).total_seconds() ) )
    df = pd.concat( all_df )
    print(len(df))
    #g_df = toGeoDataPoint( df, proj4 )    
    #background = gp.read_file(ice_file)
    
    #simplePlot("Test area",g_df,bb,background)
    



if __name__ == "__main__":
    main()
