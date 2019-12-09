#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 14:50:54 2019

@author: jon
"""

import MalardClient.MalardClient as c

import numpy as np
import pandas as pd

from datetime import datetime

client = c.MalardClient()

jakobshavn = "/data/puma1/scratch/cryotempo/notebook/shpfiles/jakobshaven_NPS.shp"

extentFilter=c.MaskFilter(p_shapeFile = jakobshavn)

ds = c.DataSet("cryotempo","GRIS_BaselineC_Q2","greenland")

resolution = 2000
power = 2

  
projections = ['x','y','time','elev','swathFileId','coh','power','demDiff','demDiffMad']
filters = [{"column":"power","op":"gte","threshold":100},{"column":"coh","op":"gte","threshold":0.6},{"column":"demDiffMad","op":"lt","threshold":20.0},{"column":"demDiff","op":"lte","threshold":100.0},{"column":"demDiff","op":"gte","threshold":-100.0}]   #demDiff<100, demDiff>-100, 

minT = datetime(2011,3,1,0,0,0)
maxT = datetime(2011,5,31,23,59,59)

gridCells = client.gridCellsWithinPolygon(ds, minT, maxT, extentFilter)

start_t = datetime.now()

for gc in gridCells:
    print("MinX {} MaxX {} MinY {} MaxY {}".format(gc.minX,gc.maxX,gc.minY,gc.maxY))
    
    xLower = np.arange(gc.minX, gc.maxX, resolution )
    xUpper = np.arange(gc.minX + resolution, gc.maxX + resolution, resolution )
    xMid = 0.5*(xLower + xUpper)
    
    yLower = np.arange(gc.minY, gc.maxY, resolution )
    yUpper = np.arange(gc.minY, gc.maxY, resolution )
    yMid = 0.5*(yLower + yUpper)
    
    #get the data
    info = client.executeQuery(ds, c.BoundingBox(gc.minX, gc.maxX, gc.minY, gc.maxY, minT, maxT), projections=projections, filters=filters )
    
    if "Success" in info.status:
        df = info.to_df
        client.releaseCacheHandle( info.resultFileName )
        
        print("loaded {}".format( len(df)) )
        
        x = df.x
        y = df.y
        e = df.elev
        
        x_b = resolution * ( np.floor( df["x"] / resolution  ) + 0.5)
        y_b = resolution * ( np.floor( df["y"] / resolution  ) + 0.5)
        
        dist = np.sqrt( (x-x_b)**2 + (y - y_b )**2 )
        inv_dist = 1 / ( np.power(dist, power ) )
        wgt_elev = inv_dist * e
        
        df["x_mid"] = x_b
        df["y_mid"] = y_b
        df["inv_dist"] = inv_dist
        df["wgt_elev"] = wgt_elev
        
        group_df = df.groupby(by=["x_mid","y_mid"]) 
        
        def interp( k, v ):
            x,y = k
            e = v["wgt_elev"].sum() / v["inv_dist"].sum()
            return( x,y,e )
        
        elevs = [ interp(k,v) for k,v in group_df ] 
                    
end_t = datetime.now()

print("{}s".format((end_t - start_t).total_seconds()) )    
        
      