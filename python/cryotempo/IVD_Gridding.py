#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 09:55:03 2019

@author: jon
"""

import MalardGDAL as mg
from MalardClient.MalardClient import MalardClient
from MalardClient.DataSet import DataSet
from MalardClient.BoundingBox import BoundingBox
from MalardClient.MaskFilter import MaskFilter

from datetime import datetime


client = MalardClient()

ds = DataSet("cryotempo","swath_c", "greenland" )

proj4 = client.getProjection(ds).proj4

print(proj4)

minX = 700000
minY = -2200000
cell_size = 130000


bbox = BoundingBox( minX, minX + cell_size, minY, minY + cell_size, datetime(2011,2,1,0,0),datetime(2011,5,1,0,0) )

 ## TODO: These need to be stored in Malard by DataSet and Type.    
maskFilterIce = MaskFilter( p_shapeFile="/data/puma1/scratch/cryotempo/masks/icesheets.shp"  ) 
maskFilterLRM = MaskFilter( p_shapeFile="/data/puma1/scratch/cryotempo/sarinmasks/LRM_Greenland.shp" , p_includeWithin=False ) 

filters = [{"column":"power","op":"gte","threshold":10000},{"column":"coh","op":"gte","threshold":0.8}]


maskFilters = [ maskFilterIce, maskFilterLRM ]

print("before executeQuery..")
    
resultInfo = client.executeQuery(ds, bbox, maskFilters=maskFilters, filters = filters )
 
print( resultInfo.message )

#no make the spatial one

print( len(resultInfo.to_df))
nc_path = "Grid_Spatial.nc"

tif_path = "Grid/out_test.nc"

print(mg.makeSpatialNC(resultInfo.resultFileName,proj4,nc_path))

print(mg.createGrid(nc_path,tif_path,algorithm='invdist'))

mg.plot(tif_path)



