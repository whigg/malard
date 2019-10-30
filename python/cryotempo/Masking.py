#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  8 14:18:23 2019

@author: jon
"""
from MalardClient.MalardClient import MalardClient
from MalardClient.DataSet import DataSet
from math import floor

import DataSets as pds

import pandas as pd

def interpolationGrid( startX, startY, gridCellSize, resolution ):
     
    n = floor(gridCellSize / resolution)
    r = range(0, n)
    
    xcoords = [ i * resolution + resolution * 0.5 + startX for i in r]    
    ycoords = [ i * resolution + resolution * 0.5 + startY for i in r]

    x =[]
    y= []

    for xc in xcoords:
        for yc in ycoords:
            x.append(xc)
            y.append(yc)
    
    df = pd.DataFrame()
    df['x'] = x
    df['y'] = y
    
    return df

environmentName = 'DEVv2'
    
gridCellSize = 100000
resolution = 500

mask_prefix = "SARIN"
    
client = MalardClient( environmentName, True )

dataSet = DataSet( 'cryotempo', 'GRIS_BaselineC_Q2', 'greenland')

proj4 = client.getProjection(dataSet).proj4
    
mask = '/data/puma1/scratch/cryotempo/masks/icesheets.shp' if mask_prefix == "ICE" else '/data/puma1/scratch/cryotempo/sarinmasks/{}_Greenland.shp'.format(mask_prefix) 

tmpPath = '/home/jon/data/masks/'
    
bbox = client.boundingBox(dataSet)

gridCells = client.gridCells(dataSet, bbox)    
    
for gc in gridCells:
    data = interpolationGrid( gc.minX, gc.minY, gridCellSize, resolution )
    
    point_ds = pds.PointDataSet(data, proj4)
    geoDs = point_ds.asGeoDataSet()
    geoDs.withinMask(mask, mask_prefix)
    
    results = geoDs.getData()
    stats = {}
    stats['InterpolationCount'] =float( results['within_{}'.format(mask_prefix)].sum() )
    print( client.query.publishGridCellStats(dataSet.parentDataSet, "{}_GridCellInterpolationCount".format(mask_prefix), gc.minX, gc.minY, gridCellSize, stats))
    
    mask_file = "mask_{}_{}_{}.csv".format(dataSet.dataSet, gc.minX, gc.minY )
    results.to_csv(tmpPath  + mask_file)        
 
    print(client.query.publishMask(tmpPath, mask_file, dataSet.parentDataSet, dataSet.dataSet, '{}_{}m'.format(mask_prefix, resolution), dataSet.region, gc.minX, gc.minY, gridCellSize ))
    