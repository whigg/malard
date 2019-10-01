#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 08:38:30 2019

@author: jon
"""
import json
from datetime import datetime

from MalardClient.DataSetQuery import DataSetQuery
from MalardClient.AsyncDataSetQuery import AsyncDataSetQuery
from MalardClient.BoundingBox import BoundingBox
from MalardClient.DataSet import DataSet

class MalardClient:
    def __init__(self, environmentName='DEVv2', notebook = True):
        self.environmentName = environmentName
        self.serverUrl = '://localhost:9000'
        self.query = DataSetQuery( "http" + self.serverUrl, self.environmentName )
        self.asyncQuery = AsyncDataSetQuery( "ws" + self.serverUrl, self.environmentName, notebook )    

    def boundingBox(self, dataSet ):
        
        bbox = json.loads(self.query.getDataSetBoundingBox( dataSet.parentDataSet, dataSet.dataSet, dataSet.region ))
        
        #Setup the bounding box
        minX = bbox['gridCellMinX']
        maxX = bbox['gridCellMaxX']
        minY = bbox['gridCellMinY']
        maxY = bbox['gridCellMaxY']
        minT = datetime.fromtimestamp( bbox['minTime'] )
        maxT = datetime.fromtimestamp( bbox['maxTime'] )
        numberOfPoints = bbox['totalPoints']
        return BoundingBox( dataSet, minX, maxX, minY, maxY, minT, maxT, numberOfPoints )

    def gridCells( self, boundingBox):
        bb = boundingBox
        gcs = json.loads(self.query.getGridCells(bb.dataSet.parentDataSet, bb.dataSet.dataSet, bb.dataSet.region, bb.minX, bb.maxX, bb.minY, bb.maxY, bb.minT, bb.maxT))
        
        return [BoundingBox( bb.dataSet, gc['gridCellMinX'], gc['gridCellMaxX'], gc['gridCellMinY'], gc['gridCellMaxY'], gc['minTime'], gc['maxTime'], gc['totalPoints'] ) for gc in gcs ]
        
    def executeQuery( self, boundingBox, projections=[], filters=[], xCol='x', yCol='y' ):
        bb = boundingBox
        return self.asyncQuery.executeQuery(bb.dataSet.parentDataSet, bb.dataSet.dataSet, bb.dataSet.region, bb.minX, bb.maxX, bb.minY, bb.maxY, bb.minT, bb.maxT, projections, filters, xCol, yCol)
        
    def publishGridCellStats(self, boundingBox, runName , statistics):
        return self.query.publishGridCellStats(boundingBox.dataSet.parentDataSet, runName, boundingBox.minX, boundingBox.minY, boundingBox.maxX - boundingBox.minX, statistics )
    
    def releaseCacheHandle(self, cacheHandle ):
        return self.asyncQuery.releaseCache( cacheHandle  )
