#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 08:38:30 2019

@author: jon
"""
import DataSetQuery
import AsyncDataSetQuery
import json
from datetime import datetime

class DataSet:
    def __init__(self, parentDs, dataSet, region ):
        self._parentDataSet = parentDs
        self._dataSet = dataSet
        self._region = region
        
    def __str__(self):
        return "parentDataSet={}, datSet={}, region={}".format(self.parentDataSet, self.dataSet, self.region)
        
    @property
    def parentDataSet(self):
        return self._parentDataSet
    
    @property
    def dataSet(self):
        return self._dataSet
    
    @property
    def region(self):
        return self._region
    
class BoundingBox:
    def __init__( self, dataSet, minX, maxX, minY, maxY, minT, maxT, numberOfPoints ):
        self._dataSet = dataSet
        self._minX = minX
        self._maxX = maxX
        self._minY = minY
        self._maxY = maxY
        self._minT = minT
        self._maxT = maxT
        self._numberOfPoints = numberOfPoints
    
    def __str__(self):
        return "{}, minX={}, maxX={}, minY={}, maxY={}, minT={}, maxT={} N={}".format(str(self.dataSet),self._minX, self._maxX, self._minY, self._maxY, self._minT, self._maxT, self._numberOfPoints)
    
    @property
    def dataSet(self):
        return self._dataSet
    @property
    def minX(self):
        return self._minX
    @property
    def maxX(self):
        return self._maxX
    @property
    def minY(self): 
        return self._minY
    @property
    def maxY(self):
        return self._maxY
    @property
    def minT(self): 
        return self._minT
    @property
    def maxT(self):
        return self._maxT
    @property
    def numberOfPoints(self):
        return self._numberOfPoints

class MalardClient:
    def __init__(self, environmentName='DEVv2'):
        self.environmentName = environmentName
        self.serverUrl = '://localhost:9000'
        self.query = DataSetQuery.DataSetQuery( "http" + self.serverUrl, self.environmentName )
        self.asyncQuery = AsyncDataSetQuery.AsyncDataSetQuery( "ws" + self.serverUrl, self.environmentName )    


    def boundingBox(self, dataSet : DataSet ):
        
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
        