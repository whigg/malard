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
from MalardClient.Projection import Projection
from MalardClient.Shard import Shard

class MalardClient:
    def __init__(self, environmentName='DEVv2', notebook = True):
        self.environmentName = environmentName
        self.serverUrl = '://localhost:9000'
        self.query = DataSetQuery( "http" + self.serverUrl, self.environmentName )
        self.asyncQuery = AsyncDataSetQuery( "ws" + self.serverUrl, self.environmentName, notebook )    

    def getParentDataSets(self):
        pds = self.query.getParentDataSets()
        j_obj = json.loads( pds )
        return [ n['name'] for n in j_obj ]
    
    def getDataSets(self, parentDataSet):
        listOfDatasets = self.query.getDataSets(parentDataSet)

        return [DataSet(parentDataSet, ds['name'], ds['region'] ) for ds in json.loads(listOfDatasets) ]
    
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
        return BoundingBox( minX, maxX, minY, maxY, minT, maxT, numberOfPoints )

    def gridCells( self, dataSet, boundingBox, xCol = "x", yCol="y"):
        bb = boundingBox
        gcs = json.loads(self.query.getGridCells(dataSet.parentDataSet, dataSet.dataSet, dataSet.region, bb.minX, bb.maxX, bb.minY, bb.maxY, bb.minT, bb.maxT,xCol,yCol))
        
        return [BoundingBox( gc['gridCellMinX'], gc['gridCellMaxX'], gc['gridCellMinY'], gc['gridCellMaxY'], gc['minTime'], gc['maxTime'], gc['totalPoints'] ) for gc in gcs ]
        
    def shards(self, dataSet, boundingBox, xCol = "x", yCol="y"):
        
        bb = boundingBox
        gcs = json.loads(self.query.getShards(dataSet.parentDataSet, dataSet.dataSet, dataSet.region, bb.minX, bb.maxX, bb.minY, bb.maxY, bb.minT, bb.maxT,xCol,yCol))
        
        return [Shard(BoundingBox( gc['minX'], gc['maxX'], gc['minY'], gc['maxY'], gc['minT'], gc['maxT'], gc['numberOfPoints']), gc['shardName']) for gc in gcs ]
        
        
    def executeQuery( self, dataSet, boundingBox, projections=[], filters=[], xCol='x', yCol='y', maskFilters=[] ):
        bb = boundingBox
        return self.asyncQuery.executeQuery(dataSet.parentDataSet, dataSet.dataSet, dataSet.region, bb.minX, bb.maxX, bb.minY, bb.maxY, bb.minT, bb.maxT, projections, filters, xCol, yCol, maskFilters)
        
    def executeQueryPolygon( self, dataSet, minT, maxT, maskFilters, projections=[], filters=[], xCol='x', yCol='y' ):
        return self.asyncQuery.executeQuery(dataSet.parentDataSet, dataSet.dataSet, dataSet.region, 0.0, 0.0, 0.0, 0.0, minT, maxT, projections, filters, xCol, yCol, maskFilters)
        
    def publishGridCellStats(self, dataSet, boundingBox, runName , statistics):
        return self.query.publishGridCellStats(dataSet.parentDataSet, runName, boundingBox.minX, boundingBox.minY, boundingBox.maxX - boundingBox.minX, statistics )
    
    def getSwathNamesFromIds( self, dataset, file_ids ):
        results = {}
        for f in file_ids:
            swathName = json.loads(self.query.getSwathDetailsFromId(dataset.parentDataSet, dataset.dataSet, dataset.region, f))['swathName']
            results[swathName] = f
        return results
   
    def getProjection( self, dataSet ):
        j_obj = json.loads( self.query.getProjection( dataSet.parentDataSet, dataSet.region ) )
        
        return Projection( j_obj['shortName'], j_obj['proj4'], j_obj['conditions'] )
     
    def releaseCacheHandle(self, cacheHandle ):
        return self.asyncQuery.releaseCache( cacheHandle  )
    
