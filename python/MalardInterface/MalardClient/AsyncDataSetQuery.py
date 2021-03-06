import asyncio
import websockets
import nest_asyncio
import datetime

import MalardClient.MalardHelpers as mh
import json

from MalardClient.MaskFilter import MaskFilter

def dateconverter(o):
    if isinstance(o, datetime.datetime):
        timestamp = datetime.datetime.timestamp(o) 
        return timestamp
    
class QueryResultInfo:
    def __init__(self,resultFileName,status, message):
            self._resultFileName = resultFileName
            self._status = status
            self._message = message
            self._json = json.dumps({'resultFileName':resultFileName,'status':status})
            self._df = None
            
    def __str__(self):
        return "Status={} Message={} ResultFileName={}".format(self._status, self._message, self._resultFileName)
    
    @property
    def resultFileName(self):
        return self._resultFileName
        
    @property
    def status(self):
        return self._status
    
    @property
    def message(self):
        return self._message
    
    @property
    def json(self):
        return self._json
    
    @property
    def to_df(self):
        if self._df is None:
            self._df = mh.getDataFrameFromNetCDF( self.resultFileName ) 
        
        return self._df
        

class SwathPublisherInfo:
    def __init__(self, completed, inputFileName, status, message, swathDetails = None):
        self._completed = completed
        self._inputFileName = inputFileName
        self._status = status
        self._message = message
        
        if swathDetails is None:
            self._json = json.dumps({  'completed': completed
                                 , 'inputFileName': self._inputFileName
                                 , 'status' : self._status
                                 , 'message':self._message
                                 })
            self._swathDetails = None
        else:
            self._json = json.dumps({  'completed': completed
                                 , 'inputFileName': self._inputFileName
                                 , 'status' : self._status
                                 , 'message':self._message
                                 , 'swathDetails':swathDetails
                                 })
    
            self._swathDetails = swathDetails
    
    @property
    def completed(self):
        return self._completed
    
    @property
    def inputFileName(self):
        return self._inputFileName
    
    @property
    def status(self):
        return self._status
    
    @property
    def message(self):
        return self._status
    
    @property
    def swathDetails(self):
        return self._swathDetails
    
    @property
    def json(self):
        return self._json

class PublisherInfo:
    def __init__(self, completed, status, message, hashCode ):
        self._completed = completed
        self._status = status
        self._message = message
        self._hashCode = hashCode
        self._json = json.dumps( {'completed' : completed, 'status' : completed, 'message':message, 'hash' : hashCode} )
        
    @property
    def completed(self):
        return self._completed
    @property
    def status(self):
        return self._status
    @property
    def message(self):
        return self._message
    @property
    def hashCode(self):
        return self._hashCode
    @property
    def json(self):
        return self._json
        
class AsyncDataSetQuery:    
    def __init__(self, serverUrl, envName = "DEV", notebook = True ):
            self.serverUrl = serverUrl
            self.envName = envName
            self.headers = {'Content-Type':'application/json'}

            if notebook is not None:
                print("Warning notebook flag is deprecated and will be removed in a later version.")

            self.eventLoopRunning = asyncio.get_event_loop().is_running()

    async def asyncServerRequest(self, uri, request):
        async with websockets.connect(uri) as websocket:
            await websocket.send(request)
            data = []
            while True:
                try:
                    response = await websocket.recv()
                    data.append(response)
                except websockets.ConnectionClosed:
                    return data
    
    def syncServerRequest( self, requestJson, endPoint ):
        
        loop = None
        if self.eventLoopRunning == True:
            loop = asyncio.get_running_loop()
        else:
            loop = asyncio.new_event_loop()
    
        nest_asyncio.apply(loop)
        
        results = loop.run_until_complete(self.asyncServerRequest( self.serverUrl + endPoint, requestJson))
        
        return results
        
    def validateNetCdf( self, path, startsWith, endsWith, columns ):
        request = { 'dir' : path, 'startsWith' : startsWith, 'endsWith' : endsWith, 'expectedColumns' : columns } 
        requestJson = json.dumps(request)
        return self.syncServerRequest(requestJson, '/validateasync')
        
    def filterShards( self, parentDs, dataSetName, region, minX, maxX, minY, maxY, minT, maxT, xCol='x', yCol='y', extentFilter=MaskFilter(), maskFilters=[] ):
        
        filtersAsDict = [ m.maskdict for m in maskFilters ]
        extentDict = extentFilter.maskdict
        
        request ={ 'envName': self.envName, 'parentDSName': parentDs, 'dsName':dataSetName, 'region':region, 'bbf': { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT, 'xCol':xCol, 'yCol': yCol, 'extentFilter' : extentDict, 'maskFilters' : filtersAsDict}, 'projections':[],'filters': []}
        
        requestJson = json.dumps(request,default=dateconverter)
       
        return self.syncServerRequest(requestJson, '/filtershards') 

    def filterGridCells( self, parentDs, dataSetName, region, minX, maxX, minY, maxY, minT, maxT, xCol='x', yCol='y', extentFilter=MaskFilter(), maskFilters=[] ):
        
        filtersAsDict = [ m.maskdict for m in maskFilters ]
        extentDict = extentFilter.maskdict
        
        request ={ 'envName': self.envName, 'parentDSName': parentDs, 'dsName':dataSetName, 'region':region, 'bbf': { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT, 'xCol':xCol, 'yCol': yCol, 'extentFilter' : extentDict,'maskFilters' : filtersAsDict}, 'projections':[],'filters': []}
        
        requestJson = json.dumps(request,default=dateconverter)
       
        return self.syncServerRequest(requestJson, '/filtergridcells')

    def filterGriddedPoints( self,  minX, maxX, minY, maxY, maskFilters, resolution ):    
        filtersAsDict = [ m.maskdict for m in maskFilters ]
        
        request ={ 'envName': self.envName,'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'maskFilters' : filtersAsDict, 'resolution' : resolution }
        
        requestJson = json.dumps(request,default=dateconverter)
       
        return self.syncServerRequest(requestJson, '/filtergriddedpoints')                 
    
    def executeQuery( self, parentDs, dataSetName, region, minX, maxX, minY, maxY, minT, maxT, projections, filters, xCol='x', yCol='y', extentFilter=MaskFilter(), maskFilters=[] ):
        
        filtersAsDict = [ { 'includeWithin' : m.includeWithin, 'shapeFile' : m.shapeFile, 'wkt':m.wkt } for m in maskFilters ]
        extentDict = extentFilter.maskdict
        
        request ={ 'envName': self.envName, 'parentDSName': parentDs, 'dsName':dataSetName, 'region':region, 'bbf': { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT, 'xCol':xCol, 'yCol': yCol, 'extentFilter' : extentDict, 'maskFilters' : filtersAsDict}, 'projections':projections,'filters': filters}
        
        requestJson = json.dumps(request,default=dateconverter)
           
        results = self.syncServerRequest(requestJson, '/query')     
        
        resultJson = json.loads(results[len(results)-1])
        return QueryResultInfo( resultJson['cacheName'],resultJson['status'], resultJson['message'] )
    
    def publishGridCellPoints(self, parentDataSet, dataSet, region, minX, minY, minT, size, sourceFileName, projection):
        
        if isinstance(sourceFileName,str):
            sourceFileName = [sourceFileName]
            
        hashCode = hash( (self.envName, parentDataSet, dataSet, region, minX,minY,size,projection ) )
        
        request = { 'envName' : self.envName, 'parentDsName' : parentDataSet, 'dsName' :dataSet, 'region' : region , 'gcps' : { 'minX':int(minX), 'minY':int(minY),'minT':int(minT),'size': size, 'fileName' : sourceFileName, 'projection':str(projection)}, 'hash' : int(hashCode)}
        
        requestJson = json.dumps(request,default=dateconverter)   
         
        results = self.syncServerRequest(requestJson, '/publish')  
        
        results = results[len(results) - 1]
        
        if isinstance(results, str):
            results = [results]
            
        jsonresults = json.loads(results[0])
        
        return PublisherInfo( jsonresults['completed'], jsonresults['status'], jsonresults['message'],jsonresults['hash']  )
    
    def publishSwathToGridCells( self, parentDataSet, dataSet, region, inputFileName, inputFilePath, dataTime, columnFilters, includeColumns, gridCellSize, xCol = 'lon', yCol = 'lat', maskFilters = [] ):
        
        hashCode = hash((self.envName, parentDataSet, dataSet, region, inputFileName))
        
        filtersAsDict = [ { 'includeWithin' : m.includeWithin, 'shapeFile' : m.shapeFile, 'wkt':m.wkt } for m in maskFilters ]
        
        request = {'envName' : self.envName
                   , 'parentDataSet' : parentDataSet
                   , 'dataSet' : dataSet
                   , 'region' : region
                   , 'inputFileName' : inputFileName
                   , 'inputFilePath' : inputFilePath
                   , 'dataTime' : dataTime
                   , 'columnFilters' : columnFilters
                   , 'includeColumns' : includeColumns
                   , 'gridCellSize' : gridCellSize
                   , 'hash' : hashCode
                   , 'xCol' : xCol
                   , 'yCol' : yCol
                   , 'regionFilter' : filtersAsDict }
        
        requestJson = json.dumps(request,default=dateconverter)   
        
        results = self.syncServerRequest(requestJson, '/publishswath') 
        
        results = results[len(results)-1]
        
        if isinstance(results, str):
            results = [results]
            
        jsonresults = json.loads(results[0])
        
        if jsonresults['status'] == 'Error':
            print(results[0])
        
        swathDetails = None
        if 'swathDetails' in jsonresults:
            swathDetails = jsonresults['swathDetails']
        
        return SwathPublisherInfo( jsonresults['completed'], jsonresults['inputFileName'], jsonresults['status'], str(jsonresults['message']), swathDetails  )
  
    
    def releaseCache(self, handle):
       
        if isinstance(handle, str ):
            handle = [handle]
        
        d = []
        for h in handle:
            d.append( {'handle':h } )
        
        j = json.dumps(d)
        r = self.syncServerRequest(j, '/releasecache')
        return r
    