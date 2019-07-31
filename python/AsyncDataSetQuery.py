import asyncio
import websockets
import nest_asyncio
import datetime

import json

def dateconverter(o):
    if isinstance(o, datetime.datetime):
        timestamp = datetime.datetime.timestamp(o) 
        return timestamp
    
class QueryResultInfo:
    def __init__(self,resultFileName,status):
            self._resultFileName = resultFileName
            self._status = status;
            self._json = json.dumps({'resultFileName':resultFileName,'status':status})
            
    @property
    def resultFileName(self):
        return self._resultFileName
        
    @property
    def status(self):
        return self._status
    
    @property
    def json(self):
        return self._json
            

class AsyncDataSetQuery:    
    def __init__(self, serverUrl, envName = "DEV", notebook = True ):
            self.serverUrl = serverUrl
            self.envName = envName
            self.headers = {'Content-Type':'application/json'}
            self.notebook = notebook

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
        if self.notebook == True:
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
        
    def executeQuery( self, parentDs, dataSetName, region, minX, maxX, minY, maxY, minT, maxT, projections, filters ):             
        request ={ 'envName': self.envName, 'parentDSName': parentDs, 'dsName':dataSetName, 'region':region, 'bbf': { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT}, 'projections':projections,'filters': filters}
        
        requestJson = json.dumps(request,default=dateconverter)
           
        results = self.syncServerRequest(requestJson, '/query')     
        
        resultJson = json.loads(results[len(results)-1])
        return QueryResultInfo( resultJson['cacheName'],resultJson['status'] )
    
    def publishGridCellPoints(self, parentDataSet, dataSet, region, minX, minY, size, sourceFileName, projection):
        request = { 'envName' : self.envName, 'parentDsName' : parentDataSet, 'dsName' :dataSet, 'region' : region , 'gcps' : { 'minX':minX, 'minY':minY, 'size': size, 'fileName' : sourceFileName, 'projection':projection }}
        
        requestJson = json.dumps(request,default=dateconverter)   
        results = self.syncServerRequest(requestJson, '/publish')  
        
        return results
       