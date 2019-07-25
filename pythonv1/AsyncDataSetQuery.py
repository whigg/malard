import asyncio
import websockets
import nest_asyncio
import datetime

import json

def dateconverter(o):
    if isinstance(o, datetime.datetime):
        timestamp = datetime.datetime.timestamp(o) 
        print("Converter " + str(timestamp))
        return timestamp

class AsyncDataSetQuery:    
    def __init__(self, serverUrl, envName = "DEV" ):
            self.serverUrl = serverUrl
            self.envName = envName
            self.headers = {'Content-Type':'application/json'}

    async def validate(self, uri, request):
        async with websockets.connect(uri) as websocket:
            await websocket.send(request)
            i = 1
            data = []
            while True:
                try:
                    response = await websocket.recv()
                    data.append(response)
                except websockets.ConnectionClosed:
                    return data
                #print('Receiving... ' + response + ' [' + str(i) + ']')
                i = i + 1

    async def validateNetCdf( self, path, startsWith, endsWith, columns ):
        
        loop = asyncio.get_running_loop()
        nest_asyncio.apply(loop)
        # Create a new Future object.
        fut = loop.create_future()

        request = { 'dir' : path, 'startsWith' : startsWith, 'endsWith' : endsWith, 'expectedColumns' : columns } 
        j = json.dumps(request)

        fut.set_result(await self.validate(self.serverUrl + '/validateasync', j))

        return await fut

    async def query(self, uri, request):
        print(uri)
        async with websockets.connect(uri) as websocket:
            await websocket.send(request)
            i = 1
            data = []
            while True:
                try:
                    response = await asyncio.wait_for(websocket.recv(),600)
                    data.append(response)
                except websockets.ConnectionClosed:
                    return data
                print('Receiving... ' + response + ' [' + str(i) + ']')
                i = i + 1
        
    async def executeQuery( self, parentDs, dataSetName, minX, maxX, minY, maxY, minT, maxT, projections, filters ):
        
        loop = asyncio.get_running_loop()
        nest_asyncio.apply(loop)
        # Create a new Future object.
        fut = loop.create_future()

        request ={ 'envName': self.envName, 'parentDSName': parentDs, 'dsName':dataSetName ,'bbf': { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT}, 'projections':projections,'filters': filters}
        
        j = json.dumps(request,default=dateconverter)
        
        print(j)
        
        results = await asyncio.wait_for(self.query( self.serverUrl + '/query' , j),600)
        
        fut.set_result(results)

        return await fut