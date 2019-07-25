import asyncio
import AsyncDataSetQuery
import datetime





def syncquery(query):    
    loop = asyncio.get_event_loop()
    parentDs = 'mtngla'
    dataSetName ='tandemx'
    minX=0
    maxX=200000
    minY=0
    maxY=200000
    minT=datetime.datetime(2010,7,1,0,0)
    maxT=datetime.datetime(2010,12,29,0,0)

    projections = []
    filters = []

    print(datetime.datetime.now())
    fileName = loop.run_until_complete( query.executeQuery( parentDs, dataSetName, minX, maxX, minY, maxY, minT, maxT, projections, filters ))
    print(datetime.datetime.now())
    loop.close()
    print(fileName)


environmentName = 'DEV'

query = AsyncDataSetQuery.AsyncDataSetQuery( 'ws://localhost:9000',environmentName)

localquery(query)

