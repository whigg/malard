import json
import requests
import datetime

def dateconverter(o):
    if isinstance(o, datetime.datetime):
        timestamp = datetime.datetime.timestamp(o) 
        print(timestamp)
        return timestamp

class DataSetQuery:
    def __init__(self, serverUrl, envName = "DEV" ):
        self.serverUrl = serverUrl
        self.envName = envName
        self.headers = {'Content-Type':'application/json'}
    def createEnvironment(self, name, cacheCdfPath, maskPublisherPath, pointCdfPath ):
        data = { 'name': name
                , 'cacheCdfPath': cacheCdfPath
                , 'maskPublisherPath': maskPublisherPath
                , 'pointCdfPath': pointCdfPath }

        jsonStr = json.dumps(data)
        setUrl = self.serverUrl + '/env/createenvironment/' + name 
        response = requests.post(setUrl, data=jsonStr, headers=self.headers)
        return response.text 
    def getEnvironment(self, name):
        getUrl = self.serverUrl + '/env/getenvironment/' + name
        response = requests.get(getUrl, headers=self.headers)
        return response.text
    def getParentDataSets(self):
        dsUrl = self.serverUrl + '/api/parentdatasets'
        response = requests.get(dsUrl, headers=self.headers)
        return response.text
    def getDataSets(self, parentName):
        dsUrl = self.serverUrl + '/api/datasets/' + parentName 
        response = requests.get(dsUrl, headers=self.headers)
        return response.text
    def getDataSetBoundingBox(self, parentDsName, dsName):
        dsUrl = self.serverUrl + '/api/boundingbox/' + parentDsName + '/' + dsName
        response = requests.get(dsUrl, headers=self.headers)
        return response.text
    def getGridCells(self, parentDsName, dsName, minX, maxX, minY, maxY, minT, maxT):
        gcUrl = self.serverUrl + '/api/boundingbox/' + parentDsName + '/' + dsName
        bbox = { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT }
        jsonStr = json.dumps(bbox,default=dateconverter)
        response = requests.post(gcUrl, data=jsonStr, headers=self.headers)
        return response.text
    #similar to getGridCells except a result for each time slice is also returned.
    def getShards(self, parentDsName, dsName, minX, maxX, minY, maxY, minT, maxT):
        gcUrl = self.serverUrl + '/api/shards/' + parentDsName + '/' + dsName
        bbox = { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT }
        jsonStr = json.dumps(bbox,default=dateconverter)
        response = requests.post(gcUrl, data=jsonStr, headers=self.headers)
        return response.text
    def getNetCdfFile(self, parentDsName, dsName, minX, maxX, minY, maxY, minT, maxT):
        gcUrl = self.serverUrl + '/point/netcdffile/' + self.envName + "/" + parentDsName + '/' + dsName
        bbox = { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT }
        jsonStr = json.dumps(bbox,default=dateconverter)
        response = requests.post(gcUrl, data=jsonStr, headers=self.headers)
        return response.text
    def getDataSetColumns(self, parentDsName, dsName, minX, maxX, minY, maxY, minT, maxT):
        gcUrl = self.serverUrl + '/point/datasetcolumns/' + parentDsName + '/' + dsName
        print(gcUrl)
        bbox = { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT }
        jsonStr = json.dumps(bbox,default=dateconverter)
        response = requests.post(gcUrl, data=jsonStr, headers=self.headers)
        return response.text
    def executeQuery( self, parentDsName, dsName, minX, maxX, minY, maxY, minT, maxT, projection, filters ):
        gcUrl = self.serverUrl + '/point/query/' + self.envName + "/" + parentDsName + '/' + dsName
        query = { 'bbf': { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT }, 'projection':projection,'filters': filters}
        jsonStr = json.dumps(query,default=dateconverter)
        response = requests.post(gcUrl, data=jsonStr, headers=self.headers)
        return response.text
    def releaseCache(self, handle):
        url = self.serverUrl + '/point/releasecache'
        h = { 'handle': handle }
        j = json.dumps(h)
        r = requests.post(url,data=j, headers=self.headers)
        return r.text
    def getSwathDetailsFromId(self, parentDsName, dsName, swathId):
        url = self.serverUrl + '/api/swathdetailsfromid/' + parentDsName + '/' + dsName + '/' + str(swathId)
        response = requests.get(url, headers=self.headers)
        return response.text
    def getSwathDetailsFromName(self, parentDsName, dsName, name):
        url = self.serverUrl + '/api/swathdetailsfromname/' + parentDsName + '/' + dsName + '/' + name
        response = requests.get(url, headers=self.headers)
        return response.text
    def getSwathDetails(self, parentDsName, dsName):
        url = self.serverUrl + '/api/swathdetails/' + parentDsName + '/' + dsName
        response = requests.get(url, headers=self.headers)
        return response.text
    def publishMask(self, sourcePath, fileName, parentDsName, dataSet, maskType, region, minX, minY, size ):
        url = self.serverUrl + '/mask/publishmask/' + self.envName + "/" + parentDsName + '/' + dataSet +'/' + maskType + '/' + region
        request = { 'sourceFilePath' : sourcePath, 'fileName':fileName, 'gridCell' : { 'minX':minX, 'minY':minY, 'size': size } }
        j = json.dumps(request)
        response = requests.post(url,data=j, headers=self.headers)
        return response.text
    def getMasks(self, parentDsName, dataSet ):
        url = self.serverUrl + '/mask/gridmasks/' + self.envName + "/" + parentDsName + '/' + dataSet
        response = requests.get(url, headers=self.headers)
        return response.text
    def getGridCellMasks(self, parentdataset, dataSet, maskType, region):
        url = self.serverUrl + '/mask/gridcells/'  + self.envName + "/" + parentdataset + '/' + dataSet + '/' + maskType + '/' + region
        response = requests.get(url, headers=self.headers)
        return response.text
    def getGridCellMask(self, parentdataset, dataSet, maskType, region, minX, minY, size):
        url = self.serverUrl + '/mask/gridcellmask/'  + self.envName + "/" + parentdataset + '/' + dataSet + '/' + maskType + '/' + region
        request = { 'minX':minX, 'minY':minY, 'size': size }
        j = json.dumps(request)
        response = requests.post(url, data=j, headers=self.headers)
        return response.text
    def publishGridCellStats(self, parentDsName, runName, minX, minY, size, statistics ):
        url = self.serverUrl + '/gridcellstats/publishgridcellstats/' + parentDsName + '/' + runName
        request = { 'gridCell' : { 'minX':minX, 'minY':minY, 'size': size }, 'statistics' : statistics }
        j = json.dumps(request)
        response = requests.post(url,data=j, headers=self.headers)
        return response.text
    def getAvailableRunStatistics(self, parentDsName ):
        url = self.serverUrl + '/gridcellstats/getavailablestatistics/' + parentDsName
        response = requests.get(url, headers=self.headers)
        return response.text
    def getRunStatistics(self, parentDsName, runName ):
        url = self.serverUrl + '/gridcellstats/getrunstatistics/' + parentDsName + '/' + runName
        response = requests.get(url, headers=self.headers)
        return response.text
    def getGridCellStatistics(self, parentdataset, runName, minX, minY, size):
        url = self.serverUrl + '/gridcellstats/getgridcellstatistics/' + parentdataset + '/' + runName
        request = { 'minX':minX, 'minY':minY, 'size': size }
        j = json.dumps(request)
        response = requests.post(url, data=j, headers=self.headers)
        return response.text
    def getProjection(self, shortName ):
        url = self.serverUrl + '/projection/getprojection/' + shortName
        response = requests.get(url, headers=self.headers)
        return response.text
    def publishProjection(self, shortName, proj4):
        url = self.serverUrl + '/projection/publishprojection'
        request = { 'shortName':shortName, 'proj4':proj4 }
        j = json.dumps(request)
        response = requests.post(url, data=j, headers=self.headers)
        return response.text
    def publishGridCellPoints(self, parentDataSet, dataSet, minX, minY, size, sourceFileName, projection):
        #":envName/:parent/:dsname"
        url = self.serverUrl + '/point/publishgridcellpoints/' + self.envName + '/' + parentDataSet + '/' + dataSet
        request = { 'minX':minX, 'minY':minY, 'size': size, 'fileName' : sourceFileName, 'projection':projection }
        j = json.dumps(request)
        response = requests.post(url, data=j, headers=self.headers)
        return response.text
    def validateDataFiles(self, inputDir, startsWith, endsWith, columns ):
        url = self.serverUrl + '/validation/validate'
        request = { 'dir' : inputDir, 'startsWith' : startsWith, 'endsWith' : endsWith, 'expectedColumns' : columns } 
        j = json.dumps(request)
        response = requests.post(url, data=j, headers=self.headers)
        return response.text