import json
import requests
import datetime

def dateconverter(o):
    if isinstance(o, datetime.datetime):
        timestamp = datetime.datetime.timestamp(o)
        return timestamp

class DataSetQuery:
    def __init__(self, serverUrl, envName = "DEVv2" ):
        self.serverUrl = serverUrl
        self.envName = envName
        self.headers = {'Content-Type':'application/json'}
    def createEnvironment(self, name, cacheCdfPath, maskPublisherPath, pointCdfPath, mongoConnection, swathIntermediatePath, deflateLevel = 1, serverVersion = 'v3' ):
        data = { 'name': name
                , 'cacheCdfPath': cacheCdfPath
                , 'maskPublisherPath': maskPublisherPath
                , 'pointCdfPath': pointCdfPath
                , 'mongoConnection' : mongoConnection
                , 'swathIntermediatePath' : swathIntermediatePath
                , 'deflateLevel' : deflateLevel
                , 'serverVersion' : serverVersion}

        jsonStr = json.dumps(data)
        setUrl = self.serverUrl + '/env/createenvironment/' + name
        response = requests.post(setUrl, data=jsonStr, headers=self.headers)
        return response.text
    def getEnvironment(self, name):
        getUrl = self.serverUrl + '/env/getenvironment/' + name
        response = requests.get(getUrl, headers=self.headers)
        return response.text
    def getParentDataSets(self):
        dsUrl = self.serverUrl + '/api/parentdatasets/' + self.envName
        response = requests.get(dsUrl, headers=self.headers)
        return response.text
    def getDataSets(self, parentName):
        dsUrl = self.serverUrl + '/api/datasets/' + self.envName + '/' + parentName
        response = requests.get(dsUrl, headers=self.headers)
        return response.text
    def getDataSetBoundingBox(self, parentDsName, dsName, region):
        dsUrl = self.serverUrl + '/api/boundingbox/' + self.envName + '/' + parentDsName + '/' + dsName + '/' + region
        response = requests.get(dsUrl, headers=self.headers)
        return response.text
    def getGridCells(self, parentDsName, dsName, region, minX, maxX, minY, maxY, minT, maxT, xCol='x', yCol='y' ):
        gcUrl = self.serverUrl + '/api/boundingbox/' + self.envName + '/' + parentDsName + '/' + dsName+ '/' + region
        bbox = { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT, 'xCol':xCol, 'yCol': yCol, 'maskFilters' : [] }
        jsonStr = json.dumps(bbox,default=dateconverter)
        response = requests.post(gcUrl, data=jsonStr, headers=self.headers)
        return response.text
    #similar to getGridCells except a result for each time slice is also returned.
    def getShards(self, parentDsName, dsName, region, minX, maxX, minY, maxY, minT, maxT, xCol='x', yCol='y' ):
        gcUrl = self.serverUrl + '/api/shards/' + self.envName + '/' + parentDsName + '/' + dsName + '/' + region
        bbox = { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT, 'xCol':xCol, 'yCol': yCol, 'maskFilters' : [] }
        jsonStr = json.dumps(bbox,default=dateconverter)
        response = requests.post(gcUrl, data=jsonStr, headers=self.headers)
        return response.text
    def getSwathDetailsFromId(self, parentDsName, dsName, region, swathId):
        url = self.serverUrl + '/api/swathdetailsfromid/' + self.envName + '/' + parentDsName + '/' + dsName + '/' + region + '/' + str(swathId)
        response = requests.get(url, headers=self.headers)
        return response.text
    def getSwathDetailsFromName(self, parentDsName, dsName, region, name):
        url = self.serverUrl + '/api/swathdetailsfromname/' + self.envName + '/' + parentDsName + '/' + dsName + '/' + region + '/' + name
        response = requests.get(url, headers=self.headers)
        return response.text
    def getSwathDetails(self, parentDsName, dsName, region):
        url = self.serverUrl + '/api/swathdetails/' + self.envName + '/' + parentDsName + '/' + dsName + '/' + region
        response = requests.get(url, headers=self.headers)
        return response.text
    def publishMask(self, sourcePath, fileName, parentDsName, dataSet, maskType, region, minX, minY, size ):
        url = self.serverUrl + '/mask/publishmask/' + self.envName + "/" + parentDsName + '/' + dataSet +'/' + maskType + '/' + region
        request = { 'sourceFilePath' : sourcePath, 'fileName':fileName, 'gridCell' : { 'minX':minX, 'minY':minY, 'size': size } }
        j = json.dumps(request)
        response = requests.post(url,data=j, headers=self.headers)
        return response.text
    def getMasks(self, parentDsName ):
        url = self.serverUrl + '/mask/gridmasks/' + self.envName + "/" + parentDsName
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
        url = self.serverUrl + '/gridcellstats/publishgridcellstats/'+ self.envName + '/' + parentDsName + '/' + runName
        request = { 'gridCell' : { 'minX':minX, 'minY':minY, 'size': size }, 'statistics' : statistics }
        j = json.dumps(request)
        response = requests.post(url,data=j, headers=self.headers)
        return response.text
    def getAvailableRunStatistics(self, parentDsName ):
        url = self.serverUrl + '/gridcellstats/getavailablestatistics/' + self.envName + '/' + parentDsName
        response = requests.get(url, headers=self.headers)
        return response.text
    def getRunStatistics(self, parentDsName, runName ):
        url = self.serverUrl + '/gridcellstats/getrunstatistics/' + self.envName + '/' + parentDsName + '/' + runName
        response = requests.get(url, headers=self.headers)
        return response.text
    def getGridCellStatistics(self, parentdataset, runName, minX, minY, size):
        url = self.serverUrl + '/gridcellstats/getgridcellstatistics/' + self.envName + '/' + parentdataset + '/' + runName
        request = { 'minX':minX, 'minY':minY, 'size': size }
        j = json.dumps(request)
        response = requests.post(url, data=j, headers=self.headers)
        return response.text
    def getProjectionFromShortName(self, shortName ):
        url = self.serverUrl + '/projection/getprojectionfromshortname/' + self.envName + '/' + shortName
        response = requests.get(url, headers=self.headers)
        return response.text
    def getProjection(self, parentDataSetName, region ):
        url = self.serverUrl + '/projection/getprojection/' + self.envName + '/' + parentDataSetName + '/' + region
        response = requests.get(url, headers=self.headers)
        return response.text
    def publishProjection(self, shortName, proj4, conditions):
        url = self.serverUrl + '/projection/publishprojection/' + self.envName
        request = { 'shortName':shortName, 'proj4':proj4, 'conditions' : conditions }
        j = json.dumps(request)
        response = requests.post(url, data=j, headers=self.headers)
        return response.text
    def publishProjectionRegionMapping(self, parentDataSetName, region, shortName ):
        url = self.serverUrl + '/projection/publishregionmapping/' + self.envName
        request = { 'parentDataSetName': parentDataSetName, 'region':region, 'shortName':shortName }
        j = json.dumps(request)
        response = requests.post(url, data=j, headers=self.headers)
        return response.text
    def validateDataFiles(self, inputDir, startsWith, endsWith, columns ):
        url = self.serverUrl + '/validation/validate'
        request = { 'dir' : inputDir, 'startsWith' : startsWith, 'endsWith' : endsWith, 'expectedColumns' : columns }
        j = json.dumps(request)
        response = requests.post(url, data=j, headers=self.headers)
        return response.text
