import json
import requests

class DataSetQuery:
    def __init__(self, serverUrl ):
        self.serverUrl = serverUrl
        self.headers = {'Content-Type':'application/json'}
    def setEnvironment(self, name, outputCdfPath):
        data = { 'name': name, 'outputCdfPath': outputCdfPath }
        jsonStr = json.dumps(data)
        setUrl = self.serverUrl + '/env/setenvironment'
        response = requests.post(setUrl, data=jsonStr, headers=self.headers)
        return response.text 
    def getEnvironment(self):
        getUrl = self.serverUrl + '/env/getenvironment'
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
        jsonStr = json.dumps(bbox)
        response = requests.post(gcUrl, data=jsonStr, headers=self.headers)
        return response.text
    #similar to getGridCells except a result for each time slice is also returned.
    def getShards(self, parentDsName, dsName, minX, maxX, minY, maxY, minT, maxT):
        gcUrl = self.serverUrl + '/api/shards/' + parentDsName + '/' + dsName
        bbox = { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT }
        jsonStr = json.dumps(bbox)
        response = requests.post(gcUrl, data=jsonStr, headers=self.headers)
        return response.text
    def getNetCdfFile(self, parentDsName, dsName, minX, maxX, minY, maxY, minT, maxT):
        gcUrl = self.serverUrl + '/point/netcdffile/' + parentDsName + '/' + dsName
        bbox = { 'minX':minX, 'maxX':maxX, 'minY':minY, 'maxY':maxY, 'minT':minT,'maxT':maxT }
        jsonStr = json.dumps(bbox)
        response = requests.post(gcUrl, data=jsonStr, headers=self.headers)
        return response.text