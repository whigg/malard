

import json


class DataLoad:
    def __init__(self, config):

        self._loadData = config["loadData"]
        print("LoadData {}".format(self._loadData))
        self._tarFile = config["tarFile"]
        print("Tar File {}".format(self._tarFile))
        self._path = config["path"]
        print("Path {}".format(self._tarFile))
        self._loader = config["loader"]

    @property
    def loadData(self):
        return self._loadData
    @property
    def tarFile(self):
        return self._tarFile
    @property
    def path(self):
        return self._path
    @property
    def loader(self):
        return self._loader

class Gridding:
    def __init__(self, config):
        self._runGridding = config["runGridding"]
        self._includeEsaPoca = config["includeEsaPoca"]
        self._interpolationPixels = config["interpolationPixels"]
        self._medianFilterIterations = config["medianFilterIterations"]
        self._resolution = config["resolution"]

    @property
    def runGridding(self):
        return self._runGridding

    @property
    def includeEsaPoca(self):
        return self._includeEsaPoca

    @property
    def interpolationPixels(self):
        return self._interpolationPixels

    @property
    def medianFilterIterations(self):
        return self._medianFilterIterations

    @property
    def resolution(self):
        return self._resolution

class Request:
    def __init__(self, requestFile):
        with open(requestFile,"r") as rf:
            request = rf.read()
            obj = json.loads(request)

            self._region = obj["region"]
            self._parentDataSet = obj["parentDataSet"]
            self._dataSetSwath = obj["dataSetSwath"]
            self._pocaDataSet = obj["pocaDataSet"]
            self._pocaParentDataSet = obj["pocaParentDataSet"]
            self._esaPocaDataSet = obj["esaPocaDataSet"]
            self._esaPocaDataSet_demDiff = obj["esaPocaDataSet_demDiff"]
            self._monthsAndYears = obj["monthsAndYears"]
            self._extent = obj["extent"]
            self._uncertaintyThreshold = obj["uncertaintyThreshold"]
            self._powerdB = obj["powerdB"]
            self._demDiffMad = obj["demDiffMad"]
            self._coh = obj["coh"]
            self._resultBasePath = obj["resultBasePath"]
            self._dataLoad = DataLoad(obj["dataLoad"])
            self._esaPoca = DataLoad(obj["esaPoca"])
            self._pocaDemDiff = obj["pocaDemDiff"]
            self._gridding = Gridding(obj["gridding"])

            #mandatory fields
            self._applyUncertainty = obj["applyUncertainty"]
            self._generateEsaPointProduct = obj["generateEsaPointProduct"]
            self._generateEsaGriddedProduct = obj["generateEsaGriddedProduct"]
            self._malardEnvironment = obj["malardEnvironment"]
            self._numLoadProcesses = obj["numLoadProcesses"]
            self._numGriddingProcesses = obj["numGriddingProcesses"]


    @property
    def region(self):
        return self._region

    @property
    def parentDataSet(self):
        return self._parentDataSet

    @property
    def dataSetSwath(self):
        return self._dataSetSwath

    @property
    def pocaDataSet(self):
        return self._pocaDataSet

    @property
    def pocaParentDataSet(self):
        return self._pocaParentDataSet

    @property
    def esaPocaDataSet(self):
        return self._esaPocaDataSet

    @property
    def esaPocaDataSet_demDiff(self):
        return self._esaPocaDataSet_demDiff

    @property
    def gridding(self):
        return self._gridding

    @property
    def monthsAndYears(self):
        return self._monthsAndYears

    @property
    def extent(self):
        return self._extent

    @property
    def uncertaintyThreshold(self):
        return self._uncertaintyThreshold

    @property
    def powerdB(self):
        return self._powerdB

    @property
    def demDiffMad(self):
        return self._demDiffMad

    @property
    def coh(self):
        return self._coh

    @property
    def resultBasePath(self):
        return self._resultBasePath

    @property
    def malardEnvironment(self):
        return self._malardEnvironment

    @property
    def applyUncertainty(self):
        return self._applyUncertainty

    @property
    def generateEsaPointProduct(self):
        return self._generateEsaPointProduct

    @property
    def generateEsaGriddedProduct(self):
        return self._generateEsaGriddedProduct

    @property
    def dataLoadConfig(self):
        return self._dataLoad

    @property
    def esaPocaConfig(self):
        return self._esaPoca

    @property
    def pocaDemDiff(self):
        return self._pocaDemDiff

    @property
    def numLoadProcesses(self):
        return self._numLoadProcesses

    @property
    def numGriddingProcesses(self):
        return self._numGriddingProcesses



if __name__ == "__main__":

    request = Request( r"G:\Shared drives\team\users\jon\request.json" )

    print(request)