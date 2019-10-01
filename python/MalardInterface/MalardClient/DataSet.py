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
