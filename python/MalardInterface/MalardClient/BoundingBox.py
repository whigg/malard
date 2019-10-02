class BoundingBox:
    def __init__( self, minX, maxX, minY, maxY, minT, maxT, numberOfPoints = 0):
        self._minX = minX
        self._maxX = maxX
        self._minY = minY
        self._maxY = maxY
        self._minT = minT
        self._maxT = maxT
        self._numberOfPoints = numberOfPoints
    
    def __str__(self):
        return "minX={}, maxX={}, minY={}, maxY={}, minT={}, maxT={} N={}".format(self._minX, self._maxX, self._minY, self._maxY, self._minT, self._maxT, self._numberOfPoints)
    
    @property
    def minX(self):
        return self._minX
    @property
    def maxX(self):
        return self._maxX
    @property
    def minY(self): 
        return self._minY
    @property
    def maxY(self):
        return self._maxY
    @property
    def minT(self): 
        return self._minT
    @property
    def maxT(self):
        return self._maxT
    @property
    def numberOfPoints(self):
        return self._numberOfPoints
