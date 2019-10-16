
class Projection:
    def __init__(self, shortName, proj4, conditions ):
        self._shortName = shortName
        self._proj4 = proj4
        self._conditions = conditions
        
    def __str__(self):
        return "ShortName={}, Proj4={}, Conditions={}".format(self._shortName, self._proj4, self._conditions)
        
    @property
    def shortName(self):
        return self._shortName
    
    @property
    def proj4(self):
        return self._proj4
    
    @property
    def conditions(self):
        return self._conditions
