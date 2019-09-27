# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from math import floor


import numpy as np 

gridCellSize = 100000
resolution = 500

fillValue = -2147483647


r = range(0, floor(gridCellSize / resolution) + 1 )

startX = 200000

xcoords = [ (i, i * resolution + startX) for i in r]

#print( xcoords )

def index( point, startX, resolution ):
    return ( point - startX ) / resolution


dataArray = np.empty( (10,10,10)  )
dataArray.fill( fillValue )

dataArray[0][0][0] = 9

print(dataArray[0][0][0])