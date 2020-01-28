#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 09:33:33 2019

@author: jon
"""

from MalardClient.MalardClient import MalardClient
from MalardClient.DataSet import DataSet
from MalardClient.BoundingBox import BoundingBox

client = MalardClient()

ds = DataSet("cryotempo","poca","greenland" )

dsSwath = DataSet("cryotempo","GRIS_BaselineC_Q2","greenland" )

bb = client.boundingBox(ds)

gcs = client.gridCells(ds, bb)

minX=-1600000
maxX=-1500000
minY=-2600000
maxY=-2500000
minT=1298912551
maxT=1298912551

bb = BoundingBox( minX, maxX, minY, maxY, minT, maxT )

resPoca = client.executeQuery( ds, bb )

resSwath = client.executeQuery( dsSwath, bb )