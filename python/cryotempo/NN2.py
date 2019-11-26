#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 19:16:07 2019

@author: jon
"""

from MalardClient.MalardClient import MalardClient
from MalardClient.DataSet import DataSet
from MalardClient.BoundingBox import BoundingBox
from datetime import datetime
from dateutil.relativedelta import relativedelta

import numpy as np

client = MalardClient()

ds = DataSet("cryotempo","poca","greenland" )

dsSwath = DataSet("cryotempo","GRIS_BaselineC_Q2","greenland" )

ds_oib = DataSet("cryotempo","oib","greenland" )

bb = client.boundingBox(ds)

minX=-200000
maxX=-100000
minY=-2400000
maxY=-2300000
minT=datetime(2011,3,1,0,0,0)
maxT=datetime(2011,3,31,23,59,59)

bb = BoundingBox( minX, maxX, minY, maxY, minT, maxT )

resSwath = client.executeQuery( dsSwath, bb )



minT=minT - relativedelta(days=10)
maxT=maxT + relativedelta(days=10)

bb = BoundingBox( minX, maxX, minY, maxY, minT, maxT )

resOib =  client.executeQuery( ds_oib, bb )


df_oib = resOib.to_df

df_swath = resSwath.to_df

grp_times = df_swath.groupby("time")

for k,v in grp_times:
    print("Swath {} {}".format(datetime.fromtimestamp(k),len(v) ))
    
    
oib_times = df_oib.groupby("time")

for k,v in oib_times:
    print("OIB {} {}".format(datetime.fromtimestamp(k),len(v) ))
        
    
    