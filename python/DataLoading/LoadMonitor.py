#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 20 15:34:06 2020

@author: jon
"""

import pymongo as p
import time

client = p.MongoClient("mongodb://localhost:27018")

db = client.mtngla

swathDetails = db.swathDetails


count = 0

res = swathDetails.find({"datasetName":"tdx_mad_v3"})
count = len(list(res))
prevcount = count

print(count)

while True:
    time.sleep(60)
    res = swathDetails.find({"datasetName":"tdx_mad_v3"})
    newcount = len(list(res))
    count = newcount - prevcount
    prevcount = newcount
    print("process {} last period {}".format(newcount, count))
        
    