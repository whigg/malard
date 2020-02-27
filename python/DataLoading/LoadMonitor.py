#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 20 15:34:06 2020

@author: jon
"""

import pymongo as p
import time

def main( parentDataSet, dataSetName, expectedTotal ):
    client = p.MongoClient("mongodb://localhost:27018")

    db = client[parentDataSet]
    
    swathDetails = db.swathDetails
    
    count = 0
    
    res = swathDetails.find({"datasetName":dataSetName})
    count = len(list(res))
    prevcount = count
    
    print(count)
    
    while True:
        time.sleep(60)
        res = swathDetails.find({"datasetName":dataSetName})
        newcount = len(list(res))
        count = newcount - prevcount
        prevcount = newcount
        pct_complete = newcount / expectedTotal
        print("process {} last period {} pct_complete {:.2%}".format(newcount, count, pct_complete))
        
if __name__ == "__main__":
    main("cryotempo","poca_c_n_roll", 3000 )