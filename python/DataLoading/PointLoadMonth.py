#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 13:01:37 2020

@author: jon
"""


import DataSetLoader as dsl
import DataSetLoader_poca as dslp
import PointPreProcessor as ppp
import ProcessingRequest as pr

import sys

if __name__ == "__main__":
    args = sys.argv[1:]
    
    month =int(args[0])
    year = int(args[1])
    configPath = args[2]
    
    
    processingRequest = pr.ProcessingRequest.fetchRequest(configPath)
    loadConfig = processingRequest.getConfig
    
    loadData = loadConfig["loadData"]
    applyUncertainty = loadConfig["applyUncertainty"]
    
    print("Running for month=[{month}] and year=[{year}]".format(month=month, year=year))
    
    if loadData:
        dsl.main(month,year,loadConfig)
    
        dslp.main(month,year,loadConfig)
    
    if applyUncertainty:
        ppp.main( month, year, loadConfig, maxDemDiffMad=loadConfig["demDiffMad"], newMaxDemDiffMad=None, adjustWaveform=False)