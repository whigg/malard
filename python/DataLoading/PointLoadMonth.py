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
import PointProcessor as pp
import DataSetLoader_poca_d as dslpd
import DataSetLoader_poca_c as dslpc

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
    
    loadEsaPoca = loadConfig["loadEsaPoca"] 
    
    generatePointProduct = loadConfig["generatePointProduct"]
    
    print("Running for month=[{month}] and year=[{year}]".format(month=month, year=year))
    
    if loadData:
        dsl.main(month,year,loadConfig)

        dslp.main(month,year,loadConfig)
    
    if loadEsaPoca:
        loader =  loadConfig["pocaDataSetLoader"] if "pocaDataSetLoader" in loadConfig else None
        if  loader is None or loader == "DataSetLoader_poca_d":
            dslpd.main(month,year,loadConfig )
        elif loader == "DataSetLoader_poca_c":
            dslpc.main(month, year, loadConfig)
        else:
            raise ValueError("Unsupported loader {}".format(loader))

    if applyUncertainty:
        ppp.main( month, year, loadConfig, maxDemDiffMad=loadConfig["demDiffMad"], newMaxDemDiffMad=None, adjustWaveform=False)

    if generatePointProduct:
        pp.main( month, year, loadConfig)