#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 07:57:27 2020

@author: jon
"""


import ProcessingRequest as pr

import GriddingPreProcessor as gpp

import sys

if __name__ == "__main__":
    args = sys.argv[1:]
    
    month =int(args[0])
    year = int(args[1])
    configPath = args[2]
    
    
    processingRequest = pr.ProcessingRequest.fetchRequest(configPath)
    loadConfig = processingRequest.getConfig
    
    gpp.main(month, year, loadConfig)