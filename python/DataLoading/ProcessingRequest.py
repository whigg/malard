#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 08:20:02 2020

@author: jon
"""

import json
import os

class ProcessingRequest:
    def __init__(self, requestDict ):
        self._request = requestDict
       
    def getItem(self, key ):
        if key in self._request:
            return self._request[key]
        
    @property    
    def name(self):
        t = self._request
        return "{}.json".format( t["runName"] )
    
    @property
    def getConfig(self):
        return self._request.copy()
    
    @property
    def resultPath(self):
        return self._request["resultPath"]
    
    @staticmethod
    def fetchRequest( requestPath ):
        print(requestPath)
        with open(requestPath, "r") as reader:
            obj = reader.read()
            jsonObj = json.loads(obj)
        
            return ProcessingRequest(jsonObj)

    def persistRequest(self):    
        jsonObj = json.dumps(self._request, indent=4)        
        
        saveConfig = os.path.join( self._request["resultPath"], self.name )
        print("Saving request to location {}".format(saveConfig))
        with open( saveConfig, "w" ) as config:
            config.write( jsonObj )
    