#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 14:48:05 2020

@author: jon
"""

class Environment:
    def __init__(self, name, cacheCdfPath, maskPublisherPath, pointCdfPath, mongoConnection, swathIntermediatePath, holdingBaseDir, dataBaseDir, deflateLevel, serverVersion   ):
        self._name = name
        self._cacheCdfPath = cacheCdfPath
        self._maskPublisherPath = maskPublisherPath
        self._pointCdfPath = pointCdfPath
        self._mongoConnection = mongoConnection
        self._swathIntermediatePath = swathIntermediatePath
        self._holdingBaseDir = holdingBaseDir
        self._dataBaseDir = dataBaseDir
        self._deflateLevel= deflateLevel
        self._serverVersion = serverVersion
        
    @property
    def name(self):
        return self._name
    
    @property
    def cacheCdfPath(self):
        return self._cacheCdfPath
    
    @property
    def maskPublisherPath(self):
        return self._maskPublisherPath
    
    @property
    def pointCdfPath(self):
        return self._pointCdfPath
    
    @property
    def mongoConnection(self):
        return self._mongoConnection
    
    @property
    def swathIntermediatePath(self):
        return self.swathIntermediatePath
    
    @property
    def holdingBaseDir(self):
        return self._holdingBaseDir
    
    @property
    def dataBaseDir(self):
        return self._dataBaseDir
    
    @property
    def deflateLevel(self):
        return self._deflateLevel
    
    @property
    def serverVersion(self):
        return self._serverVersion
    