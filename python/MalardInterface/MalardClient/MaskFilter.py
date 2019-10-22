#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 17:51:46 2019

@author: jon
"""
class MaskFilter:
    def __init__( self, p_shapeFile="", p_wkt="" , p_includeWithin = True ):
        self._includeWithin = p_includeWithin
        self._shapeFile = p_shapeFile
        self._wkt = p_wkt
        
    @property
    def includeWithin(self):
        return self._includeWithin
    
    @property
    def shapeFile(self):
        return self._shapeFile
    
    @property
    def wkt(self):
        return self._wkt
    
    @property
    def maskdict(self):
        return { 'includeWithin' : self._includeWithin, 'shapeFile' : self._shapeFile, 'wkt':self._wkt } 