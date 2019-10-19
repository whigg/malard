#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 17:51:46 2019

@author: jon
"""
class MaskFilter:
    def __init__( self, p_shapeFile, p_includeWithin = True ):
        self._includeWithin = p_includeWithin
        self._shapeFile = p_shapeFile
        
    @property
    def includeWithin(self):
        return self._includeWithin
    
    @property
    def shapeFile(self):
        return self._shapeFile