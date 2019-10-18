#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 10:34:28 2019

@author: jon
"""

class Shard:
    def __init__( self, bb, s ):
        self._boundingBox = bb
        self._shardName = s
        
    @property
    def boundingBox(self):
        return self._boundingBox
    
    @property
    def shardName(self):
        return self._shardName