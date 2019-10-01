#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 13:25:07 2019

@author: jon
"""

import MalardClient as mc

client = mc.MalardClient('DEVv2', False)

print( client.query.getEnvironment('DEVv2') )