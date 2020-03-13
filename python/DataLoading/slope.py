#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  6 12:47:32 2020

@author: jon
"""


import Grid_6_ConvertToTiff_Interp as g

gris_dem = '/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/combined/GrIS_dem_2km.tif'

slope_dem = '/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/combined/GrIS_dem_2km_slope.tif'

g.gdal_slope(gris_dem,slope_dem,alg='ZevenbergenThorne')
                

