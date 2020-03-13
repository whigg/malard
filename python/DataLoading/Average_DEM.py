#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 13 14:04:40 2020

@author: jon
"""


import Grid_6_ConvertToTiff_Interp as gtt

from os import listdir
import os

#load tiled DEM
dem_path = "/data/puma/scratch/cryotempo/processeddata/greenland_nw/Final_Unc_7_MinP_1_MaxPix_8_DemDiffMad_6_Resolution_2000"

acc_file = os.path.join(dem_path,"accumulate.tif")
out_acc_file = os.path.join(dem_path,"greenland_nw_average.tif")

files = [ os.path.join(dem_path,f) for f in listdir(dem_path) if f.startswith("greenland") and f.endswith(".tif") ]

gtt.gdal_sum( files[0], files[1], out_acc_file, len(files), len(files), a_noDataValue=-32768, b_noDataValue=-32768, out_noDataValue=-32678 )
os.rename(out_acc_file, acc_file)


for f in files[2:]:
    gtt.gdal_sum( f, acc_file, out_acc_file, len(files), 1, a_noDataValue=-32768, b_noDataValue=-32768, out_noDataValue=0 )
    os.rename(out_acc_file, acc_file)