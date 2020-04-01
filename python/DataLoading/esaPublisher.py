#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  1 09:55:28 2020

@author: jon
"""
from os import listdir
import os

source_path = "/data/puma/scratch/cryotempo/processeddata/greenland_oib/GrIS_OIB_PDD_100_PwrdB_-160_Coh_0.6_Unc_6_MaxPix_8_DemDiffMad_6_Res_2000/pointProduct/2011/04"

files = [ f for f in listdir(source_path)]

with open("batch.txt","w") as cfg:
    for f in files:
        cfg.write("put {path} Inbox/_temporary_/{file}\n".format(path=os.path.join(source_path, f), file=f))
        cfg.write("rename Inbox/_temporary_/{file} Inbox/{file}\n".format(file=f))
    
    cfg.write("bye\n")
    