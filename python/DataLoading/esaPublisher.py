#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  1 09:55:28 2020

@author: jon
"""
from os import listdir
import os

# move to /data/eagle/puma/scratch/Work/CryoTEMPO/sshkeys
# ssh-add earthwaveserveraccess
# type in passphrase 
# sftp -b batch.txt -i /data/eagle/team/shared/Work/CryoTEMPO/sshkeys/earthwaveserveraccess cryo-uoe@science-pds.cryosat.esa.int


source_path = "/data/puma/scratch/cryotempo/processeddata/greenland_oib/GrIS_OIB_PDD_100_PwrdB_-160_Coh_0.6_Unc_7_MaxPix_8_DemDiffMad_6_Res_2000Load/pointProduct/2011/04"
grid_path =  "/data/puma/scratch/cryotempo/processeddata/greenland_oib/GrIS_OIB_PDD_100_PwrdB_-160_Coh_0.6_Unc_7_MaxPix_8_DemDiffMad_6_Res_2000Load/griddedProduct/2011/04"

  

files = [ (source_path, f) for f in listdir(source_path)]
grid_files = [ (grid_path, f) for f in listdir(grid_path) ]

files += grid_files

with open("batch.txt","w") as cfg:
    for p,f in files:
        cfg.write("put {path} Inbox/_temporary_/{file}\n".format(path=os.path.join(p, f), file=f))
        cfg.write("rename Inbox/_temporary_/{file} Inbox/{file}\n".format(file=f))
    
    cfg.write("bye\n")

