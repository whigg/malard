#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 10:47:00 2020

@author: jon
"""
import logging
import sys

import os
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)


def main( month, year ):
    
    logging.info("ESA File Watcher: year={} month={}".format(year, month))
    
    
    run_dt = datetime( year, month, 1)
    
    esa_l2_path = "esa:SIR_SIN_L2/{}".format(run_dt.strftime("%Y/%m"))

    target_path = "data-holding:cryotempo/sir_sin_l2_baseline_d/{}".format(run_dt.strftime("y%Y/m%m"))

    mount_path = "/data/eagle/data-holding/cryotempo/sir_sin_l2_baseline_d/{}".format(run_dt.strftime("y%Y/m%m"))

    if not os.path.exists( mount_path ):
        logging.info( "Creating folder {}".format( mount_path ) )
        os.makedirs( mount_path )            
    
    command = "rclone copy {} {}".format(esa_l2_path, target_path)
    
    logging.info( command )
    
    os.system(command)
    

if __name__ == "__main__":
    
    args = sys.argv[1:]

    month = int(args[0])
    year = int( args[1]) 
    
    main( month, year )    
    
    
