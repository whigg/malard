#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 14:44:34 2020

@author: jon
"""

import MalardClient.MalardClient as mc

from xml.etree.ElementTree import parse
from os import listdir

from shutil import copyfile

import os


year = 2011
month = "05"

#greenland extent
g_min_lon = -90 
g_max_lon = -6.8349
g_min_lat = 55
g_max_lat = 84.2163

holding_path = "cryotempo/sir_sin_l2_baseline_d/{}/{}".format(year, month)
data_target_dir = "project-cryo-tempo/data/greenland/poca_d"

client = mc.MalardClient()

env = client.getEnvironment("DEV_EXT")

holding_base = env.holdingBaseDir

data_root = env.dataBaseDir

data_dir = os.path.join( data_root, data_target_dir )

h_dir = os.path.join(holding_base, holding_path)

header_files = [f for f in listdir(h_dir) if f.endswith(".HDR")]

g_count = 0
total_files = len(header_files)
count = 0

for h_f in header_files:
    with open( os.path.join( h_dir, h_f ), "rt" ) as f:
        xml_doc = parse(f) 
        root = xml_doc.getroot()
        
        start_lat_ele = root.find("Variable_Header/SPH/Product_Location/Start_Lat")
        if start_lat_ele.attrib.get("unit") == "10-6 deg":
            start_lat = int(start_lat_ele.text) * 1e-6
            start_long = int(root.findtext("Variable_Header/SPH/Product_Location/Start_Long"))* 1e-6
            stop_lat = int(root.findtext("Variable_Header/SPH/Product_Location/Stop_Lat"))* 1e-6
            stop_long = int(root.findtext("Variable_Header/SPH/Product_Location/Stop_Long")) * 1e-6
                
            if start_lat > g_min_lat and start_long > -90 and start_long < 0:
                
                file_name = "{}.nc".format(root.findtext("Fixed_Header/File_Name"))
                full_file_path = os.path.join( h_dir, file_name )
                target_path = os.path.join( data_dir, file_name )
                
                try:
                    copyfile(full_file_path, target_path)
                    print("File copy success[{}]".format(file_name))
                except:
                    print("File copy failed[{}]".format(file_name))
                    
                g_count = g_count + 1
        
    count = count + 1
    
    if count % 100 == 0:
        print( "Greenland files {} files processed {} out of  {}".format(g_count, count, total_files) )



