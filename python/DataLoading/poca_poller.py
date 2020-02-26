#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 14:44:34 2020

@author: jon
"""

import MalardClient.MalardClient as mc

from xml.etree.ElementTree import parse
from os import listdir
from datetime import datetime
from shutil import copyfile

import os
import sys

def intersect_extent( x1, x2, y1, y2, minX, maxX, minY, maxY  ):
    
    toRight = True if x1 > maxX and x2 >maxX else False
    toLeft = True if x1 < minX and x2 < minX else False
    below = True if y1 < minY and y2 < minY else False
    above = True if y1 > maxY and y2 > maxY else False
    
    return False if toRight or toLeft or below or above else True

def main(month, year):
    #greenland extent
    g_min_lon = -88
    g_max_lon = 7
    g_min_lat = 51
    g_max_lat = 82
    
    run_date = datetime( year, month, 1 )
    
    holding_path = "cryotempo/sir_sin_l2_baseline_d/{}".format(run_date.strftime("y%Y/m%m"))
    data_target_dir = "project-cryo-tempo/data/greenland/poca_d"

    client = mc.MalardClient()

    env = client.getEnvironment("MALARD-PROD")

    holding_base = env.holdingBaseDir

    data_root = env.dataBaseDir

    data_dir = os.path.join( data_root, data_target_dir )
    
    if not os.path.exists( data_dir ):
        os.makedirs( data_dir )
    
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
                print( "Start_Lon={} Stop_Lon={} Start_Lat={} Stop_Lon={}".format(start_lat, stop_lat, start_long, stop_long))
                if intersect_extent( start_long, stop_long, start_lat, stop_lat, g_min_lon, g_max_lon, g_min_lat, g_max_lat ):
                    print( "File {} intersects with extent".format( h_f ) )
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

if __name__ == "__main__":

    args = sys.argv[1:]
    
    month = int(args[0])
    year = int(args[1])

    main( month, year )
