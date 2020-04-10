#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 10 11:00:54 2020

@author: jon
"""


import os
import sys
import json

from os import listdir

import ProcessingRequest as pr

import parallel_run_config as prc

import DataSetLoader as dsl

base_dir = '/data/snail/scratch/rawdata/DataSetLoaderBase/'

def getMonthsAndYears( files ):
    
    monthsAndYears = { dsl.monthandyear( f ) for f in files  }
    
    l_monthsyears = [(m,y) for m,y in monthsAndYears ]
            
    l_monthsyears.sort(key=dsl.index)
    print(l_monthsyears)
    return l_monthsyears

def find_swath_dir( path ):
    sub_path = listdir(path)
    if len( [ 1 for f in sub_path if f.endswith(".nc")]) == len( sub_path ):
        return path
    else:
        if len(sub_path) != 1:
            raise ValueError("No files with extension {} found".format(".nc"))
        return find_swath_dir(os.path.join(path, sub_path[0]) )
    
def main( loadData, applyUncertainty, loadEsaPoca, runGridding, generateESAPointProduct, generateESAGriddedProduct ):
    for parentDataSet in listdir( base_dir ):
        
        parentDataSetPath = os.path.join(base_dir, parentDataSet )
        for region in listdir( parentDataSetPath ):
            
            regionPath = os.path.join( parentDataSetPath, region )
            for ds in listdir( os.path.join( regionPath ) ):
                   
                dataSetPath = os.path.join( regionPath, ds  )
                
                files = listdir( dataSetPath )
                if len(files) == 1 and files[0].endswith("tgz"):
                    
                    os.chdir(dataSetPath)
                    print(os.getcwd())
                    
                    command = "tar -xvzf {}".format(files[0])
                    os.system(command)
                    
                    os.remove(files[0])
                
                #Now look for the swath dir
                swath_dir = find_swath_dir(dataSetPath)
                print(swath_dir)
                
                monthsAndYears = [(8,2019),(9,2019),(10,2019),(11,2019),(12,2019),(1,2020),(2,2020)]#getMonthsAndYears(listdir(swath_dir))
                print(monthsAndYears)
                #Load the swath and the Poca.
                ds_swath = "swath_{}".format(ds)
                ds_poca = "poca_{}".format(ds)
                demDiffMad = 6
                pocaDemDiff = 100
                resultBasePath = "/data/puma/scratch/cryotempo/processeddata/greenland_bd"
                powerdB = -160
                resolution = 2000
                uncertainty = 7
                maxPixelDist = 8
                minCoh = 0.6
                
                run = "BDFinal_{}_PDD_{}_PwrdB_{}_Coh_{}_Unc_{}_MaxPix_{}_DemDiffMad_{}_Res_{}".format(ds, pocaDemDiff, powerdB, minCoh, uncertainty, maxPixelDist, demDiffMad, resolution )
    
                output_dir = os.path.join(resultBasePath, run)
    
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)     
    
                loadConfig = {"swathDir" : swath_dir
                              ,"dataSet" : ds_swath
                              , "dataSetPoca": ds_poca
                              , "parentDataSet" : parentDataSet
                              , "resultsetName" : "{}".format(ds_swath)
                              , "region" : region
                              , "runName" : run
                              , "keepIntermediateDems" : True
                              , "minPoints" : 1
                              , "maxPixelDist" : maxPixelDist
                              , "uncertainty_threshold" : uncertainty
                              , "demDiffMad" : demDiffMad
                              , "resolution" : resolution
                              , "pocaDemDiff" :pocaDemDiff
                              , "resultPath" : output_dir
                              , "powerdB" : powerdB
                              , "coh" : minCoh
                              , "loadData" : loadData
                              , "applyUncertainty" : applyUncertainty
                              , "medianFilterIterations" : [8]
                              , 'loadEsaPoca' : loadEsaPoca
                              , "pocaPath" : '/data/snail/scratch/rawdata/greenland/poca_d'
                              , "pocaParentDataSet" : parentDataSet
                              , "pocaInputDataSet" : "poca_d_esa"
                              , "pocaDataSet" : "poca_d_esa_demDiff"
                              , "MalardEnvironment": "MALARD-PROD"
                              , "generatePointProduct": generateESAPointProduct
                              , "GridIncludePoca":True
                              , "generateESAGriddedProduct":generateESAGriddedProduct
                              , "regionMaskShpFile": "/data/puma/scratch/cryotempo/masks/greenland/icesheet_noperiph.shp"
                              , "regionLRMMaskShpFile" : "/data/puma/scratch/cryotempo/masks/greenland/LRM_Greenland.shp"}
                
                request = pr.ProcessingRequest(loadConfig)
                
                request.persistRequest()
                
                if loadData or applyUncertainty or loadEsaPoca or generateESAPointProduct:
                
                    prc.main("PointLoadMonth", 6, monthsAndYears, request )
                
                if runGridding :
                    g_monthsyears = [(6,2019),(7,2019),(8,2019),(9,2019),(10,2019),(11,2019),(12,2019),(1,2020)]
                    print(g_monthsyears)
                
                    prc.main("GriddingRunMonth", 2, g_monthsyears, request)
                    
if __name__ == "__main__":
    args = sys.argv[1:]
    
    requestFile = args[0]    
    with open( requestFile, "r" ) as f:
        fileReader = f.read()    
        requestObj = json.loads(fileReader)
        #print(requestObj)
        print(requestObj["dataLoad"]["loadData"])    
        #main(loadData, applyUncertainty, loadEsaPoca, runGridding, generateESAPointProduct, generateESAGriddedProduct )
