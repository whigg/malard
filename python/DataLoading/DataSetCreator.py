#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 10 11:00:54 2020

@author: jon
"""


import os

from os import listdir

import ProcessingRequest as pr

import parallel_run_config as prc
import GriddingPreProcessor as gpp

import DataSetLoader as dsl

base_dir = '/data/snail1/scratch/rawdata/DataSetLoaderBase/'

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
    
def main( loadData, applyUncertainty, runGridding ):
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
                
                monthsAndYears = getMonthsAndYears(listdir(swath_dir))
                print(monthsAndYears)
                #Load the swath and the Poca.
                ds_swath = "swath_c_{}".format(ds)
                ds_poca = "poca_c_{}".format(ds)
                demDiffMad = 6
                pocaDemDiff = 100
                resultBasePath = "/data/puma1/scratch/cryotempo/processeddata/antarctica"
                powerdB = -160
                resolution = 2000
                uncertainty = 7
                maxPixelDist = 8
                minCoh = 0.6
                
                run = "{}_PDD_{}_PwrdB_{}_Coh_{}_Unc_{}_MaxPix_{}_DemDiffMad_{}_Res_{}".format(ds, pocaDemDiff, powerdB, minCoh, uncertainty, maxPixelDist, demDiffMad, resolution )
    
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
                              #, "uncertainty_threshold" : uncertainty
                              , "demDiffMad" : demDiffMad
                              , "resolution" : resolution
                              , "pocaDemDiff" :pocaDemDiff
                              , "resultPath" : output_dir
                              , "powerdB" : powerdB
                              , "coh" : minCoh
                              , "loadData" : loadData
                              , "applyUncertainty" : applyUncertainty
                              , "medianFilterIterations" : 4 
                              , "pocaParentDataSet" : parentDataSet
                              , "pocaDataSet" : "ThwaitesBergPOCA"
                              , "MalardEnvironment": "MALARD-PROD"
                              , "generatePointProduct": False}
                
                request = pr.ProcessingRequest(loadConfig)
                
                request.persistRequest()
                
                prc.main("PointLoadMonth", 6, monthsAndYears, request )
                
                if runGridding :
                    g_monthsyears = monthsAndYears[1:-1]
                    print(g_monthsyears)
                
                    for m,y in g_monthsyears:
                        gpp.main( m, y, loadConfig )
                    
if __name__ == "__main__":
    loadData = False
    applyUncertainty = False
    generatePointProduct = False
    runGridding = True
    
    main(loadData, applyUncertainty, runGridding )