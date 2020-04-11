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
from shutil import copyfile

import Request as r

import ProcessingRequest as pr

import parallel_run_config as prc

import DataSetLoader as dsl

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

def getLoadPath( loadConfig, region, parentDataSet, dataSet ):

    print( "tar file {} region {} parentDataSet {} dataSet {} ".format(loadConfig.tarFile, region, parentDataSet, dataSet ) )
    if loadConfig.tarFile is None:
        return loadConfig.path
    else:
        # need to make the load path
        start_dir = os.getcwd()
        try:
            targetpath = os.path.join( loadConfig.path , "{}/{}/{}".format(parentDataSet,region,dataSet) )
            if not os.path.exists(targetpath):
                print("Making directory {}".format(targetpath))
                os.makedirs(targetpath)

            # need to copy the tar file over to local machine
            if not os.path.isfile(loadConfig.tarFile):
                raise ValueError("TarFile in load config needs to be a file {}".format( loadConfig.tarFile ))

            targetFile = os.path.join(targetpath, "{}.tgz".format(dataSet))
            copyfile( loadConfig.tarFile, targetFile )

            # need to unzip
            os.chdir(targetpath)
            print(os.getcwd())

            command = "tar -xvzf {}".format("{}.tgz".format(dataSet))
            os.system(command)

            # need to remove the copied tgz file
            os.remove(targetFile)
        finally:
            os.chdir(start_dir)
    return targetpath

def main( request ):

    dataSetPath = getLoadPath( request.dataLoadConfig, request.region, request.parentDataSet, request.dataSetSwath )
    print("Dataset path is: {}".format(dataSetPath))
    pocaDataSetPath = getLoadPath( request.esaPocaConfig, request.region, request.parentDataSet, request.pocaDataSet )
    print("Poca Dataset path is: {}".format(pocaDataSetPath))
    #Now look for the swath dir
    swath_dir = find_swath_dir(dataSetPath)

    poca_dir = find_swath_dir(pocaDataSetPath)
    print(swath_dir)

    if len(request.monthsAndYears) == 0:
        monthsAndYears = getMonthsAndYears(listdir(swath_dir))
    else:
        monthsAndYears = [ ( my["m"],my["y"] ) for my in request.monthsAndYears ]

    print(monthsAndYears)
    #Load the swath and the Poca.
    ds_swath = request.dataSetSwath
    ds_poca = request.pocaDataSet
    demDiffMad = request.demDiffMad
    pocaDemDiff = request.pocaDemDiff
    resultBasePath = request.resultBasePath
    powerdB = request.powerdB
    resolution = request.gridding.resolution
    uncertainty = request.uncertaintyThreshold
    maxPixelDist = request.gridding.interpolationPixels
    minCoh = request.coh

    run = "pocadd_{}_pwrdb_{}_coh_{}_unc_{}_pix_{}_ddm_{}_Res_{}".format(pocaDemDiff, powerdB, minCoh, uncertainty, maxPixelDist, demDiffMad, resolution )

    output_dir = os.path.join(resultBasePath, run)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    loadConfig = {  "swathDir" : swath_dir
                  , "dataSet" : ds_swath
                  , "dataSetPoca": ds_poca
                  , "parentDataSet" : request.parentDataSet
                  , "resultsetName" : "{}".format(ds_swath)
                  , "region" : request.region
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
                  , "loadData" : request.dataLoadConfig.loadData
                  , "dataSetLoader" : request.dataLoadConfig.loader
                  , "applyUncertainty" : request.applyUncertainty
                  , "medianFilterIterations" : [request.gridding.medianFilterIterations]
                  , 'loadEsaPoca' : request.esaPocaConfig.loadData
                  , 'pocaDataSetLoader' : request.esaPocaConfig.loader
                  , "pocaPath" : poca_dir
                  , "pocaParentDataSet" : request.pocaParentDataSet
                  , "pocaInputDataSet" : request.esaPocaDataSet
                  , "pocaDataSet" : request.e
                  , "MalardEnvironment": request.malardEnvironment
                  , "generatePointProduct": request.generateEsaPointProduct
                  , "GridIncludePoca": request.gridding.includeEsaPoca
                  , "generateESAGriddedProduct": request.generateEsaGriddedProduct
                  , "regionMaskShpFile": "/data/puma/scratch/cryotempo/masks/greenland/icesheet_noperiph.shp"
                  , "regionLRMMaskShpFile" : "/data/puma/scratch/cryotempo/masks/greenland/LRM_Greenland.shp"}

    runRequest = pr.ProcessingRequest(loadConfig)

    runRequest.persistRequest()

    if request.dataLoadConfig.loadData or request.applyUncertainty or request.esaPocaConfig.loadData or request.generateEsaPointProduct:
        prc.main("PointLoadMonth", 6, monthsAndYears, runRequest )

    if request.gridding.runGridding:
        print(monthsAndYears)
        prc.main("GriddingRunMonth", 2, monthsAndYears, runRequest)

if __name__ == "__main__":
    args = sys.argv[1:]
    
    requestFile = args[0]    

    main( r.Request(requestFile) )
