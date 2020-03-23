#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import Point_1_LoadAndJoin as landj
import Point_2_CalcSlopeAndRough as slope
import Point_3_ApplyUncertainty as unc

import netCDF4

import MalardClient.MalardClient as mc

import datetime as d
import dateutil.relativedelta as rd
import os
import sys

import numpy as np
import pandas as pd

import ProcessingRequest as pr

#Filters
#minCoh = 0.5
maxDemDiff = 100
#maxDemDiffMad = 20
minPower = 100

offset = 20000

tempdir = '/data/puma/scratch/malard/tempnetcdfs'

def createNetCDF( df, filename ):        
    fullpath = os.path.join(tempdir, filename)
    ds = netCDF4.Dataset(fullpath, 'w', format='NETCDF4')
    ds.createDimension( 'row', None )
        
    for column, dtype in zip(df.columns, df.dtypes):
        col = ds.createVariable(column, dtype, ('row',))
        col[:] = np.array(df[column])
        
    ds.close()

    return fullpath

def main(month, year, loadConfig, maxDemDiffMad = None, newMaxDemDiffMad = None, adjustWaveform = True):

    malardEnv = loadConfig["MalardEnvironment"]
    client = mc.MalardClient(environmentName=malardEnv)
    
    tempDir = client.getEnvironment(malardEnv).cacheCdfPath
    print(tempDir)
    
    swath_ds = mc.DataSet(loadConfig["parentDataSet"],loadConfig["dataSet"],loadConfig["region"])
    poca_ds = mc.DataSet(loadConfig["parentDataSet"],loadConfig["dataSetPoca"],loadConfig["region"])
    
    publish_pds = loadConfig["parentDataSet"]
    resDataSet = loadConfig["resultsetName"]
    
    resDataSet = resDataSet if newMaxDemDiffMad is None else "{}_{}".format(resDataSet, newMaxDemDiffMad)
    
    
    bb_swath = client.boundingBox(swath_ds)
    print(bb_swath)    
    
    projName = client.getProjection(swath_ds).shortName
    
     
    minT = d.datetime( year, month, 1,0,0,0 )
    maxT = d.datetime( year, month, 1, 0,0,0 ) + rd.relativedelta(months=1) - d.timedelta(seconds=1)
    
    gcs = client.gridCells( swath_ds, bb_swath )
    #  x = -300k to -400k, y = -1300k to -1400k
    #gcs = [mc.BoundingBox( -400000, -300000, -1400000, -1300000, minT, maxT ) ]    
    
    print( "Number of gridcells found to process {}".format(len(gcs) ))
   
    print( "Processing window start {} end {}".format(minT,maxT) )
    
    minPowerdB = loadConfig["powerdB"]
    minCoh = loadConfig["coh"]
    
    #removed demDiffMad filter.
    filters = [{"column":"powerScaled","op":"gte","threshold":minPower}
                ,{"column":"coh","op":"gte","threshold":minCoh}
                ,{"column":"demDiff","op":"lte","threshold":maxDemDiff}
                ,{"column":"demDiff","op":"gte","threshold":-maxDemDiff}]
    
    if not swath_ds.dataSet.endswith("_d"):
        filters.append({"column":"powerdB","op":"gte","threshold":minPowerdB})
                
    if maxDemDiffMad is not None:
        print("Adding filter demDiffMad {}".format(maxDemDiffMad))
        filters.append( {"column":"demDiffMad","op":"lte","threshold":maxDemDiffMad}   )
    
    columns_swath = ['sampleNb','wf_number','x','y','lat','lon','inRegionMask','powerScaled','powerdB','coh','demDiffMad','demDiff','time','elev','swathFileId','heading']
    
    m_st = d.datetime.now()
    
    stats = {}
    
    time_load_and_join = 0
    time_slope = 0
    time_uncertainty = 0
    time_pub = 0
    
    for i,gc in enumerate(gcs):
        
        print( "starting to process grid cell [{}]".format(i) )
        
        st = d.datetime.now()
        bb = mc.BoundingBox( gc.minX, gc.maxX, gc.minY, gc.maxY, minT, maxT )
        
        df = pd.DataFrame()
        
        #try:
        st = d.datetime.now()
        
        bb_offset = mc.BoundingBox( bb.minX - offset, bb.maxX + offset, bb.minY - offset, bb.maxY + offset, bb.minT, bb.maxT  )
   
        df, status = landj.loadAndJoinToPoca(swath_ds, poca_ds, filters, columns_swath, malardEnv, bb=bb, bb_offset=bb_offset, newDemDiffMad=newMaxDemDiffMad, adjustWaveform=adjustWaveform)

        step_1 = d.datetime.now()
        time_load_and_join += ( step_1 - st ).total_seconds()
        print("Status flag {} and length joined {}".format(status, len(df)))

        if len(df) > 0 and status:
           # try:
            print("GC minX {} GC maxX {}, DF minX {} DF maxX {}".format(bb_offset.minX, bb_offset.maxX, df["x"].min(), df["x"].max()))
            print("GC minY {} GC maxY {}, DF minY {} DF maxY {}".format(bb_offset.minY, bb_offset.maxY, df["y"].min(), df["y"].max()))
        
            df = slope.calcSlopeAndRoughNess( df, stats, bb )
            
            t_slp = d.datetime.now()
            time_slope += (t_slp - step_1 ).total_seconds()               
            
            print("Dropping the data outside grid cell NrPts={}".format(len(df)))
            df = df[df["x"] >= bb.minX]
            df = df[df["x"] <= bb.maxX]
            df = df[df["y"] >= bb.minY]
            df = df[df["y"] <= bb.maxY]
            print("Completed dropping the data outside grid cell NrPts={}".format(len(df)))
            if len(df) > 0:
                df = unc.applyUncertainty(df)
                
                t_unc = d.datetime.now()
                time_uncertainty += (t_unc - t_slp).total_seconds()
                
                filename = "uncertainty_{parent_dataset}_{dataset}_{region}_{year}_{month}_{minx}_{miny}.nc".format( parent_dataset=swath_ds.parentDataSet, dataset=swath_ds.dataSet,region=swath_ds.region, year=year, month=month,minx=gc.minX, miny=gc.minY )
                path = createNetCDF( df, filename  )
                
                print( "About to publish filename {} for date {}.".format(filename, minT))
                
                publishStatus = client.asyncQuery.publishGridCellPoints(publish_pds, "_".join([resDataSet,"unc"]), swath_ds.region, gc.minX, gc.minY, df['time'].min(), gc.maxX-gc.minX, path, projName)
                
                print("Published results to malard {}.".format( publishStatus.message ) )
                
                time_pub += ( d.datetime.now() - t_unc ).total_seconds()
                
                os.remove(path)
                #except:
            #    print("Error occurred processing grid cell X=[{}] Y=[{}]".format(gc.minX, gc.minY ) )
    #except:
        #    print("Error loading data")
        
        del df
                    
        end = d.datetime.now()
        
        print("MinX={} MinY={} Took={}s".format(gc.minX, gc.minY, (end - st).total_seconds() ))
        print( "Completed processing grid cell [{}]".format(i) )
        
    print("Processing took : {}s".format((d.datetime.now() - m_st).total_seconds()))
    
    print("load and join took {}s, slope took {}s, uncertainty took {}s, publication took {}s".format( int(time_load_and_join), int(time_slope), int(time_uncertainty), int(time_pub)  ) )
    
    for k,v in stats.items():
        print( "Timing Name={}s Timing={}s".format(k,v) )
        
if __name__ == "__main__":
    args = sys.argv[1:]
    
    month = args[0]
    year = args[1]
    configPath = args[2]
        
    processingRequest = pr.ProcessingRequest.fetchRequest(configPath)
    loadConfig = processingRequest.getConfig
    
    print("Running for month=[{month}] and year=[{year}]".format(month=month, year=year))
    
    main(int(month),int(year), loadConfig, maxDemDiffMad=loadConfig["demDiffMad"], newMaxDemDiffMad=None, adjustWaveform=False)

