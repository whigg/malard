#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from datetime import timedelta

import MalardClient.MalardClient as mc

import Gridding_1_noDataValues as grid
import Grid_6_ConvertToTiff_Interp as grid_tif
import medianFilter as mf

#minPoints = 1 # 2,5,10
#maxPixelDist = 6 # 2, 4                  #Number of pixels to use in the second interpolation phase.
smoothIterations=0 
#res = 2000
#uncertainty_threshold = 10.0  ## 3, 5, 7, 
inner_resolution = 200.0
maxCentreDist = 200

class TempFile:
    def __init__(self, base_path, step_name, dataset, pub_date, ext=".tif" ):
        self._name = os.path.join( base_path, "{step}_{parentds}_{ds}_{region}_{year}_{month}{ext}" .format(step=step_name
                                                                                                          , parentds=dataset.parentDataSet
                                                                                                          , ds=dataset.dataSet
                                                                                                          , region=dataset.region
                                                                                                          , year=pub_date.year
                                                                                                          , month=pub_date.month
                                                                                                          , ext=ext))
    @property
    def name(self):
        return self._name
        
    def remove(self):
        os.remove(self._name)

 
def main(pub_month, pub_year, loadConfig, notebook=False):
    
    region = loadConfig["region"]
    parentDataSet = loadConfig["parentDataSet"]
    minPoints = loadConfig["minPoints"]
    maxPixelDist = loadConfig["maxPixelDist"]
    uncertainty_threshold = loadConfig["uncertainty_threshold"]
    demDiffMad = loadConfig["demDiffMad"]
    powerdB = loadConfig["powerdB"]
    res = loadConfig["resolution"]
    keepIntermediates = loadConfig["keepIntermediateDems"]
    pocaDemDiff = loadConfig["pocaDemDiff"]
    uncResultDataSet = loadConfig["resultsetName"] 
    mask = '/data/puma/scratch/cryotempo/masks_raster/GrIS_noLRM_{}.tif'.format(res)
    gris_dem = '/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/combined/GrIS_dem_2km.tif'
    
    pub_date = datetime(pub_year, pub_month,15,0,0,0)
    window_start = pub_date - relativedelta(days=pub_date.day) + relativedelta(days=1) - relativedelta(months=1)
    window_end = window_start + relativedelta(months=3) - timedelta(seconds=1)    
    
    output_dir = loadConfig["resultPath"]
    client = mc.MalardClient("MALARD-PROD",notebook=notebook)
    
    projections = ['x','y','time','demDiff','elev_adjustment','Q_uStd','elev','coh','powerScaled','inRegionMask']
    
    #projections = ['x','y','time','powerScaled','coh','elev','inRegionMask']
    
    filters_poca = [{"column":"height_err","op":"eq","threshold":0.0}, {"column":"demDiff","op":"lte","threshold":pocaDemDiff}, {"column":"demDiff","op":"gte","threshold":-pocaDemDiff}]
    
    output_dir = loadConfig["resultPath"]
    
    
    filters = [{"column":"Q_uStd","op":"lte","threshold":uncertainty_threshold},{"column":"demDiffMad","op":"lte","threshold":demDiffMad},{"column":"powerdB","op":"gte","threshold":powerdB}]
    datasetName = "{}_unc".format(uncResultDataSet)

    swath_ds = mc.DataSet(parentDataSet,datasetName,region)
    print(swath_ds)
    bb_swath = client.boundingBox(swath_ds)
    print(bb_swath)
    
    esa_poca_ds = mc.DataSet( "cryotempo", "poca_c_nw_esa", "greenland" )
    
    extent = "{} {} {} {}".format(bb_swath.minX,bb_swath.minY,bb_swath.maxX,bb_swath.maxY)
    
    gcs = client.gridCells( swath_ds, bb_swath )
    
    proj4 = "'{}'".format(client.getProjection(swath_ds).proj4)
    
    print( "Number of gridcells found to process {}".format(len(gcs) ))
    
    idw_list = []    
    
    pre_idw = datetime.now()
    for gc in gcs:
        print( "Processing minX=[{}] maxX=[{}] minY=[{}] maxY=[{}]".format(gc.minX, gc.maxX, gc.minY, gc.maxY) )
        bb = mc.BoundingBox( gc.minX, gc.maxX, gc.minY, gc.maxY, window_start, window_end  )
        resultInfo = client.executeQuery(swath_ds, bb, projections=projections, filters=filters)
        
        print("Result of querying results Status={} Message={}".format(resultInfo.status, resultInfo.message))
        
        if resultInfo.status == "Success" and not resultInfo.resultFileName.startswith("Error") :
            
            pocaInfo = client.executeQuery( esa_poca_ds, bb, filters=filters_poca  )

            pocaDf =  pocaInfo.to_df if pocaInfo.status == "Success" and not pocaInfo.resultFileName.startswith("Error") else pd.DataFrame()           
            
            print( "Poca points to include {}".format(len(pocaDf)))
            
            df = resultInfo.to_df
            
            print("Applying elevation adjustment")
            if 'elev_adjustment' in df.columns:
                df['elev'] = df['elev'] + df['elev_adjustment']
            
            print("Found [{}] points for uncertainty [{}]".format(len(df), uncertainty_threshold))        
            print("before adding poca [{}]".format(len(df)))
            df = pd.concat( [pocaDf, df], sort=False  )
            print("after adding poca [{}]".format(len(df)))
            
            df_idw = grid.griddata(df,"elev", inner_resolution, maxCentreDist, minPoints)
            print( "GC X={} Y={} N={} MeanElev={}".format(gc.minX, gc.minY, len(df_idw), df_idw['elev'].mean()) )        
            idw_list.append( df_idw )
            
            client.releaseCacheHandle(resultInfo.resultFileName)
    

    if len(idw_list) == 0:
        print( "No data found to process" )
        return
            
    df_all_idw = pd.concat( idw_list )
    
    post_idw = datetime.now()
    
    print( "Load from Malard and IDW took {}s".format((post_idw - pre_idw).total_seconds())  )
    

    padded_df = grid.setNoData(df_all_idw, "elev", inner_resolution, -999)
        
    csv = TempFile(output_dir, "Gridded", swath_ds, pub_date, ".csv") 
    grid.saveCSV( padded_df, 6,7,"elev","NW",csv.name )
    
    post_csv = datetime.now()
    
    print( "Creation of padded intermediate file took {}s".format( (post_csv - post_idw).total_seconds() ) )
    
    inDem = TempFile(output_dir, "Gridded_2", swath_ds, pub_date)
    grid_tif.gdal_translate(csv.name,inDem.name,a_srs=proj4,a_nodata=-999)
    
    fillDem = TempFile(output_dir,"grid_3_fill", swath_ds, pub_date)
    grid_tif.gdal_fillnodata(inDem.name,fillDem.name,-999,md=maxPixelDist,si=smoothIterations)
    
    smoothDem = TempFile(output_dir,"grid_4_smooth",swath_ds, pub_date)
    outRes = "{res} {res}".format(res=res)
    
    grid_tif.gdal_warp(fillDem.name,smoothDem.name,r='cubic',tr=outRes,tap="",et=0,dstnodata=-32768, te=extent)
    
    bds = grid_tif.ras_getBounds(smoothDem.name)
    
    print("{} {} {} {}".format(bds[0],bds[1],bds[2],bds[3]))
    
    #What is going on with these flippind axis?
    
    maskDem = TempFile(output_dir, "grid_5_mask", swath_ds, pub_date )
    
    grid_tif.gdal_warp(mask,maskDem.name,r="cubic",tr=outRes,t_srs=proj4,te=extent,tap="",et=0)            
    
    dem = os.path.join( output_dir, "greenland_{year}_{month}.tif".format(year=pub_year, month=pub_date.month))
    #################
    #This output is the product - e.g. in the file dem pub_date = datetime(pub_year, pub_month,15,0,0,0)
    #This outputs the dem to use - may need to change to have nodate -32768 - only at 0 for the colormaps
    grid_tif.gdal_calc(smoothDem.name,maskDem.name,dem,"A*(B>0)-32768*(B<1)",-32768)
    
    grisDem = TempFile(output_dir, "grid_6_adem", swath_ds, pub_date)
    print( grisDem.name)
    
    #raise ValueError("Working out what is going on")
    
    grid_tif.gdal_warp(gris_dem,grisDem.name,r="cubic",tr=outRes,t_srs=proj4,te=extent,tap="",et=0)            
    
    diffFilePath = dem.replace(".tif","_diff.tif")
    grid_tif.gdal_diff(dem,grisDem.name,diffFilePath,-32768,-32768,-32768)
    
    post_dem = datetime.now()
    
    medianMask = mf.applyMedianFilter(diffFilePath, loadConfig["medianFilterIterations"])
    
    #Now apply the median mask to the final dem.
    grid_tif.gdal_calc(dem,medianMask,dem.replace(".tif","_maskeddem.tif"),"A*(B>0)-32768*(B<1)",-32768)
    
    print("Dem creation took {}s".format((post_dem - post_csv).total_seconds()))
    
    print("Total run time {}s".format((post_dem - pre_idw).total_seconds()))
    
    if keepIntermediates == False:
        csv.remove()
        inDem.remove()
        fillDem.remove()
        smoothDem.remove()
        maskDem.remove()
