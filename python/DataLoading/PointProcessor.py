#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 15:47:16 2019

@author: jon
"""

import netCDF4 as n
from datetime import datetime
from dateutil.relativedelta import relativedelta
from datetime import timedelta

from MalardClient.MalardClient import MalardClient
from MalardClient.DataSet import DataSet
from MalardClient.BoundingBox import BoundingBox

import ShapeFileIndex as s

import os
import Header as h
import numpy as np

import pandas as pd

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def formatXYStr( x ):

    strXY = "+{}".format(x) if x >=0 else str(x)
    
    padding = ["_" for i in range(0, 9 - len(strXY)) ]
    
    return "{}{}".format(strXY, "".join(padding))

def writePointProduct(output_path, dataSet, bbox, data, proj4, swathIds, index ):
    
    yearmonth = bbox.minT.strftime("%Y_%m")
    yearmonthpath = yearmonth.replace("_","/")
    fileNameExt = ".nc"
    filetype = "THEM_POINT"
    fileName = "CS_OFFL_{}_{}_{}_{}_{}_V001".format( filetype, dataSet.region.upper(), yearmonth, formatXYStr(bbox.minX), formatXYStr(bbox.minY) )
    productPath = "{}/{}{}".format( yearmonthpath,fileName, fileNameExt )#"y{}/m{}/cell_{}_{}/{}{}".format(bbox.minT.year, bbox.minT.month, formatXYStr(bbox.minX), formatXYStr(bbox.minY), fileName, fileNameExt)
    fullPath = os.path.join(output_path, productPath)
    headerPath = os.path.join(output_path, "{}/{}.HDR".format( yearmonthpath, fileName )) #"{}/y{}/m{}/cell_{}_{}/{}.HDR".format(output_path, bbox.minT.year, bbox.minT.month, formatXYStr(bbox.minX), formatXYStr(bbox.minY), fileName)

    fileDescription = "L3 Point thematic product containing swath data generated from CryoSat2 SARIN data."    
    
    index.addGridCell(bbox, productPath )
    
    ensure_dir(fullPath)
    
    dataset = n.Dataset(fullPath,'w',format='NETCDF4')
            
    row = dataset.createDimension('row')
    
    minX = data['x'].min() 
    maxX = data['x'].max()
    minY = data['y'].min() 
    maxY = data['y'].max()
    minT = datetime.fromtimestamp(data['time'].min()).isoformat()
    maxT = datetime.fromtimestamp(data['time'].max()).isoformat()
    
    dataset.cdm_data_type = "Point"                 
    dataset.Conventions = "CF-1.7"
    dataset.Metadata_Conventions = "Unidata Dataset Discovery v1.0"                
    dataset.comment = "Point file containing elevation estimates"                 
    dataset.contact = "support@cryotempo.org"               
    dataset.creator_email = "support@cryotempo.org"                 
    dataset.creator_url = "http://www.earthwave.co.uk"                  
    dataset.date_created = datetime.now().isoformat()                                
    dataset.date_modified = datetime.now().isoformat()
    dataset.DOI = "10.5270/CR2-2xs4q4l"
    #dataset.external_dem = "DEM"        
    dataset.geospatial_y_min = minY                 
    dataset.geospatial_y_max = maxY         
    dataset.geospatial_x_min = minX                
    dataset.geospatial_x_max = maxX                  
    dataset.geospatial_y_units = "metres" 
    dataset.geospatial_x_units = "metres"
    dataset.geospatial_projection = proj4   
    dataset.institution = "ESA, UoE, Earthwave, isardSAT "                 
    dataset.keywords = "Land Ice > Elevation Model  > Elevation Points > Swath Processing > CryoSat2 "                 
    dataset.keywords_vocabulary = "NetCDF Climate and Forecast Standard Names "                 
    dataset.platform = " Cryosat-2"
    dataset.processing_level = "L3"
    dataset.product_version = "1.0"
    dataset.project = "CryoTEMPO which is an evolution of CryoSat+ CryoTop "                 
    dataset.references = "CryoSat-2 swath interferometric altimetry for mapping ice elevation and elevation change, In Advances in Space Research, (2017), ISSN 0273-1177, https://doi.org/10.1016/j.asr.2017.11.014"
    dataset.source = "Swath data generated from CryoSat-2 SARIn data."
    dataset.version = 1
    dataset.summary = "Land Ice Elevation Thematic Point Product." 
    dataset.time_coverage_duration = "P1M"
    dataset.time_coverage_start = minT 
    dataset.time_coverage_end = maxT
    dataset.title = "Land Ice Elevation Thematic Point Product."
    swathIdsList = [ "{} : {}".format(fileid, file) for file, fileid in swathIds.items() ] 
    dataset.fileids = np.array(swathIdsList)

    
    # Create coordinate variables for 4-dimensions
    times = dataset.createVariable('time', np.int32, ('row',))
    times.long_name = "Measurement of time"
    times.units = "Seconds from 1970 in the UTC timezone."
    times.short_name = "time"
    xs = dataset.createVariable('x', np.float32, ('row',))
    xs.long_name = "Distance in horizontal direction on the Earth’s surface."
    xs.units = "metres"
    xs.standard_name = "x"
    ys = dataset.createVariable('y', np.float32, ('row',))
    ys.long_name = "Distance in vertical direction on the Earth’s surface."
    ys.units = "metres"
    ys.standard_name = "y"
    elevations = dataset.createVariable('elevation', np.float32, ('row',))
    elevations.units = "metres"
    elevations.long_name = "Elevation estimate for a point in space and time" 
    elevations.coordinates = "x y"
    uncertainties = dataset.createVariable('uncertainty', np.float32, ('row',))
    uncertainties.units = "metres"
    uncertainties.long_name = "Uncertainty estimate for a point in space and time" 
    uncertainties.coordinates = "x y"
    pocaswath = dataset.createVariable('pocaswath', "S5" , ('row',))
    pocaswath.units = "poca or swath" ; 
    pocaswath.long_name = "Indicates whether the point is from the Swath processing or is a POCA input" ; 
    pocaswath.coordinates = "x y" ;
    inputfileid = dataset.createVariable('inputfileid', np.int32, ('row',))
    inputfileid.units = "Numeric Id" ; 
    inputfileid.long_name = "Numeric id that uses the Variable Attribute as a String Lookup" ; 
    inputfileid.coordinates = "x y" ;
    
    times[:] = np.array(data['time'])
    xs[:] = np.array(data['x'])
    ys[:] = np.array(data['y'])
    elevations[:] = np.array(data['elev'])
    uncertainties[:] = np.array(data['Q_uStd'])
    pocaswath[:] = np.array(data['swathPoca'],"S5")
    inputfileid[:] = np.array(data['swathFileId'])
    dataset.close()

    size = os.stat(fullPath).st_size
    
    attributes = { "File_Description" : fileDescription, "File_Type": filetype,"File_Name" : fileName, "Validity_Start" : minT, "Validity_Stop" : maxT, "Creator" : "Earthwave", "Creator_Version" : 0.1, "Tot_size" : size,"Projection": proj4, "Min_X":minX, "Max_X":maxX,"Min_Y":minY,"Max_Y":maxY } 

    with open( headerPath, "wt", encoding="utf8"  ) as f:
        f.write( h.createHeader( attributes, source_files = swathIds ))

def main( pub_month, pub_year, loadConfig ):

    region = loadConfig["region"]
    parentDataSet = loadConfig["parentDataSet"]
    uncertainty_threshold = loadConfig["uncertainty_threshold"] if "uncertainty_threshold" in loadConfig else None
    powerdB = loadConfig["powerdB"]
    coh = loadConfig["coh"]
    dataSetName = loadConfig["resultsetName"]

    pocaParentDataSet = loadConfig["pocaParentDataSet"]
    pocaDataSetName = loadConfig["pocaDataSet"]
    pocaDemDiff = loadConfig["pocaDemDiff"]
    output_path = os.path.join( loadConfig["resultPath"], "pointProduct")
    ensure_dir(output_path)

    malardEnv = loadConfig["MalardEnvironment"]

    client = MalardClient( malardEnv )

    uncDatasetName = "{}_unc".format(dataSetName) if uncertainty_threshold is not None else dataSetName
    uncDataSet = DataSet( parentDataSet, uncDatasetName, region)
    dataSet = DataSet( parentDataSet, dataSetName, region)
    
    pocaDataSet = DataSet(pocaParentDataSet, pocaDataSetName, region )
    pocaDataSet_noDemDiff = DataSet(pocaParentDataSet, pocaDataSetName.replace("_demDiff",""), region )

    projections = ['x','y','time','elev','powerdB','coh','demDiff','demDiffMad','swathFileId','Q_uStd']
    filters =  [{'column':'Q_uStd','op':'lte','threshold':uncertainty_threshold},{'column':'powerdB','op':'gte','threshold':powerdB},{'column':'coh','op':'gte','threshold':coh},{'column':'inRegionMask','op':'eq','threshold':1.0}]
    filters_poca = [{"column":"demDiff","op":"lte","threshold":pocaDemDiff}, {"column":"demDiff","op":"gte","threshold":-pocaDemDiff},{'column':'inRegionMask','op':'eq','threshold':1.0}]

    from_dt = datetime(pub_year, pub_month, 1,0,0,0)
    to_dt = from_dt + relativedelta(months=1) - timedelta(seconds=1)

    bb = client.boundingBox( uncDataSet )
    gridcells = client.gridCells(uncDataSet, BoundingBox(bb.minX, bb.maxX, bb.minY, bb.maxY, from_dt, to_dt))
    
    proj4 = client.getProjection(uncDataSet).proj4
    
    print("Number of Gridcells found to process {}".format(len(gridcells)))
    process_start = datetime.now()
    
    print("MinT={} MaxT={}".format(from_dt, to_dt))
    #Create a shapefile index for each month
    index = s.ShapeFileIndex(output_path, "THEM_POINT", proj4, uncDataSet.region, from_dt )
    
    for i, gc in enumerate(gridcells):
        gc_start = datetime.now()
        month_gc = BoundingBox(gc.minX, gc.maxX, gc.minY, gc.maxY, from_dt, to_dt)
        queryInfo = client.executeQuery(uncDataSet, month_gc, projections=projections, filters=filters)
       
        if queryInfo.status == "Success" and not queryInfo.resultFileName.startswith("Error") :
            
            data = queryInfo.to_df

            dataSwathStr = np.array( len(data), "S5"  )
            dataSwathStr.fill("swath")
            data["swathPoca"] = dataSwathStr
            swath_file_ids = data['swathFileId'].unique()
            pocaInfo = client.executeQuery( pocaDataSet, gc, filters=filters_poca  )
            
            pocaDf = pd.DataFrame()
            if pocaInfo.status == "Success" and not pocaInfo.resultFileName.startswith("Error"):
                pocaDf = pocaInfo.to_df
                
                if len(pocaDf) > 0:
                    pocaStr = np.empty(len(pocaDf), "S5")
                    pocaStr.fill( "poca" )
                    pocaDf["swathPoca"] = pocaStr
                    poca_file_ids = pocaDf['swathFileId'].unique()
                    print( "Poca points to include {}".format(len(pocaDf)))
                        
                    data = pd.concat([data, pocaDf], sort=False)

            print("Found {} data rows".format(len(data)))
            if len(data) > 0:
                results = client.getSwathNamesFromIds( dataSet, swath_file_ids )
                if len(pocaDf) > 0:
                    try: 
                        results.update(client.getSwathNamesFromIds(pocaDataSet_noDemDiff , poca_file_ids ))
                    except KeyError as ex:
                        print("Exception caught while retrieving swathIds for data set {} file ids {}".format(pocaDataSet_noDemDiff, poca_file_ids))
                        raise KeyError(ex)
                    
                writePointProduct(output_path, dataSet, month_gc, data, proj4, results, index )
                
            client.releaseCacheHandle(pocaInfo.resultFileName)
        else:
            print("Grid Cells skipped X=[{}] Y=[{}] with message [{}] ".format(gc.minX, gc.minY, queryInfo.status))
        client.releaseCacheHandle(queryInfo.resultFileName)

    index.close()
    gc_elapsed = ( datetime.now() - gc_start).total_seconds()
    print('Processed [{}] grid cells. Took=[{}]s'.format(i+1, gc_elapsed ))

    process_elapsed = ( datetime.now() - process_start ).total_seconds()
    print("Took [{}s] to process".format(process_elapsed))
