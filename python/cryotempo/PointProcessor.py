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
from MalardClient.MaskFilter import MaskFilter

import ShapeFileIndex as s

import sys
import os

import Header as h
 
import numpy as np

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def writePointProduct(output_path, dataSet, bbox, data, proj4, swathIds, index ):
    
    yearmonth = bbox.minT.strftime("%Y%m")
    fileNameExt = ".nc"
    filetype = "THEM_POINT"
    fileName = "CS_TEST_{}_{}_{}_{}_{}_V1".format( filetype, dataSet.region, yearmonth, bbox.minX, bbox.minY )
    productPath = "y{}/m{}/cell_{}_{}/{}{}".format(bbox.minT.year, bbox.minT.month, bbox.minX, bbox.minY, fileName, fileNameExt)
    fullPath = "{}/{}".format(output_path, productPath)
    headerPath = "{}/y{}/m{}/cell_{}_{}/{}.HDR".format(output_path, bbox.minT.year, bbox.minT.month, bbox.minX, bbox.minY, fileName)

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
    dataset.contact = "cryotempo@earthwave.co.uk"               
    dataset.creator_email = "cryotempo@earthwave.co.uk"                 
    dataset.creator_url = "http://www.earthwave.co.uk"                  
    dataset.date_created = datetime.now().isoformat()                                
    dataset.date_modified = datetime.now().isoformat() 
    #dataset.external_dem = "DEM"        
    dataset.geospatial_y_min = minY                 
    dataset.geospatial_y_max = maxY         
    dataset.geospatial_x_min = minX                
    dataset.geospatial_x_max = maxX                  
    dataset.geospatial_y_units = "metres" 
    dataset.geospatial_x_units = "metres"
    dataset.geospatial_projection = proj4   
    dataset.institution = "ESA, UoE, Earthwave, isardSAT"                 
    dataset.keywords = "Land Ice > Elevation Model  > Elevation Points > Swath Processing > CryoSat2"                 
    dataset.keywords_vocabulary = "NetCDF COARDS Climate and Forecast Standard Names"                 
    dataset.platform = " Cryosat-2"
    dataset.processing_level = "L3"
    dataset.product_version = "1.0"
    dataset.project = "CryoTEMPO which is an evolution of CryoSat+ CryoTop"                 
    dataset.references = "http://www.cryotempo.org"
    dataset.source = "Swath data generated from Cryo-Sat2 SARIN data."
    dataset.version = 1
    dataset.summary = "Land Ice Elevation Thematic Point Product" 
    dataset.time_coverage_duration = "P1M"
    dataset.time_coverage_start = minT 
    dataset.time_coverage_end = maxT
    dataset.title = "Land Ice Elevation Thematic Point Product"
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
    uncertainties[:] = np.zeros(len(data))
    swathVector = np.array(range(0,len(data)),"S5")
    swathVector.fill("swath")
    pocaswath[:] = swathVector
    inputfileid[:] = np.array(data['swathFileId'])
    dataset.close()

    size = os.stat(fullPath).st_size
    
    attributes = { "File_Description" : fileDescription, "File_Type": filetype,"File_Name" : fileName, "Validity_Start" : minT, "Validity_Stop" : maxT, "Creator" : "Earthwave", "Creator_Version" : 0.1, "Tot_size" : size,"Projection": proj4, "Min_X":minX, "Max_X":maxX,"Min_Y":minY,"Max_Y":maxY } 

    with open( headerPath, "wt", encoding="utf8"  ) as f:
        f.write( h.createHeader( attributes, source_files = swathIds ))

def main( argv ):
    
    argv = argv[1:]
    
    year = 2012
    
    processing_dates = []
    publication_dt = datetime( year, 2, 1,0,0,0)
    
    end_dt = datetime( year, 2 , 29, 23,59,59)
    
    while publication_dt <=  end_dt :
        next_publication_dt = publication_dt + relativedelta(months=1) - timedelta(seconds=1)
        processing_dates.append( ( publication_dt , next_publication_dt ) )
        publication_dt = next_publication_dt + timedelta(seconds=1)
       
    output_path = '/home/jon/data/point'
    gridCellSize = 100000
    
    client = MalardClient( notebook=True )
    dataSet = DataSet( 'cryotempo', 'GRIS_BaselineC_Q2', 'greenland')
    
    projections = ['x','y','time','elev','power','coh','demDiff','demDiffMad','swathFileId']
    filters =  [{'column':'power','op':'gte','threshold':10000},{'column':'coh','op':'gte','threshold':0.8},{'column':'demDiff','op':'lte','threshold':100.0},{'column':'demDiff','op':'gte','threshold':-100.0},{'column':'demDiffMad','op':'lte','threshold':10.0}]
    
    maskFilterIce = MaskFilter( p_shapeFile="/data/puma1/scratch/cryotempo/masks/icesheets.shp"  ) 
    maskFilterLRM = MaskFilter( p_shapeFile="/data/puma1/scratch/cryotempo/sarinmasks/LRM_Greenland.shp" , p_includeWithin=False ) 
    shapeFilters = [ maskFilterIce, maskFilterLRM ]
    
    minT = datetime(2011,2,1,0,0,0)
    maxT = datetime(2016,6,30,23,59,59) 
    gridcells = client.gridCellsWithinPolygon(dataSet, minT, maxT, extentFilter=maskFilterIce, maskFilters=[maskFilterLRM] )
    
    proj4 = client.getProjection(dataSet).proj4
    
    print("Number of Gridcells found to process {}".format(len(gridcells)))
    process_start = datetime.now()
    
    for from_dt, to_dt in processing_dates:
        print("MinT={} MaxT={}".format(from_dt, to_dt))
        #Create a shapefile index for each month
        index = s.ShapeFileIndex(output_path, "THEM_POINT", proj4, dataSet.region, from_dt )    
        for i, gc in enumerate(gridcells):
            gc_start = datetime.now()
            month_gc = BoundingBox(gc.minX, gc.maxX, gc.minY, gc.maxY, from_dt, to_dt)
            queryInfo = client.executeQuery(dataSet, month_gc, projections=projections, filters=filters, maskFilters=shapeFilters)
            
            if queryInfo.status == "Success":
                data = queryInfo.to_df
                print("Found {} data rows".format(len(data)))
                if len(data) > 0:
                    file_ids = data['swathFileId'].unique()
                    results = client.getSwathNamesFromIds( dataSet, file_ids )
                    writePointProduct(output_path, dataSet, month_gc, data, proj4, results, index )
                 
            client.releaseCacheHandle(queryInfo.resultFileName)
        
        index.close()        
        gc_elapsed = ( datetime.now() - gc_start).total_seconds() 
        print('Processed [{}] grid cells. Took=[{}]s'.format(i+1, gc_elapsed ))
         
    process_elapsed = ( datetime.now() - process_start ).total_seconds()
    print("Took [{}s] to process".format(process_elapsed))
         
if __name__ == "__main__":
    main(sys.argv)
