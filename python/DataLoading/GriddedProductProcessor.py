#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 15:26:36 2019

@author: jon
"""
import netCDF4 as n

from datetime import datetime
from datetime import timezone

import Timeseries as t
import os
import Header as h
import numpy as np

from osgeo import gdal

def getPixel(x, y, gt):
    '''
    :param x: pandas df
    :param y: pandas df
    :param gt: GeoTransform
    :return:
    ''' 
    py = ((x-gt[0])/gt[1]).astype('int64')
    px = ((gt[3]-y)/-gt[5]).astype('int64')
    
    return px, py

def getArray( dem ):
    ad = gdal.Open(dem, gdal.GA_ReadOnly)
    adArray = ad.GetRasterBand(1).ReadAsArray()
    adGt = ad.GetGeoTransform()
    
    return ( adArray, adGt )    

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def createGriddedProductFromTif(dataSet, resultRaster, loadConfig, bbox, pub_date, proj4, source_fill=-32768, fillValue = -2147483647):
  
    demArray, demGt = getArray(resultRaster)
    print(demArray.shape)
    
    trDem = np.transpose(demArray)
    
    print(trDem.shape)
    m, n = trDem.shape
    
    resolution = loadConfig["resolution"]
    
    bounds = t.getExtent(resultRaster)
    startX = bounds[0]
    startY = bounds[2]
    
    print("Dem Bounds {}".format(bounds))
    
    xcoords = [ i * resolution + resolution * 0.5 + startX for i in range(0,m) ]    
    ycoords = [ i * resolution + resolution * 0.5 + startY for i in range(0,n) ]
    
    print("Xcoords {}".format(len(xcoords)))
    print("Ycoords {}".format(len(ycoords)))

    data = np.empty((m,n,1))
    data.fill(fillValue)
    
    for i,x in enumerate( xcoords ):
        for j,y in enumerate( ycoords ):
            try:
                value = demArray[ getPixel( np.array(x), np.array(y), demGt ) ]
                if value == source_fill:
                    data[i][j][0] = fillValue
                else:
                    data[i][j][0] = value
            except IndexError as ex:
                print("getPixel failed for i={} j={} X={} Y={}".format(i,j,x,y))
                raise ex
                    
    writeGriddedProduct(loadConfig["resultPath"], dataSet, bbox, xcoords, ycoords, data, resolution, proj4, pub_date, fillValue)
    
def writeGriddedProduct(output_path, dataSet, bbox,  xcoords, ycoords, data, resolution, proj4, pub_date, fill_value ):
    
    output_path = os.path.join(output_path, "griddedProduct")
    
    pubdatestr = "{}".format(pub_date.strftime("%Y_%m"))
    fileNameExt = ".nc"
    filetype = "THEM_GRID_"
    fileName = "CS_OFFL_{}_{}_{}_V001".format( filetype, dataSet.region, pubdatestr )
    productPath = "{}/{}/{}{}".format( pub_date.strftime("%Y"), pub_date.strftime("%m"),fileName, fileNameExt )
    fullPath = os.path.join(output_path, productPath)

    headerPath = os.path.join(output_path, "{}/{}/{}.HDR".format( pub_date.strftime("%Y"), pub_date.strftime("%m"), fileName ))

    fileDescription = "L3 Gridded thematic product containing interpolated swath data that is generated from CryoSat2 SARIN data."    

    ensure_dir(fullPath)
    ensure_dir(headerPath)
    
    dataset = n.Dataset(fullPath,'w',format='NETCDF4')
    
    minX = bbox.minX
    maxX = bbox.maxX
    minY = bbox.minY 
    maxY = bbox.maxY
    minT = bbox.minT.isoformat()
    maxT = bbox.maxT.isoformat()
    
    pubdt_iso = pub_date.isoformat()
    
    dataset.cdm_data_type = "Gridded"                 
    dataset.Conventions = "CF-1.7"
    dataset.Metadata_Conventions = "Unidata Dataset Discovery v1.0"                
    dataset.comment = "Gridded file containing elevation estimates on a regular grid"                 
    dataset.contact = "cryotempo@earthwave.co.uk"               
    dataset.creator_email = "cryotempo@earthwave.co.uk"                 
    dataset.creator_url = "http://www.earthwave.co.uk"                  
    dataset.date_created = datetime.now().isoformat()                                
    dataset.date_modified = datetime.now().isoformat()
    dataset.DOI = "10.5270/CR2-2xs4q4l";    
    dataset.geospatial_y_min = minY                 
    dataset.geospatial_y_max = maxY         
    dataset.geospatial_x_min = minX                
    dataset.geospatial_x_max = maxX                  
    dataset.geospatial_y_units = "metres" 
    dataset.geospatial_x_units = "metres"
    dataset.geospatial_projection = proj4   
    dataset.geospatial_resolution = 2000
    dataset.geospatial_resolution_units = "metres"
    dataset.geospatial_global_uncertainty = 15.0
    dataset.geospatial_global_uncertainty_units = "metres"
    dataset.institution = "ESA, UoE, Earthwave, isardSAT"                 
    dataset.keywords = "Land Ice > Gridded > Elevation Model  > Elevation Points > Swath Processing > CryoSat-2"                 
    dataset.keywords_vocabulary = "NetCDF Climate and Forecast Standard Names"                 
    dataset.platform = " Cryosat-2"
    dataset.processing_level = "L3"
    dataset.product_version = "1.0"
    dataset.project = "CryoTEMPO which is an evolution of CryoSat+ CryoTop"                 
    dataset.references = "CryoSat-2 swath interferometric altimetry for mapping ice elevation and elevation change, In Advances in Space Research, (2017), ISSN 0273-1177, https://doi.org/10.1016/j.asr.2017.11.014"
    dataset.source = "Gridded Swath data generated from CryoSat-2 SARIn data."
    dataset.version = 1
    dataset.summary = "Land Ice Elevation Thematic Gridded Product" 
    dataset.time_coverage_duration = "P3M"
    dataset.time_coverage_start = minT 
    dataset.time_coverage_end = maxT
    dataset.title = "Land Ice Elevation Thematic Gridded Product"

    x = dataset.createDimension('x', len(xcoords))
    y = dataset.createDimension('y', len(ycoords))
    time = dataset.createDimension('time', 1)
    nv = dataset.createDimension('nv', 2)
    
    # Create coordinate variables for 4-dimensions
    times = dataset.createVariable('time', np.int32, ('time',))
    times.long_name = "Measurement of time"
    times.units = "Seconds from 1970 in the UTC timezone."
    times.short_name = "time"
    xs = dataset.createVariable('x', np.float32, ('x',))
    xs.long_name = "Polar Stereographic (EPSG: 3413) X Coordinate"
    xs.units = "metres"
    xs.standard_name = "x"
    ys = dataset.createVariable('y', np.float32, ('y',))
    ys.long_name = "Polar Stereographic (EPSG: 3413) Y Coordinate"
    ys.units = "metres"
    ys.standard_name = "y"
    elevations = dataset.createVariable('elevation', np.float32, ('time','x','y'), fill_value = fill_value)
    elevations.units = "metres"
    elevations.long_name = "CryoTempo: Gridded Elevation" 
    elevations.coordinates = "x y"

    nvs = dataset.createVariable('nv', np.int32, ('nv',))
    nvs.long_name = "Vertex"
    nvs.comment = "Vertex with values 0 or 1, where 0 is westerly or southerly and 1 is northerly or easterly"
    nvs.units = "Binary: 0 or 1"
    nvs.short_name = "nv"
    x_bnds = dataset.createVariable('x_bnds', np.float32, ('x','nv') )
    x_bnds.long_name = "x minimum and maximum bounds"
    x_bnds.comment = "x values at the west and east boundary of each pixel."                
    x_bnds.units = "metres" 
    x_bnds.short_name = "x_bnds"
    y_bnds = dataset.createVariable('y_bnds', np.float32, ('y','nv') )
    y_bnds.long_name = "y minimum and maximum bounds"
    y_bnds.comment = "y values at the north and south boundary of each pixel."                
    y_bnds.units = "metres" 
    y_bnds.short_name = "y_bnds"
    
    def boundsArray( coords, resolution ):
        bounds = [ ( coord - 0.5 * resolution, coord + 0.5 * resolution ) for coord in coords ]
        boundsArray = np.empty((len(coords), 2))
        
        for i, x in enumerate(bounds):
            lower, upper = x
            boundsArray[i][0] = lower
            boundsArray[i][1] = upper
        
        return boundsArray

    pub_dateUTC = datetime(pub_date.year, pub_date.month, pub_date.day, pub_date.hour, pub_date.minute, pub_date.second, tzinfo=timezone.utc)
    timesArray = np.zeros(1) 
    timesArray.fill( int(pub_dateUTC.timestamp() ) )
    
    indicatorVariables = [0,1]
    
    times[:] = timesArray
    xs[:] = xcoords
    x_bnds[:] = boundsArray(xcoords, resolution)
    ys[:] = ycoords
    y_bnds[:] = boundsArray(ycoords, resolution)
    
    nvs[:] = np.array(indicatorVariables)
    elevations[:,:,:] = data
            
    dataset.close()
    
    size = os.stat(fullPath).st_size
    attributes = { "Pub_Date": pubdt_iso, "File_Description" : fileDescription, "File_Type": "THEM_GRID_", "File_Name" : fileName, "Validity_Start" : minT, "Validity_Stop" : maxT, "Min_X" : bbox.minX, "Max_X" : bbox.maxX, "Min_Y" : bbox.minY, "Max_Y" : bbox.maxY, "Creator" : "Earthwave", "Creator_Version" : 0.1, "Tot_size" : size,"Projection": proj4, "Start_X":minX, "Stop_X":maxX,"Start_Y":minY,"Stop_Y":maxY,"Grid_Pixel_Width" : 2000, "Grid_Pixel_Height" : 2000  } 

    with open( headerPath, "wt", encoding="utf8"  ) as f:
        f.write( h.createHeader( attributes, gridded=True, source_files = {} ))

                