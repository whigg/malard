#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  5 14:59:22 2020

@author: jon
"""


import os
import Grid_6_ConvertToTiff_Interp as grid_tif
import Timeseries as t

from osgeo import gdal
import numpy as np

from datetime import datetime

import MalardClient.MalardClient as mc

client = mc.MalardClient("MALARD-PROD")

class TempFile:
    def __init__(self, base_path, step_name, dataset, pub_date, ext=".tif" ):
        self._name = os.path.join( base_path, "{step}_{ds}_{year}_{month}{ext}".format(step=step_name
                                                                                                , ds=dataset
                                                                                                , year=pub_date.year
                                                                                                , month=pub_date.month
                                                                                                , ext=ext))
    @property
    def name(self):
        return self._name
        
    def remove(self):
        os.remove(self._name)


def getPixel(x, y, gt):
    '''
    :param x: pandas df
    :param y: pandas df
    :param gt: GeoTransform
    :return:
    '''
    py = ((x-gt[0])/gt[1]).astype('int64')
    px = ((y-gt[3])/gt[5]).astype('int64')
    return px, py


def loadFile( file ):
    dem = gdal.Open(file, gdal.GA_ReadOnly)
    demArray = dem.GetRasterBand(1).ReadAsArray()
    demGt = dem.GetGeoTransform()
    return ( dem, demArray, demGt )

#load up the stage 3 DEM after it has been filled

def create_and_apply_quadrant_mask( input_tif_path, output_tif_path, inner_dem_pattern, final_dem_pattern, comparison_dem, slope_dem, year, month, centre_offset=1000, useSlope=False):
    
    if not os.path.exists(output_tif_path):
        os.mkdir(output_tif_path)
    
    inner_sampled = inner_dem_pattern.format( year=year, month=month )
    final_tif = final_dem_pattern.format( year=year, month=month )
    
    inner_tif = os.path.join(input_tif_path, inner_sampled )
    final_file = os.path.join(input_tif_path, final_tif )


    inner_bounds = grid_tif.ras_getBounds( inner_tif )
    print("Inner Bounds {}".format(inner_bounds))

    final_bounds = grid_tif.ras_getBounds( final_file)
    print("Outer bounds {}".format(final_bounds))

    xs = range(int(final_bounds[0])+1000, int(final_bounds[2]), 2000)
    ys = range(int(final_bounds[1])+1000, int(final_bounds[3]), 2000)
    print(xs)

    minX = int(final_bounds[0])
    maxX = int(final_bounds[2])
    minY = int(final_bounds[1])
    maxY = int(final_bounds[3])

    extent = { "minX":minX, "maxX":maxX, "minY":minY,"maxY":maxY }


    dem, demArray, demGt = loadFile( final_file  )
    inner_dem, inner_demArray, inner_demGt = loadFile( inner_tif  )
    print(ys[1])
    print(xs[1])

    print(len(xs))
    print(len(ys))
    print( demArray.shape )

    outArray = np.empty( demArray.shape  )
    outArray.fill(-32768)
    
    #load the slope den
    slopeDem, slopeArray, slopeGt = loadFile(slope_dem)
    
    print("minSlope={} maxSlope={}".format(np.max(slopeArray), np.min(slopeArray)))

    def quadrant_has_pixels( demArray, demGt, x_centre, y_centre, offset, extent  ):

        inner_x = range( max( extent["minX"] + 100, x_centre - offset + 100 ), min(extent["maxX"] - 100, x_centre + offset), 200 )
        inner_y = range( max( extent["minY"] + 100, y_centre - offset + 100 ), min(extent["maxY"] - 100, y_centre + offset), 200 )

        for i_x in inner_x:
            for i_y in inner_y:
                if demArray[getPixel( np.array(i_x), np.array(i_y), demGt )] != -999:
                    return True
        return False

    quadrant_width = int(centre_offset / 2 + 200)

    for i, x in enumerate(xs):
        for j, y in enumerate(ys):
            if demArray[ getPixel( np.array(x), np.array(y), demGt )  ] != -32768:
                try:
                    upperLeft = quadrant_has_pixels( inner_demArray, inner_demGt, x - centre_offset, y + centre_offset, quadrant_width, extent )
                    upperRight = quadrant_has_pixels( inner_demArray, inner_demGt, x + centre_offset, y + centre_offset, quadrant_width, extent )
                    bottomLeft = quadrant_has_pixels( inner_demArray, inner_demGt, x - centre_offset, y - centre_offset, quadrant_width, extent )
                    bottomRight = quadrant_has_pixels( inner_demArray, inner_demGt, x + centre_offset, y - centre_offset, quadrant_width, extent )

                    if abs(slopeArray[getPixel(np.array(x), np.array(y), slopeGt)]) < 0.5 and useSlope:
                        outArray[j,i] = 1
                    elif upperLeft and upperRight and bottomLeft and bottomRight:
                        outArray[j,i] = 1
                    else:
                        outArray[j,i] = -32768

                except IndexError:
                    print("IndexError at x={} and y={}".format(x,y))

    outArray = np.flip(outArray,axis=0)

    coverage_file_name = final_tif.replace( ".tif","_mask_{}.tif".format(centre_offset) )
    
    if coverage_file_name == final_tif:
        raise ValueError("Coverage file cannot have the same name as the output file.")
    
    
    coverage_file = os.path.join(output_tif_path, coverage_file_name )
    
    

    driver = gdal.GetDriverByName('GTiff')
    result = driver.CreateCopy(coverage_file, gdal.Open(final_file))
    result.GetRasterBand(1).WriteArray(outArray)
    result = None

    slope_str = "withSlope" if useSlope else "noSlope"
    
    final_mask = os.path.join(output_tif_path, final_tif.replace(".tif", "_quad_mask_{}_{}.tif".format(centre_offset, slope_str)))
    
    grid_tif.gdal_calc(final_file, coverage_file, final_mask,"A*(B>0)-32768*(B<1)",-32768)
    
    if final_file == final_mask:
        raise ValueError("Final mask cannot have the same name as the output file.")
    
    ##Now do the ArcticDEM differences
    grisDem = TempFile(output_tif_path, "grid_6_adem","greenland_nw_{}".format(centre_offset), datetime(2011,2,1))

    outres = "{} {}".format(2000,2000)

    proj4 = client.getProjection(mc.DataSet("cryotempo","swath_c_nw","greenland")).proj4

    bds = final_bounds
    te = "{} {} {} {}".format(bds[0],bds[1],bds[2],bds[3])

    grid_tif.gdal_warp(comparison_dem,grisDem.name,r="cubic",tr=outres,t_srs='"{}"'.format(proj4),te=te,tap="",et=0)            

    #Diff before adjustment
    pre_diff = os.path.join(output_tif_path, final_mask.replace(".tif","_before_diff.tif"))
    grid_tif.gdal_diff(final_file,grisDem.name, pre_diff, -32768,-32768,-32768)
    #After adjustment
    post_diff = os.path.join(output_tif_path, final_mask.replace(".tif","_after_diff.tif".format(centre_offset)))
    grid_tif.gdal_diff(final_mask,grisDem.name, post_diff, -32768,-32768,-32768)
    
    grisDem.remove()
    os.remove(coverage_file)
    
coverage = [1000]

tif_path = '/data/puma/scratch/cryotempo/processeddata/greenland_nw_adjust/SwathAndPocaESAC_Unc_7_MinP_1_MaxPix_8_DemDiffMad_6_Resolution_2000'

inner_dem_pattern = "Gridded_2_test_swath_c_nw_adjust_6_unc_greenland_{year}_{month}.tif"

gris_dem = '/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/combined/GrIS_dem_2km.tif'

slope_dem = '/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/combined/GrIS_dem_2km_slope.tif'

final_dem_pattern = "greenland_{year}_{month}.tif"

year = 2011
month = 2

for c in coverage:
    create_and_apply_quadrant_mask( tif_path, os.path.join(tif_path,"mask"), inner_dem_pattern, final_dem_pattern, gris_dem, slope_dem, year, month, centre_offset=c, useSlope=True)
