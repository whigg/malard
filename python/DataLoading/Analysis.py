#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 13:23:17 2020

@author: jon
"""

from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import Timeseries as t
import matplotlib.pyplot as plt

temp = "temp"

def create_dem_gif_by_month(start_date, end_date):
    dt = start_date

    tifs = []
     
    out_path = "{}/{}_u7_p8_m6.gif".format(analysis_path, area)
    
    while dt <= end_date:
        
        file = "{area}_{year}_{month}.tif".format(year=dt.year, month=dt.month,area="greenland" )
        
        full_file = os.path.join(file_path, file)
        print(full_file)
        if os.path.isfile(full_file) :
            fig, ax = plt.subplots(1, 1, figsize=(6,12),sharey=True)
        
            t.nicePlot(full_file,fig, ax,"CryoTEMPO DEM Year={} Month={}".format(dt.year, dt.month),"Elevation (metres)",vmin=-25,vmax=3500)
        
            saveName = '{}/greenland_{}_{}_temp.png'.format(analysis_path, dt.year,dt.month)
            fig.savefig(saveName)
            tifs.append(saveName)
            
        dt = dt + relativedelta(months=1)
        
    t.create_gif( tifs, 1.4, out_path )

    for tif in tifs:
        os.remove(tif)

def create_month_to_month_diff( start_date, end_date, nMonths = 1 ):
    
    dt_pre = start_date
    dt_post = start_date + relativedelta(months = nMonths)
    
    tifs = []
    
    temp_path = "temp"
    
    out_path = "analysis/greenland_month_diff_u7_p8_m6_lag_{}.gif".format(nMonths)
    
    while dt_pre <= end_date:
        
        file_post = os.path.join( file_path, "greenland_nw_{year}_{month}.tif".format(year=dt_post.year, month=dt_post.month ))
        file_post_warp = os.path.join( temp_path, "greenland_nw_{year}_{month}_warp.tif".format(year=dt_post.year, month=dt_post.month ))
        
        file_pre = os.path.join( file_path, "greenland_nw_{year}_{month}.tif".format(year=dt_pre.year, month=dt_pre.month ))
        file_pre_warp = os.path.join( temp_path, "greenland_nw_{year}_{month}_warp.tif".format(year=dt_pre.year, month=dt_pre.month ))
        
        post_minX, post_maxX, post_minY, post_maxY = t.getExtent(file_post)
        pre_minX, pre_maxX, pre_minY, pre_maxY = t.getExtent(file_pre)
        
        
        extent = "{minX} {minY} {maxX} {maxY}".format(minX=max(post_minX, pre_minX), minY=min(pre_minY, post_minY), maxX=max(pre_maxX, post_maxX), maxY=min(pre_maxY, post_maxY))
        
        print(extent)
        
        dt_pre = dt_post
        dt_post = dt_post + relativedelta(months=nMonths)
        
        file_diff = 'analysis/greenland_{}_{}_{}_temp.tif'.format(dt_post.year,dt_post.month,nMonths)
        
        t.gdal_warp(file_pre, file_pre_warp ,te=extent)
        
        t.gdal_warp(file_post, file_post_warp ,te=extent)
        
        t.gdal_diff(file_post_warp,file_pre_warp,file_diff,-32768,-32768,0)
        
        fig, ax = plt.subplots(1, 1, figsize=(6,12),sharey=True) 
        t.nicePlot(file_diff,fig, ax,"DEM Diff Year={} Month={} Lag={}".format(dt_post.year, dt_post.month, nMonths),"Diff (metres)",vmin=-20,vmax=20)
        
        saveName = '{}/greenland_{}_{}_{}.png'.format(analysis_path, dt_post.year,dt_post.month,nMonths)
        fig.savefig(saveName)
        tifs.append(saveName)
        
        os.remove( file_post_warp )
        os.remove( file_pre_warp )
        os.remove( file_diff )
        
    
    t.create_gif( tifs, 1.4, out_path )

    for tif in tifs:
        os.remove(tif)
        
def create_arctic_dem_diff( start_date, end_date, nMonths = 1 ):
    
    dt = start_date
    
    tifs = []
    
    temp_path = "temp"
    
    out_path = "{}/{}_ArcticDemDiff.gif".format(analysis_path, area)
    
    while dt <= end_date:
        
        file_post = os.path.join( file_path, "{area}_{year}_{month}.tif".format(area=area, year=dt.year, month=dt.month ))
        if os.path.isfile(file_post):
            file_post_warp = os.path.join( temp_path, "{area}_{year}_{month}_warp.tif".format(area=area, year=dt.year, month=dt.month ))
            
            arcticDEM_full = "/data/puma/scratch/cryotempo/underlying_dems/greenland_arctic_dem/combined/arcticdem_mosaic_100m_v3.0.tif"
            arctiCDem_cut = os.path.join( temp_path, "GrIS_dem_temp_{}_{}.tif".format(dt.year,dt.month))
            
            post_minX, post_maxX, post_minY, post_maxY = t.getExtent(file_post)
                    
            extent = "{minX} {minY} {maxX} {maxY}".format(minX=post_minX, minY=post_minY, maxX=post_maxX, maxY=post_maxY)
            
            file_diff = 'analysis/greenland_arcticdem_diff_{}_{}.tif'.format(dt.year,dt.month)
            
            outres = "{} {}".format(2000,2000)
            
            t.gdal_warp(arcticDEM_full,arctiCDem_cut,r="cubic",tr=outres,t_srs='"+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"', te=extent,tap="",et=0)
            
            t.gdal_warp(file_post, file_post_warp ,te=extent)
            
            print(t.getExtent(file_post_warp))
            print(t.getExtent(arctiCDem_cut))
            t.gdal_diff(file_post_warp,arctiCDem_cut,file_diff,-32768,-9999,0)
            
            fig, ax = plt.subplots(1, 1, figsize=(6,12),sharey=True) 
            t.nicePlot(file_diff,fig, ax,"ArcticDEM Diff Year={} Month={}".format(dt.year, dt.month, nMonths),"Diff (metres)",vmin=-20,vmax=20)
            
            saveName = 'analysis/greenland_arcticdem_diff_{}_{}.png'.format(dt.year,dt.month)
            fig.savefig(saveName)
            tifs.append(saveName)
            
            os.remove( file_post_warp )
            os.remove( arctiCDem_cut )
            #os.remove( file_diff )
            
        dt = dt + relativedelta(months=nMonths)
        
    
    t.create_gif( tifs, 1.4, out_path )

    for tif in tifs:
        os.remove(tif)
        
def create_diff_from_base(path, start_date, end_date, nMonths = 1 ):
    
    dt = start_date
    
    tifs = []
    
    out_path = os.path.join(path, "gifs")
    
    file = "greenland_nw_meandiff.gif"
    
    while dt <= end_date:
        
        date_str = dt.strftime("%Y%m%d")
        
        file_post = os.path.join( path, "greenland_nw_{}.tif".format(date_str))
        file_post_warp = os.path.join( temp, "greenland_nw_{}_warp.tif".format(date_str))
        
        base = os.path.join( path, "Greenland_nw_mean.tif")
        base_warp = os.path.join( temp, "Greenland_nw_{}_base_warp.tif".format(date_str ))
        
        post_minX, post_maxX, post_minY, post_maxY = t.getExtent(file_post)
                
        extent = "{minX} {minY} {maxX} {maxY}".format(minX=post_minX, minY=post_minY, maxX=post_maxX, maxY=post_maxY)
        
        file_diff = 'analysis/greenland_arcticdem_base_diff_{}_{}.tif'.format(dt.year,dt.month)
        
          
        t.gdal_warp(base,base_warp, te=extent)
        
        t.gdal_warp(file_post, file_post_warp ,te=extent)
        
        print(t.getExtent(file_post_warp))
        print(t.getExtent(base_warp))
        t.gdal_diff(file_post_warp,base_warp,file_diff,-32768,-32768,0)
        
        fig, ax = plt.subplots(1, 1, figsize=(6,12),sharey=True) 
        t.nicePlot(file_diff,fig, ax,"CryoTEMPO vs Mean Year={} Month={}".format(dt.year, dt.month),"Diff (metres)",vmin=-20,vmax=20)
        
        saveName = 'analysis/greenland_meandiff_diff_{}_{}.png'.format(dt.year,dt.month)
        fig.savefig(saveName)
        tifs.append(saveName)
        
        os.remove( file_post_warp )
        os.remove( base_warp )
        os.remove( file_diff )
        
        dt = dt + relativedelta(months=nMonths)
        
    
    t.create_gif( tifs, 1.4, out_path, file )

    for tif in tifs:
        os.remove(tif)


def main():

    tif_path = "/data/eagle/team/shared/Work/CryoTEMPO/PM5/SwathTimeSeries/u7_p8_ddm6_r2000"
    start_date = datetime( 2010,8,1 )
    end_date = datetime(2016,11,1 )
    
    #create_dem_gif_by_month(start_date, end_date)
    
    #create_month_to_month_diff(start_date, end_date, 1)
    
    #create_arctic_dem_diff( start_date, end_date )
    
    create_diff_from_base( tif_path, start_date, end_date, 1 )
    
if __name__ == "__main__":
    main()
    
    