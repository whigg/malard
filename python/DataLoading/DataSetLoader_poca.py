"""Loads poca data into Malard from the swath files 

Usage:
    python3 DataSetLoader_poca.py <year>    
"""

import sys
from os import listdir
import pandas as pd
import numpy as np
import netCDF4 
import os
import MalardClient.MalardClient as mc

import ProcessingRequest as pr
import DataSetLoader as dsl

"""Takes a list of swath files and column mappings, e.g. pocaH -> elev and creates a NetCDF with the mapped column names.

Args:
    swaths: a list of file name date pairs
    src_dir: path to read the files from
    tempDir: directory that the temporary files will be written to
    columnMappings: a dictionary of column name mappings to the standard malard names, eg. pocaLat to lat, pocaLon to lon
    region: used to check whether the swath covers the input region

Returns:
    List of file name date pairs.    


"""
def createTempFiles( swaths, src_dir, tempDir, columnMappings, region ):
    
    output_swaths = []
    
    for s,dt in swaths:
        print("Creating temp poca file {}".format(s))
        nc = netCDF4.Dataset(os.path.join(src_dir, s))
        data = {}
        
        if "lat" not in nc.variables:
            print( "Skipping file because lat column is missing {}".format(s) )
            continue
        
        lats = nc.variables["lat"]
        
        if region == "greenland":
            if np.max(lats) < 52:
                print( "Skipping file. Not in greenland {}".format(s))
                continue
        elif region == "antarctic":
            if np.max(lats) > -50:
                print( "Skipping file. Not in antarctic {}".format(s))
                continue
            
        for v in columnMappings.keys():
            d = nc.variables[v]
            srs = pd.Series(d[:])
            data[v] = srs

        df = pd.DataFrame(data)
        
       
        nc.close()
            
        ds = netCDF4.Dataset(os.path.join(tempDir, s),'w',format='NETCDF4')
        ds.createDimension( 'row', None )
            
        for column, dtype in zip(df.columns, df.dtypes):
            col = ds.createVariable(columnMappings[column], dtype, ('row',))
            col[:] = np.array(df[column])
            
        ds.close()
        output_swaths.append( (s, dt ) )
    
    return output_swaths
        
        
def cleanUpTempFiles( swaths, tempDir ):
    for s,d in swaths:
        os.remove( os.path.join(tempDir, s) )
        
def main(month, year, loaderConfig):
    
    parentDataSet = loaderConfig["parentDataSet"]
    dataSet = loaderConfig["dataSetPoca"]
    region = loaderConfig["region"]
    swathdir = loaderConfig["swathDir"]
    environmentName = loaderConfig["MalardEnvironment"]

    tempdir = '/data/puma/scratch/malard/tempnetcdfs'
    columnFilters = []
    pocaColumns = {'pocaH' : 'elev', 'pocaLat' : 'lat','pocaLon' : 'lon', 'pocaWf' : 'wf_number' }
    
    includeColumns = []
    
    ice_file = "/data/puma/scratch/cryotempo/masks/greenland/icesheets.shp" if region == "greenland" else "/data/puma1/scratch/cryotempo/sarinmasks/AntarcticaReprojected.shp"
    maskFilterIce = mc.MaskFilter( p_shapeFile=ice_file)
    lrm_file = "/data/puma/scratch/cryotempo/masks/greenland/LRM_Greenland.shp" if region == "greenland" else "/data/puma1/scratch/cryotempo/sarinmasks/LRM_Antarctica.shp"
    maskFilterLRM = mc.MaskFilter( p_shapeFile= lrm_file, p_includeWithin=False )
    maskFilters = [maskFilterIce, maskFilterLRM]
    
    gridCellSize = 100000
    
    years = [year]
    months = [month]
    
    for year in years:
        log_file = open( "log/{}_{}_{}_{}_swath_loading_log.csv".format(year, parentDataSet, dataSet, region),"w" )
        for month in months:
            swathfiles = [(f,dsl.dateFromFileName(f)) for f in listdir(swathdir) if  f.endswith(".nc") and dsl.isyearandmonth( f, year, month )]
            
            filtered_swaths = createTempFiles( swathfiles, swathdir, tempdir, pocaColumns, region )
            
            message = 'Processing Year Month %d-%d. Num Swaths %d' % (year,month,len(filtered_swaths) )
            print(message)
            log_file.write( message + "\n")
            if len(filtered_swaths) > 0:
                dsl.publishData(environmentName, filtered_swaths, parentDataSet, dataSet, region, tempdir, columnFilters, includeColumns, gridCellSize, maskFilters )
            
            cleanUpTempFiles( filtered_swaths, tempdir )


if __name__ == "__main__":
    args = sys.argv[1:]
    
    month = args[0]
    year = args[1]
    configPath = args[2]
        
    processingRequest = pr.ProcessingRequest.fetchRequest(configPath)
    loadConfig = processingRequest.getConfig
    
    print("Running for month=[{month}] and year=[{year}]".format(month=month, year=year))
    
    main(int(month),int(year), loadConfig)
    
