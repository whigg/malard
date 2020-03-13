import os
import rasterio
import numpy as np

def keywords(kwargs):
    paramsText=""
    for key,value in kwargs.items():
        paramsText="{} -{} {}".format(paramsText,key,value)
    return paramsText

def gdal_translate(inFile,outFile,**kwargs):
    paramsText = keywords(kwargs)
    command = "gdal_translate {} {} {}".format(paramsText,inFile,outFile)
    print(command)
    os.system(command) 

def gdal_slope(inFile,outFile,**kwargs):
    paramsText = keywords(kwargs)
    command = "gdaldem slope {} {} {}".format(paramsText,inFile,outFile)
    print(command)
    os.system(command)

def gdal_diff(a_in,b_in,outFile,a_noDataValue,b_noDataValue,out_noDataValue):
    calc = "(A-B)*(A>{})*(B>{})".format(a_noDataValue,b_noDataValue)
    command = 'gdal_calc.py -A {} -B {} --overwrite --outfile={} --NoDataValue={} --calc="{}"'.format(a_in,b_in,outFile,out_noDataValue,calc)
    print(command)
    os.system(command)

def gdal_sum(a_in,b_in, outFile, a_noDataValue,b_noDataValue,out_noDataValue):
    calc = "(A+B)*(A>{})+(B>{})".format(wgt_lhs, wgt_rhs, a_noDataValue,b_noDataValue)
    command = 'gdal_calc.py -A {} -B {} --overwrite --outfile={} --NoDataValue={} --calc="{}"'.format(a_in,b_in,outFile,out_noDataValue,calc)
    print(command)
    os.system(command)
    
def gdal_calc(a_in,b_in,outFile,calc,out_noDataValue):
    command = 'gdal_calc.py -A {} -B {} --overwrite --outfile={} --NoDataValue={} --calc="{}"'.format(a_in,b_in,outFile,out_noDataValue,calc)
    print(command)
    os.system(command)
 
def gdal_warp(inFile,outFile,**kwargs):
    paramsText = keywords(kwargs)
    command = "gdalwarp -overwrite -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=IF_SAFER {} {} {}".format(paramsText,inFile,outFile)
    print(command)
    os.system(command) 
    
def gdal_fillnodata(inFile,outFile,noDataValue,**kwargs):
    paramsText = keywords(kwargs)
    command = "gdal_fillnodata.py -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=IF_SAFER {} {} {}".format(paramsText,inFile,outFile)
    print(command)
    os.system(command)
    command2 = "gdal_edit.py -a_nodata {} {}".format(noDataValue,outFile)
    print(command2)
    os.system(command2)   

def ras_getBounds(inFile):
    raster = rasterio.open(inFile)
    yAx=[raster.bounds[1],raster.bounds[3]]
    xAx=[raster.bounds[0],raster.bounds[2]]
    raster.close()
    return [np.min(xAx),np.min(yAx),np.max(xAx),np.max(yAx)] # Why had to reverse y? Is it due to order of y in CSV file?

#numBinsList=[5]
#limitList=[2.5]

numBinsList=[6] # Prod = 6
#limitList=[2.5,3.7,5] # Prod = 6
#limitList=[3,5,7,10,15] # Prod = 7
limitList=[7] # Prod = 7
    
#years = [2011,2012,2013,2014,2015,2016]
years = [2012]
areaLabel = 'Jak' #'pet' # or 'jak', 'str'

maxPixelDist = 8 # Prod = 8   ### 8 for 200m, 18 for 100m # i.e. sums to 2km included 2 end points. This is multples of 200m if res is 200m. Set to 8 so max interpolation is approx 2km.
smoothIterations=0 # Prod = 0
res = 2000 # Prod = 2000

def main():
    adFull = '/data/puma1/scratch/cryotempo/uncertainty/ArcticDEM/AllAD_100m.tif'
    mask = '/data/puma1/scratch/cryotempo/masks_raster/GrIS_noLRM.tif'
    outRes = "{} {}".format(res,res)
    
    for year in years:
        for numBins in numBinsList:
            for limit in limitList:
                param='elev'
                print("Start {} with {} bins, {} limit".format(year,numBins,limit))
                #Load data
                loadName = 'Joined-{}-Mar-May-{}'.format(areaLabel,year)
                
                dem = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_Grid_{}_bins{}_limit{}.tif'.format(loadName,param,numBins,limit) 
         
                csv = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/csv/{}_Grid_{}_bins{}_limit{}.csv'.format(loadName,param,numBins,limit) 
                inDem = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_Grid_{}_bins{}_limit{}_InnerRes.tif'.format(loadName,param,numBins,limit) 
                slope = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_Grid_slope_bins{}_limit{}.tif'.format(loadName,numBins,limit) 
                arcticDEM = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_AD_cut_bins{}_limit{}.tif'.format(loadName,numBins,limit) 
                maskDem = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_mask_bins{}_limit{}.tif'.format(loadName,numBins,limit) 
    
                
                diff = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_Diff_{}_bins{}_limit{}.tif'.format(loadName,param,numBins,limit) 
                diffT = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_Diff_{}_bins{}_limit{}_temp.tif'.format(loadName,param,numBins,limit) 
                
                #Grid cell level:
                gdal_translate(csv,inDem,a_srs="EPSG:3413",a_nodata=-999)
                
                #Global level from then on:
                #Pre step to merge above translated DEMs
                
                
                fillDem = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_Grid_{}_bins{}_limit{}_interp.tif'.format(loadName,param,numBins,limit) 
                gdal_fillnodata(inDem,fillDem,-999,md=maxPixelDist,si=smoothIterations)
                
                smoothDem = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_Grid_{}_bins{}_limit{}_smooth.tif'.format(loadName,param,numBins,limit) 
                
                gdal_warp(fillDem,smoothDem,r='cubic',tr=outRes,tap="",et=0,dstnodata=-32768)
                
                bds = ras_getBounds(smoothDem)
                
                #What is going on with these flippind axis?
                
                gdal_warp(mask,maskDem,r="cubic",tr=outRes,t_srs='"+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"',
                  te="{} {} {} {}".format(bds[0],bds[1],bds[2],bds[3]),tap="",et=0)            
                
                #################
                #This output is the product - e.g. in the file dem
                #This outputs the dem to use - may need to change to have nodate -32768 - only at 0 for the colormaps
                gdal_calc(smoothDem,maskDem,dem,"A*(B>0)",0)
                #################
                
                gdal_slope(dem,slope,alg='ZevenbergenThorne')
                
                #Swapped the bds axis around - what is going on?
                
                gdal_warp(adFull,arcticDEM,r="cubic",tr=outRes,t_srs='"+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"',
                  te="{} {} {} {}".format(bds[0],bds[1],bds[2],bds[3]),tap="",et=0)
                
                gdal_diff(dem,arcticDEM,diff,0,-9999,-32768)
                #gdal_warp(diffT,diff,r="cubic",tr=outRes,t_srs='"+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"')
                
                #csvU = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/csv/{}_Grid_Q_uStd_bins{}_limit{}.csv'.format(loadName,numBins,limit) 
                #uncert = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_Grid_uncert_bins{}_limit{}.tif'.format(loadName,numBins,limit) 
                #gdal_translate(csvU,uncert,a_srs="EPSG:3413",a_nodata=0)
                
                #Mask AD so has same cells as for our DEM
                adMask = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_AD_mask_bins{}_limit{}.tif'.format(loadName,numBins,limit) 
                gdal_calc(dem,arcticDEM,adMask,"B*(A>0)",0)
        
                adSlope = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_AD_slope_bins{}_limit{}.tif'.format(loadName,numBins,limit) 
                gdal_slope(adMask,adSlope,alg='ZevenbergenThorne')
                
                slopeDiff = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_Diff_slope_{}_bins{}_limit{}.tif'.format(loadName,param,numBins,limit) 
                gdal_diff(slope,adSlope,slopeDiff,0,0,0)
                
if __name__ == "__main__":
    main()