import os
import rasterio
import numpy as np
import imageio
from matplotlib import pyplot as plt
import matplotlib as mpl
import rasterio.plot as rp

limit=7
numBins = 6
year = 2011
areaLabel = 'Jak'
#extent = "-230000 -2340000 -130000 -2210000"
extent = "-230000 -2340000 -120000 -2170000"

def format_func(value, tick_number):
    return "{}".format(int(value/1000))

def keywords(kwargs):
    paramsText=""
    for key,value in kwargs.items():
        paramsText="{} -{} {}".format(paramsText,key,value)
    return paramsText

def gdal_diff(a_in,b_in,outFile,a_noDataValue,b_noDataValue,out_noDataValue):
    calc = "(A-B)*(A>{})*(B>{})".format(a_noDataValue,b_noDataValue)
    command = 'gdal_calc.py -A {} -B {} --overwrite --outfile={} --NoDataValue={} --calc="{}"'.format(a_in,b_in,outFile,out_noDataValue,calc)
    print(command)
    os.system(command)
    
def gdal_warp(inFile,outFile,**kwargs):
    paramsText = keywords(kwargs)
    command = "gdalwarp -overwrite -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=IF_SAFER {} {} {}".format(paramsText,inFile,outFile)
    print(command)
    os.system(command) 
    
param='elev'
#cm='viridis'
cm='ocean'

def getExtent( ras ):
    raster = rasterio.open(ras)
    
    extent = (raster.bounds[0], raster.bounds[2], raster.bounds[1], raster.bounds[3] )

    raster.close()
    
    return extent
    

def nicePlot(ras,fig,axis,title,colourlabel,nodata=-9999,vmin=0,vmax=0):
    raster = rasterio.open(ras)
    yAx=[raster.bounds[1],raster.bounds[3]]
    xAx=[raster.bounds[0],raster.bounds[2]]
    axis.set_ylim(np.min(yAx),np.max(yAx))
    axis.set_xlim(np.min(xAx),np.max(xAx))
    
    if vmin==0 and vmax==0:
        vals = raster.read()
        vals = vals[vals!=nodata]
        vmin=vals.min()
        vmax=vals.max()

    bar = fig.colorbar(ax=axis,mappable=mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=vmin, vmax=vmax), cmap=cm))
    bar.set_label(colourlabel)

    axis.set_xlabel("X Coordinate (km)")
    axis.set_ylabel("Y Coordinate (km)")

    axis.xaxis.set_major_formatter(plt.FuncFormatter(format_func))
    axis.yaxis.set_major_formatter(plt.FuncFormatter(format_func))
    
    rp.show(raster, ax=axis, transform=raster.transform, cmap=cm, title=title,vmin=vmin,vmax=vmax)
    raster.close()

def create_gif(filenames, duration, output_path, file):
    if not os.path.exists(output_path):
        print("Output path: ".format(output_path))
        os.makedirs(output_path)
        
    output_file = os.path.join(output_path, file)
        
    images = []
    for filename in filenames:
        images.append(imageio.imread(filename))
    imageio.mimsave(output_file, images, duration=duration)
    
def main():
    year = 2011
    baseLoadName = 'Joined-{}-Mar-May-{}'.format(areaLabel,year)
    baseDem = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_Grid_elev_bins{}_limit{}.tif'.format(baseLoadName,numBins,limit)
    baseDemCut = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/diffs/{}_Grid_elev_bins{}_limit{}_cut.tif'.format(baseLoadName,numBins,limit)
    gdal_warp(baseDem,baseDemCut,te=extent)
    
    saveList = []
    for year in [2011,2012,2013,2014,2015,2016]:
        loadName = 'Joined-{}-Mar-May-{}'.format(areaLabel,year)
        dem = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/{}_Grid_elev_bins{}_limit{}.tif'.format(loadName,numBins,limit)
        demCut = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/diffs/{}_Grid_elev_bins{}_limit{}_cut.tif'.format(loadName,numBins,limit)
        
        
        diffTif = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/tifs/diffs/diff_{}_Grid_elev_bins{}_limit{}.tif'.format(loadName,numBins,limit)
        
        gdal_warp(dem,demCut,te=extent)
        
        gdal_diff(demCut,baseDemCut,diffTif,0,0,-32768)
        
        fig, ax = plt.subplots(1, 1, figsize=(6,6),sharey=True)
        
        nicePlot(diffTif,ax,"{} vs 2011".format(year),"Diff (metres)",vmin=-25,vmax=1)
        
        saveName = '/data/puma1/scratch/cryotempo/uncertainty/processeddata/figs/diffs/{}_Diff_bins{}_limit{}.tif'.format(loadName,numBins,limit)
        fig.savefig(saveName)
        saveList.append(saveName)
    
    create_gif(saveList, 1.4, '/data/puma1/scratch/cryotempo/uncertainty/processeddata/figs/diffs/Jak.gif')

if __name__ == "__main__":
    main()

    
    
