from osgeo import ogr
import osgeo.osr as osr
from osgeo import gdal

def makeSpatialNC(inputNCPath,proj4,outputNCPath,outLayerName='data',xField='x',yField='y',zField='elev'):
    
    #Load data 
    inDriver = ogr.GetDriverByName("NetCDF")
    inSource = inDriver.Open(inputNCPath)

    #Move data to memory otherwise setting features doesn't appear to work
    memDriver=ogr.GetDriverByName('MEMORY')
    source=memDriver.CreateDataSource('memData')
    layer=source.CopyLayer(inSource.GetLayer(),'data',['OVERWRITE=YES'])
    
    #Set Projection
    t_srs = osr.SpatialReference()
    t_srs.ImportFromProj4(proj4)
    srs = ogr.GeomFieldDefn()
    srs.SetSpatialRef(t_srs)
    
    #Add Geometry
    layer.CreateGeomField(srs,1)
    for feature in layer:
        wkt = "POINT(%f %f %f)" %  (feature.GetField(xField) , feature.GetField(yField) , feature.GetField(zField))
        point = ogr.CreateGeometryFromWkt(wkt)
        feature.SetGeometry(point)
        layer.SetFeature(feature)
    
    #Output data
    outDriver = ogr.GetDriverByName("netCDF")
    ofile_opts = [ 'FORMAT=NC4C', 'COMPRESS=DEFLATE', 'WRITE_GDAL_TAGS=YES' ]
    outSource = outDriver.CreateDataSource(outputNCPath,ofile_opts)
    outSource.CopyLayer(layer,'data',['OVERWRITE=YES'])
    
    #Sync and Release
    outSource.SyncToDisk()
    outSource.Release()
    inSource.Release()
    source.Release()
    
    return outputNCPath


def createGrid(inputNCPath,outputTifPath,algorithm='invdist'):
    #output = gdal.Grid(outputTifPath,inputNCPath,algorithm=algorithm)
    output = gdal.Grid(outputTifPath,inputNCPath,algorithm='invdist:power=2.0:smoothing=0.0:radius1=2000.0:radius2=4000.0:angle=0.0:max_points=500:min_points=1:nodata=0.0', format="NetCDF", width=50, height=50)  
    output.FlushCache()
    return outputTifPath

def plot(filePath):
    import rasterio
    from rasterio.plot import show
    raster = rasterio.open(filePath)
    show(raster, transform=raster.transform, cmap='viridis')