package com.earthwave.pointstream.impl

import org.gdal.ogr.ogr
import org.gdal.ogr.{Driver, Layer, Geometry, ogrConstants, DataSource,FieldDefn,Feature}

class NetCdfRasterWriter( filename : String ) {

  val inmemDriver = ogr.GetDriverByName("MEMORY")
  val inmemsource = inmemDriver.CreateDataSource("memData")
  val outlayer = inmemsource.CreateLayer("geometry")

  def write( xCol : ucar.ma2.Array, yCol : ucar.ma2.Array, zCol : ucar.ma2.Array, filter : Array[Int]) = {

    filter.foreach( f_pt =>
    {
      val feature = new Feature( outlayer.GetLayerDefn() )
      val x = xCol.getDouble(f_pt)
      val y = yCol.getDouble(f_pt)
      val e = zCol.getDouble(f_pt)

      val pt : Geometry = new Geometry( ogrConstants.wkbPoint )
      pt.SetPoint(0, x,y,e )
      feature.SetGeometry(pt)
      outlayer.CreateFeature(feature)
      pt.delete()
      feature.delete()
    })
  }

  def close() ={
    //Output data
    val outDriver = ogr.GetDriverByName("netCDF")
    val outSource = outDriver.CreateDataSource(filename)
    outSource.CopyLayer(outlayer,"data")

    //Sync and Release
    outSource.SyncToDisk()
    outSource.delete()

    outlayer.delete()
    inmemsource.delete()
    inmemDriver.delete()
  }
}
