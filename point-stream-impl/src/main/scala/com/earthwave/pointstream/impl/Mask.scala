package com.earthwave.pointstream.impl


import com.earthwave.catalogue.api.{BoundingBoxFilter, MaskFilter}
import org.gdal.ogr.ogr
import org.gdal.ogr.{Driver, Layer, Geometry, ogrConstants, DataSource,FieldDefn,Feature}

object Mask
{
  def getMask( bbf : BoundingBoxFilter ) : Mask ={

    if( bbf.extentFilter.shapeFile.isEmpty && bbf.extentFilter.wkt.isEmpty && bbf.maskFilters.isEmpty )
    {
       new NullMask(bbf)
    }
    else
    {
      new CompositeMask(bbf)
    }
  }
}


trait Mask {


  def getExtent() : (Double, Double, Double, Double )

  def checkInMask( x : Double, y : Double ) : Boolean

  def hasFilters() : Boolean

  def close()
}

class CompositeMask( bbf : BoundingBoxFilter ) extends Mask
{
  val driver = ogr.GetDriverByName("ESRI Shapefile")
  val inmemDriver = ogr.GetDriverByName("MEMORY")

  val hasExtent = if( !bbf.extentFilter.shapeFile.isEmpty || !bbf.extentFilter.wkt.isEmpty ){  true }else{false}
  val extentFilter: Option[Mask] =  if( hasExtent && !bbf.extentFilter.shapeFile.isEmpty )
                      {
                          Some(new ShapeMask( bbf.extentFilter, bbf.minX, bbf.maxX, bbf.minY, bbf.maxY, driver, inmemDriver ))
                      }
                      else if( hasExtent && !bbf.extentFilter.wkt.isEmpty  )
                      {
                          Some(new GeometryMask( bbf.extentFilter ) )
                      }
                      else if( hasExtent ){
                          throw new Exception("Bounding box is empty then an extent is required.")
                      }
                      else
                      {
                          None
                      }

  val filters = bbf.maskFilters.map( mf => if( !mf.shapeFile.isEmpty() )
                                            {
                                              new ShapeMask( mf, bbf.minX, bbf.maxX, bbf.minY, bbf.maxY, driver, inmemDriver )
                                            }
                                            else
                                            {
                                                new GeometryMask(mf)
                                            } )

  override def getExtent()  : (Double, Double, Double, Double ) =
  {
    if( hasExtent )
    {
      extentFilter.get.getExtent()
    }
    else
    {
      ( bbf.minX, bbf.maxX, bbf.minY, bbf.maxY )
    }
  }

  override def checkInMask( x : Double, y : Double ) : Boolean = {

    if (hasExtent == true) {
      val inExtent = extentFilter.get.checkInMask(x, y)
      if (filters.isEmpty == true)
      {
        return inExtent
      }
      else if( inExtent == false )
      {
        return false
      }
      else
      {
        return filters.map(c => c.checkInMask(x, y)).reduce((x, y) => if (x == true && y == true) {true} else {false})
      }
    }
    else {
      return if (filters.isEmpty == true) {true} else {filters.map(c => c.checkInMask(x, y)).reduce((x, y) => if (x == true && y == true) {true} else {false})}
    }
  }

  override def hasFilters(): Boolean = {
    true
  }

  override def close() = {

    if(hasExtent){ extentFilter.get.close() }

    filters.foreach(f => f.close())

    inmemDriver.delete()
    driver.delete()
  }
}

class NullMask( bbf : BoundingBoxFilter  ) extends Mask{

  override def checkInMask(x: Double, y: Double): Boolean = {
    true
  }

  override def getExtent(): (Double, Double, Double, Double) = {
    ( bbf.minX, bbf.maxX, bbf.minY, bbf.maxY)
  }

  override def hasFilters(): Boolean = {
    false
  }


  override def close(): Unit = {

  }
}





class ShapeMask( maskFilter : MaskFilter, minX : Double, maxX : Double, minY : Double, maxY : Double, driver : Driver, inmemDriver : Driver ) extends Mask{

  def createLayer( f : MaskFilter ) : ( Boolean, Layer, DataSource ) =
  {
    if( minX == 0.0 && maxX == 0.0 && minY == 0.0 && maxY == 0.0 )
    {
      val source = driver.Open(f.shapeFile)
      val layer = source.GetLayer(0)
      (f.includeWithin, layer, source)
    }
    else
    {
      val source = driver.Open(f.shapeFile)
      val layer = source.GetLayer(0)

      val inmemsource = inmemDriver.CreateDataSource("memData")

      //Create a Polygon from the extent tuple
      val ring = new Geometry(ogrConstants.wkbLinearRing)
      ring.AddPoint(minX, minY)
      ring.AddPoint(minX, maxY)
      ring.AddPoint(maxX, maxY)
      ring.AddPoint(maxX, minY)
      ring.AddPoint(minX, minY)
      val poly = new Geometry(ogrConstants.wkbPolygon)
      poly.AddGeometry(ring)

      val outLayer = inmemsource.CreateLayer("filter")

      //Add an ID field
      val idField = new FieldDefn("id", ogrConstants.OFTInteger)
      outLayer.CreateField(idField)

      //Create the feature and set values
      val featureDefn = outLayer.GetLayerDefn()
      val feature = new Feature(featureDefn)
      feature.SetGeometry(poly)
      feature.SetField("id", 1)
      outLayer.CreateFeature(feature)
      feature.delete()

      val result_layer = inmemsource.CreateLayer("clippedmask")

      layer.Clip( outLayer, result_layer  )

      ring.delete()
      poly.delete()
      idField.delete()
      layer.delete()
      source.delete()
      outLayer.delete()

      (f.includeWithin, result_layer, inmemsource)
    }

  }

  val layers = createLayer( maskFilter )

  val ls = (layers._1, layers._2)

  override def getExtent(): (Double, Double, Double, Double) = {

      val extent = ls._2.GetExtent()

      (extent(0),extent(1),extent(2),extent(3))
  }

  override def checkInMask( x : Double, y : Double ) : Boolean={

    def inMask( layer : Layer, includeInMask : Boolean  ) : Boolean= {
      val pt: Geometry = new Geometry(ogrConstants.wkbPoint)
      pt.SetPoint_2D(0, x, y)

      //Set up a spatial filter such that the only features we see when we
      //loop through "lyr_in" are those which overlap the point defined above
      layer.SetSpatialFilter(pt)

      //Loop through the overlapped features and display the field of interest
      val ret = if (layer.GetFeatureCount() > 0 && includeInMask == true) {
        true
      }
      else if( layer.GetFeatureCount() == 0 && includeInMask == false  )
      {
        true
      }
      else
      {
        false
      }
      pt.delete()
      ret
    }
    inMask(ls._2, ls._1 )
  }

  override def hasFilters(): Boolean = {
    true
  }

  override def close() ={
      layers._2.delete()
      layers._3.delete()
  }
}

class GeometryMask( maskFilter : MaskFilter ) extends Mask
{
  val geometry = ogr.CreateGeometryFromWkt(maskFilter.wkt)

  override def getExtent(): (Double, Double, Double, Double) = {
    val data = new Array[Double](4)
    geometry.GetEnvelope(data)
    ( data(0),data(1),data(2),data(3) )
  }

  override def checkInMask(x: Double, y: Double): Boolean = {

    val pt: Geometry = new Geometry(ogrConstants.wkbPoint)
    pt.SetPoint_2D(0, x, y)

    val ret = pt.Within(geometry)

    pt.delete()
    ret
  }

  override def hasFilters(): Boolean = {
    true
  }

  override def close() = {
    geometry.delete()
  }
}