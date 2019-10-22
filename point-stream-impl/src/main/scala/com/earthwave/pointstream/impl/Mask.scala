package com.earthwave.pointstream.impl


import com.earthwave.catalogue.api.{BoundingBoxFilter, MaskFilter}
import org.gdal.ogr.ogr
import org.gdal.ogr.{Driver, Layer, Geometry, ogrConstants, DataSource,FieldDefn,Feature}

object Mask
{
  def getMask( bbf : BoundingBoxFilter, driver : Driver, inmemDriver : Driver ) : Option[Mask] ={

    if( bbf.extentFilter.shapeFile.isEmpty && bbf.extentFilter.wkt.isEmpty && bbf.maskFilters.isEmpty )
    {
       None
    }
    else
    {
      Some( new CompositeMask(bbf, driver, inmemDriver) )
    }
  }
}


trait Mask {


  def getExtent() : (Double, Double, Double, Double )

  def checkInMask( x : Double, y : Double ) : Boolean

  def close()
}

class CompositeMask( bbf : BoundingBoxFilter, driver : Driver, inmemDriver : Driver ) extends Mask
{
  val boxEmpty = if( bbf.minX == 0.0 && bbf.maxX == 0.0 && bbf.minY ==0.0 && bbf.maxY == 0.0){true}else{false}
  val extentFilter: Option[Mask] =  if( boxEmpty && !bbf.extentFilter.shapeFile.isEmpty )
                      {
                          Some(new ShapeMask( bbf.extentFilter, bbf.minX, bbf.maxX, bbf.minY, bbf.maxY, driver, inmemDriver ))
                      }
                      else if( boxEmpty && !bbf.extentFilter.wkt.isEmpty  )
                      {
                          Some(new GeometryMask( bbf.extentFilter ) )
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
    if( boxEmpty )
    {
      extentFilter.get.getExtent()
    }
    else
    {
      ( bbf.minX, bbf.maxX, bbf.minY, bbf.maxY )
    }
  }

  override def checkInMask( x : Double, y : Double ) : Boolean = {

    if( boxEmpty && !extentFilter.get.checkInMask(x,y) )
    {
      false
    }
    else
    {
       filters.map( f => f.checkInMask(x,y) ).reduce( (x,y) => if( x == true && y == true){ true }else{false} )
    }
  }

  override def close() = {

    if(boxEmpty){ extentFilter.get.close() }

    filters.foreach(f => f.close())

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

      //open the memory datasource with write access
      val tmp=inmemDriver.Open("memData",1)

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

      val result_layer = source.CreateLayer("clippedmask")

      layer.Clip( outLayer, result_layer  )

      ring.delete()
      poly.delete()
      idField.delete()
      layer.delete()
      source.delete()

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



  override def close() = {
    geometry.delete()
  }
}