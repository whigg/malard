package com.earthwave.pointstream.impl


import com.earthwave.catalogue.api.{BoundingBoxFilter, MaskFilter}
import org.gdal.ogr.ogr
import org.gdal.ogr.{Driver, Layer, Geometry, ogrConstants, DataSource,FieldDefn,Feature}

object Mask
{
  def getMask( bbf : BoundingBoxFilter, driver : Driver, inmemDriver : Driver ) : Option[Mask] ={

    val filters = bbf.maskFilters

    if( !filters.isEmpty )
    {
      if( !filters.head.shapeFile.isEmpty )
      {
        Some(new ShapeMask( bbf, driver, inmemDriver ))
      }
      else
      {
        Some(new GeometryMask( bbf ))
      }
    }
    else
    {
      None
    }

  }
}


trait Mask {


  def getExtent() : (Double, Double, Double, Double )

  def checkInMask( x : Double, y : Double ) : Boolean

  def close()
}



class ShapeMask( bbf : BoundingBoxFilter, driver : Driver, inmemDriver : Driver ) extends Mask{

  val boxEmpty = if( bbf.minX == 0.0 && bbf.maxX == 0.0 && bbf.minY ==0.0 && bbf.maxY == 0.0){true}else{false}

  def createLayer( f : MaskFilter ) : ( Boolean, Layer, DataSource ) =
  {
    if( boxEmpty)
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
      ring.AddPoint(bbf.minX,bbf.minY)
      ring.AddPoint(bbf.minX, bbf.maxY)
      ring.AddPoint(bbf.maxX, bbf.maxY)
      ring.AddPoint(bbf.maxX, bbf.minY)
      ring.AddPoint(bbf.minX, bbf.minY)
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

      (f.includeWithin, result_layer, inmemsource)
    }

  }

  val filters = bbf.maskFilters
  val layers = filters.map( mf => { createLayer( mf )
  })

  val ls = layers.map( l => (l._1, l._2) )

  override def getExtent(): (Double, Double, Double, Double) = {
    val boxEmpty = if( bbf.minX == 0.0 && bbf.maxX == 0.0 && bbf.minY ==0.0 && bbf.maxY == 0.0){true}else{false}

    val extent = if(boxEmpty) {
      val extents = layers.map(f => {
        f._2.GetExtent()
      })

      val extent = extents.reduce((x, y) => {
        val array = new Array[Double](4)
        array(0) = Math.min(x(0), y(0))
        array(1) = Math.max(x(1), y(1))
        array(2) = Math.min(x(2), y(2))
        array(3) = Math.max(x(3), y(3))
        array
      })
      ( extent(0), extent(1), extent(2), extent(3) )
    }
    else
    {
      ( bbf.minX, bbf.maxX, bbf.minY, bbf.maxY )
    }
    extent
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

    val includePoint = ls.map( x => inMask(x._2,x._1) ).reduce( (x,y) => if( x == true && y == true ){true}else{false}  )

    includePoint

  }

  override def close() ={
    layers.foreach(l => {
      l._2.delete()
      l._3.delete()
    })

  }

}

class GeometryMask( bbf: BoundingBoxFilter ) extends Mask
{
  val geometries = bbf.maskFilters.map(f => ogr.CreateGeometryFromWkt(f.wkt))

  override def getExtent(  ): (Double, Double, Double, Double) = {
    val boxEmpty = if( bbf.minX == 0.0 && bbf.maxX == 0.0 && bbf.minY ==0.0 && bbf.maxY == 0.0){true}else{false}

    val extent = if(boxEmpty) {
      val extents = geometries.map(f => { val data = new Array[Double](4)
        f.GetEnvelope(data)
        data
      })

      val extent = extents.reduce((x, y) => {
        val array = new Array[Double](4)
        array(0) = Math.min(x(0), y(0))
        array(1) = Math.max(x(1), y(1))
        array(2) = Math.min(x(2), y(2))
        array(3) = Math.max(x(3), y(3))
        array
      })
      ( extent(0), extent(1), extent(2), extent(3) )
    }
    else
    {
      ( bbf.minX, bbf.maxX, bbf.minY, bbf.maxY )
    }
    extent
  }

  override def checkInMask(x: Double, y: Double): Boolean = {

    val pt: Geometry = new Geometry(ogrConstants.wkbPoint)
    pt.SetPoint_2D(0, x, y)

    val ret = geometries.map(g => pt.Within(g) ).reduce((x,y) => if( x == true && y == true){true}else{false} )

    pt.delete()
    ret
  }



  override def close() = {
    geometries.foreach(g => g.delete() )
  }
}