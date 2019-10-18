package com.earthwave.pointstream.impl

import com.earthwave.catalogue.api.BoundingBoxFilter
import org.gdal.ogr.{Geometry, Layer, ogrConstants}
import ucar.ma2.DataType

import scala.collection.mutable.ListBuffer

object ArrayHelper {

  def checkInMask( layer : List[(Boolean,Layer)], x : Double, y : Double ) : Boolean={

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

    val includePoint = layer.map( x => inMask(x._2,x._1) ).reduce( (x,y) => if( x == true && y == true ){true}else{false}  )

    includePoint

  }

  def buildMask( xArr : ucar.ma2.Array, yArr : ucar.ma2.Array, tArr : ucar.ma2.Array, bbf : BoundingBoxFilter, f : List[(Operator,ucar.ma2.Array)], layer : List[(Boolean,Layer)] ) : Array[Int]={

    val shapeFile = !bbf.maskFilters.isEmpty
    val mask = new ListBuffer[Int]()
    val numberOfFilters = f.length

    for( i <- 0 until xArr.getSize.toInt )
    {
      val x = xArr.getDouble(i)
      val y = yArr.getDouble(i)
      val t = tArr.getDouble(i)

      if( x >= bbf.minX && x <= bbf.maxX && y >= bbf.minY && y <= bbf.maxY && t >= bbf.minT && t <= bbf.maxT ) {
        val filterRes = f.map(x => x._1.op(x._2.getDouble(i))).filter(res => res == true)
        if( numberOfFilters == filterRes.length ) {
          if( shapeFile )
          {
            if( checkInMask(layer, x, y)) {
              mask.append(i)
            }
          }
          else
          {
            mask.append(i)
          }
        }
      }
    }

    mask.toArray
  }

  def applyMask( dataType : DataType, src : ucar.ma2.Array, mask : Array[Int] ) : ucar.ma2.Array = {

    val dt = dataType
    val length = mask.length

    val origin = new Array[Int](length)

    if( dt == DataType.DOUBLE )
    {
      val array = new Array[Double](mask.length)

      for( j <- 0 until length )
      {
         array(j) = src.getDouble(mask(j))
      }

      return ucar.ma2.Array.factory(array)
    }
    else if( dt == DataType.FLOAT )
    {
      val array = new Array[Float](mask.length)

      for( j <- 0 until length )
      {
        array(j) = src.getFloat(mask(j))
      }

      return ucar.ma2.Array.factory(array)
    }
    else if( dt == DataType.LONG )
    {
      val array = new Array[Long](mask.length)

      for( j <- 0 until length )
      {
        array(j) = src.getLong(mask(j))
      }

      return ucar.ma2.Array.factory(array)
    }
    else if( dt == DataType.INT )
    {
      val array = new Array[Int](mask.length)

      for( j <- 0 until length )
      {
        array(j) = src.getInt(mask(j))
      }

      return ucar.ma2.Array.factory(array)
    }
    else if( dt == DataType.SHORT )
    {
      val array = new Array[Short](mask.length)

      for( j <- 0 until length )
      {
        array(j) = src.getShort(mask(j))
      }

      return ucar.ma2.Array.factory(array)
    }
    else if( dt == DataType.STRING || dt == DataType.OBJECT) {
      val array = new Array[String](mask.length)

      for (j <- 0 until length) {
        array(j) = src.getObject(mask(j)).toString
      }
      return ucar.ma2.Array.makeArray(DataType.STRING, array)
    }

    throw new Exception(s"Unexpected column type: ${dt.toString}")
  }

  def convertArray( src : ucar.ma2.Array, dt : DataType ) : ucar.ma2.Array = {

    val length = src.getSize.toInt

    if( dt == DataType.DOUBLE )
    {
      val array = new Array[Double](length)

      for( j <- 0 until length )
      {
        array(j) = src.getDouble(j)
      }

      return ucar.ma2.Array.factory(array)
    }
    else if( dt == DataType.FLOAT )
    {
      val array = new Array[Float](length)

      for( j <- 0 until length )
      {
        array(j) = src.getDouble(j).toFloat
      }

      return ucar.ma2.Array.factory(array)
    }
    else if( dt == DataType.LONG )
    {
      val array = new Array[Long](length)

      for( j <- 0 until length )
      {
        array(j) = src.getDouble(j).toLong
      }

      return ucar.ma2.Array.factory(array)
    }
    else if( dt == DataType.INT )
    {
      val array = new Array[Int](length)

      for( j <- 0 until length )
      {
        array(j) = src.getDouble(j).toInt
      }

      return ucar.ma2.Array.factory(array)
    }
    else if( dt == DataType.SHORT )
    {
      val array = new Array[Short](length)

      for( j <- 0 until length )
      {
        array(j) = src.getDouble(j).toShort
      }

      return ucar.ma2.Array.factory(array)
    }

    throw new Exception(s"Unsupported conversion from DataType: ${dt.toString}")
  }

}
