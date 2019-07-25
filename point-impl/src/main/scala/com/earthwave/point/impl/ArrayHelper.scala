package com.earthwave.point.impl

import com.earthwave.catalogue.api.BoundingBoxFilter
import ucar.ma2.DataType

import scala.collection.mutable.ListBuffer

object ArrayHelper {


  def buildMask( xArr : ucar.ma2.Array, yArr : ucar.ma2.Array, tArr : ucar.ma2.Array, bbf : BoundingBoxFilter, f : List[(Operator,ucar.ma2.Array)] ) : Array[Int]={

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
          mask.append(i)
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
