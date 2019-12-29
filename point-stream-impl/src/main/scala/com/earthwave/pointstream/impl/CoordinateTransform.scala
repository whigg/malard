package com.earthwave.pointstream.impl

import com.earthwave.projection.api.Projection
import ucar.ma2.DataType


case class TransformedCoordinates( x : Array[Double], y : Array[Double], proj : Array[String])
{

  def getVariableDefinitions() : List[WriterColumn.Column] = {

    List[WriterColumn.Column]( WriterColumn.Column("x", 16, DataType.DOUBLE), WriterColumn.Column("y", 16, DataType.DOUBLE) )
  }

  def getX() ={ "x" }

  def getY() = {"y"}

}


case class ProjectionMapped( shortName : String, conditions : List[Condition])

object CoordinateTransform {


  def transformArray( lat : ucar.ma2.Array, lon : ucar.ma2.Array, projections : List[Projection], cache : CoordinateTransformCache ) : Option[TransformedCoordinates] ={

    val projectionsMapped = projections.map( p => (ProjectionMapped(p.shortName, Condition.parseConditions(p.conditions)),p))

    val latlon = lat.getSize.toInt
    val xArray = new Array[Double](latlon)
    val yArray = new Array[Double](latlon)
    val projArray = new Array[String](latlon)

    try{
      for( i <- 0 until latlon )
      {
        val lt = lat.getDouble(i)
        val ln = lon.getDouble(i)

        if( !lt.isNaN() && !ln.isNaN() ) {
          val shortName = projectionsMapped.filter(c => c._1.conditions.length == c._1.conditions.filter(c => c.op(lt)).length).head

          val coordTransformation = cache.get(shortName._2)
          val transform = new TransformToXY(lt, ln, coordTransformation)

          val x = (transform.getX())
          val y = (transform.getY())

          xArray(i) = x
          yArray(i) = y
          projArray(i) = shortName._1.shortName
        }
        else{
          xArray(i) = Double.NaN
          yArray(i) = Double.NaN
        }
      }
    }
    catch {
      case e : Exception => return None
      }

    Some(TransformedCoordinates(xArray, yArray, projArray))
  }


}
