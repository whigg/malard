package com.earthwave.point.api

import com.earthwave.catalogue.api.BoundingBoxFilter
import play.api.libs.json.{Format, Json}

object Messages {

  case class Cache( handle : String )

  case class Filter( column : String, op : String, threshold : Double )

  case class Query( bbf : BoundingBoxFilter, projection : List[String], filters : List[Filter] )

  case class Column( name : String )

  case class Columns( column : List[Column] )

  case class Geometry(`type` : String, coordinates : List[Double] )

  case class Feature(`type` : String, geometry : Geometry, properties: Map[String,Double]  )

  case class FeatureCollection(`type` : String, features: List[Feature] )

  case class GridCellPoints( projection : String, minX : Long, minY : Long, size : Long, fileName : String )

  //Serialisation helpers.
  object Cache
  {
    implicit val format : Format[Cache] = Json.format[Cache]
  }

  object Column
  {
    implicit val format : Format[Column] = Json.format[Column]
  }

  object Filter
  {
    implicit val format : Format[Filter] = Json.format[Filter]
  }

  object Query
  {
    implicit val format : Format[Query] = Json.format[Query]
  }

  object Columns
  {
    implicit val format : Format[Columns] = Json.format[Columns]
  }

  object Geometry{
    implicit val format : Format[Geometry] = Json.format[Geometry]
  }

  object Feature{
    implicit val format : Format[Feature] = Json.format[Feature]
  }

  object FeatureCollection{
    implicit val format : Format[FeatureCollection] = Json.format[FeatureCollection]
  }

  object GridCellPoints
  {
    implicit val format : Format[GridCellPoints] = Json.format[GridCellPoints]
  }
}
