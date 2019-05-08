package com.earthwave.point.api

import com.earthwave.catalogue.api.BoundingBoxFilter
import play.api.libs.json.{Format, Json}

object Messages {

  case class Aggregate( fieldName : String, op : String )

  case class Query( bbf : BoundingBoxFilter, aggs : List[Aggregate] )

  case class Column( name : String )

  case class Columns( column : List[Column] )

  object Column
  {
    implicit val format : Format[Column] = Json.format[Column]
  }

  object Columns
  {
    implicit val format : Format[Columns] = Json.format[Columns]
  }


}
