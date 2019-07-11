package com.earthwave.pointstream.api

import com.earthwave.catalogue.api.BoundingBoxFilter
import com.earthwave.point.api.Messages.Filter
import play.api.libs.json.{Format, Json}


case class StreamQuery( envName : String, parentDSName : String, dsName : String, bbf : BoundingBoxFilter, projections : List[String], filters : List[Filter] )

object StreamQuery
{
  implicit val format : Format[StreamQuery] = Json.format[StreamQuery]
}
