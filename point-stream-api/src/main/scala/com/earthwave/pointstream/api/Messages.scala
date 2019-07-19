package com.earthwave.pointstream.api

import com.earthwave.catalogue.api.BoundingBoxFilter
import com.earthwave.point.api.Messages.{Filter, GridCellPoints}
import play.api.libs.json.{Format, Json}


case class StreamQuery( envName : String, parentDSName : String, dsName : String, region : String, bbf : BoundingBoxFilter, projections : List[String], filters : List[Filter] )

object StreamQuery
{
  implicit val format : Format[StreamQuery] = Json.format[StreamQuery]
}

case class PublishRequest(envName : String, parentDsName: String, dsName: String, region : String, gcps : GridCellPoints)

object PublishRequest
{
  implicit val format : Format[PublishRequest] = Json.format[PublishRequest]
}

case class QueryStatus( completed : Boolean, cacheName : String, status : String )

object QueryStatus
{
  implicit val format : Format[QueryStatus] = Json.format[QueryStatus]
}

case class PublisherStatus( completed : Boolean, fileName : String, status : String )

object PublisherStatus
{
  implicit val format : Format[PublisherStatus] = Json.format[PublisherStatus]
}

