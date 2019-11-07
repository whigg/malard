package com.earthwave.pointstream.api


import com.earthwave.catalogue.api.{BoundingBoxFilter, SwathDetail}
import play.api.libs.json.{Format, Json}


case class StreamQuery( envName : String, parentDSName : String, dsName : String, region : String, bbf : BoundingBoxFilter, projections : List[String], filters : List[Filter] )

object StreamQuery
{
  implicit val format : Format[StreamQuery] = Json.format[StreamQuery]
}

case class PublishRequest(envName : String, parentDsName: String, dsName: String, region : String, gcps : GridCellPoints, hash : Long)

object PublishRequest
{
  implicit val format : Format[PublishRequest] = Json.format[PublishRequest]
}

case class QueryStatus( completed : Boolean, cacheName : String, status : String, message : String )

object QueryStatus
{
  implicit val format : Format[QueryStatus] = Json.format[QueryStatus]
}

case class PublisherStatus( completed : Boolean, message : String, status : String, hash : Long )

object PublisherStatus
{
  implicit val format : Format[PublisherStatus] = Json.format[PublisherStatus]
}

case class SwathToGridCellsRequest( envName : String, parentDataSet : String, dataSet : String, region : String, inputFileName : String, inputFilePath : String, dataTime : Long, columnFilters : List[Filter], includeColumns : Set[String], gridCellSize : Long, hash : Long )

object SwathToGridCellsRequest
{
  implicit val format : Format[SwathToGridCellsRequest] = Json.format[SwathToGridCellsRequest]
}

case class GridCellPoints( projection : String, minX : Long, minY : Long, minT : Long, size : Long, fileName : List[String] )

object GridCellPoints
{
  implicit val format : Format[GridCellPoints] = Json.format[GridCellPoints]
}

case class Filter( column : String, op : String, threshold : Double )

object Filter
{
  implicit val format : Format[Filter] = Json.format[Filter]
}

case class Query( bbf : BoundingBoxFilter, projection : List[String], filters : List[Filter] )

object Query
{
  implicit val format : Format[Query] = Json.format[Query]
}

case class SwathProcessorStatus( completed : Boolean, inputFileName : String, status : String, message : String, swathDetails : Option[SwathDetail], hash : Long )

object SwathProcessorStatus {
  implicit val format: Format[SwathProcessorStatus] = Json.format[SwathProcessorStatus]
}

case class Cache( handle : String )

case class Column( name : String )

case class Columns( column : List[Column] )

//Serialisation helpers.
object Cache
{
  implicit val format : Format[Cache] = Json.format[Cache]
}

object Column
{
  implicit val format : Format[Column] = Json.format[Column]
}

object Columns
{
  implicit val format : Format[Columns] = Json.format[Columns]
}
