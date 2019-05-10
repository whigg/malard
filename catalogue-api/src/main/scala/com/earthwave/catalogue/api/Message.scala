package com.earthwave.catalogue.api

import java.util.Date

import play.api.libs.json.{Format, Json}

/**
  * The CatalogueFilter message class.
  */
case class CatalogueFilter( dbName : String, dsName : Option[String], year : Option[Int], month : Option[Int], lat : Option[Double], lon :Option[Double], x : Option[Double], y: Option[Double] )

/**
  * The CatalogueFilter message class.
  */
case class CatalogueElement(  dsName : String
                              , shardName : String
                              , projection : String
                              , year : Int
                              , month : Int
                              , gridCellMinX : Long
                              , gridCellMaxX : Long
                              , gridCellMinY : Long
                              , gridCellMaxY : Long
                              , gridCellSize : Long
                              , minX : Double
                              , maxX : Double
                              , minY : Double
                              , maxY : Double
                              , minLat : Double
                              , maxLat : Double
                              , minLon : Double
                              , maxLon : Double
                              , minTime : Date
                              , maxTime : Date
                              , count : Long
                              , qualityCount : Long
                           )

object CatalogueFilter {
  /**
    * Format for converting CatalogueFilter messages to and from JSON.
    *
    * This will be picked up by a Lagom implicit conversion from Play's JSON format to Lagom's message serializer.
    */
  implicit val format: Format[CatalogueFilter] = Json.format[CatalogueFilter]
}

object CatalogueElement {
  /**
    * Format for converting CatalogueElement messages to and from JSON.
    *
    * This will be picked up by a Lagom implicit conversion from Play's JSON format to Lagom's message serializer.
    */
  implicit val format: Format[CatalogueElement] = Json.format[CatalogueElement]
}

case class Catalogue( catalogueElements : List[CatalogueElement]  )

object Catalogue
{
  implicit val format: Format[Catalogue] = Json.format[Catalogue]
}

case class DataSet( name: String )

object DataSet
{
  implicit val format: Format[DataSet] = Json.format[DataSet]
}

case class DataSets( dataSets : Seq[DataSet] )

object DataSets
{
  implicit val format: Format[DataSets] = Json.format[DataSets]
}

case class BoundingBox( gridCellMinX : Long
                        , gridCellMaxX : Long
                        , gridCellMinY : Long
                        , gridCellMaxY : Long
                        , minTime : Date
                        , maxTime : Date
                        , totalPoints : Long
                        , numberOfShards : Long )

object BoundingBox
{
  implicit val format : Format[BoundingBox] = Json.format[BoundingBox]
}

case class BoundingBoxes( boxes : Seq[BoundingBox] )

object BoundingBoxes
{
  implicit val format : Format[BoundingBoxes] = Json.format[BoundingBoxes]
}

case class BoundingBoxFilter( minX : Long
                              , maxX : Long
                              , minY : Long
                              , maxY : Long
                              , minT : Date
                              , maxT : Date)

object BoundingBoxFilter
{
  implicit val format : Format[BoundingBoxFilter] = Json.format[BoundingBoxFilter]
}

case class Shard(shardName : String, minX : Double, maxX : Double, minY : Double, maxY : Double, minT : Date, maxT : Date, numberOfPoints: Long)

object Shard{
  implicit val format :Format[Shard] = Json.format[Shard]
}

case class Shards( shards : Seq[Shard] )

object Shards
{
  implicit val format : Format[Shards] = Json.format[Shards]
}

case class SwathDetails( swathDetails : List[SwathDetail] )

object SwathDetails
{
  implicit  val format : Format[SwathDetails] = Json.format[SwathDetails]
}

case class SwathDetail( swathName : String, pointCount : Long, gridCells : List[GridCell] )

object SwathDetail
{
  implicit val format : Format[SwathDetail] = Json.format[SwathDetail]
}

case class GridCell(projection : String, x : Long, y : Long, pointCount : Long)

object GridCell
{
  implicit val format : Format[GridCell] = Json.format[GridCell]
}