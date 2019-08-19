package com.earthwave.catalogue.api

import java.time.{LocalDate, LocalDateTime}
import java.util.Date

import play.api.libs.json.{Format, Json}

/**
  * The CatalogueFilter message class.
  */
case class CatalogueElement(  dsName : String
                              , region : String
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
                              , minTime : Double
                              , maxTime : Double
                              , count : Long
                           )

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

case class DataSetRegion( name: String, region : String )

object DataSetRegion
{
  implicit val format: Format[DataSetRegion] = Json.format[DataSetRegion]
}
case class DataSet( name: String )

object DataSet
{
  implicit val format: Format[DataSet] = Json.format[DataSet]
}



case class BoundingBox( gridCellMinX : Long
                        , gridCellMaxX : Long
                        , gridCellMinY : Long
                        , gridCellMaxY : Long
                        , minTime : Double
                        , maxTime : Double
                        , totalPoints : Long
                        , numberOfShards : Long)

object BoundingBox
{
  implicit val format : Format[BoundingBox] = Json.format[BoundingBox]
}

case class BoundingBoxFilter( minX : Double
                              , maxX : Double
                              , minY : Double
                              , maxY : Double
                              , minT : Double
                              , maxT : Double
                              , xCol: String
                              , yCol: String)

object BoundingBoxFilter
{
  implicit val format : Format[BoundingBoxFilter] = Json.format[BoundingBoxFilter]
}

case class Shard(shardName : String, minX : Double, maxX : Double, minY : Double, maxY : Double, minT : Double, maxT : Double, numberOfPoints: Long)

object Shard{
  implicit val format :Format[Shard] = Json.format[Shard]
}

case class SwathDetail(   datasetName :String
                        , region : String
                        , catalogueName : String
                        , swathName : String
                        , swathId : Int
                        , year : Int
                        , month : Int
                        , swathPointCount : Int
                        , filteredSwathPointCount : Int
                        , insertTime : LocalDateTime
                        , gridCells : List[GridCellFile])

object SwathDetail
{
  implicit val format : Format[SwathDetail] = Json.format[SwathDetail]
}

case class GridCell(projection : String, x : Long, y : Long, pointCount : Long)

object GridCell
{
  implicit val format : Format[GridCell] = Json.format[GridCell]
}

case class GridCellFile(t : Long, projection : String, x : Long, y : Long, pointCount : Long, fileName : String)

object GridCellFile
{
  implicit val format : Format[GridCellFile] = Json.format[GridCellFile]
}

