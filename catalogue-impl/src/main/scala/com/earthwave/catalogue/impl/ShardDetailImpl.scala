package com.earthwave.catalogue.impl

import com.earthwave.catalogue.api.CatalogueElement
import org.mongodb.scala.Document

object ShardDetailImpl
{
  def fromDocument( doc : Document ): CatalogueElement= {

    val dsName: String = doc.getString("dsName")
    val region: String = doc.getString("region")
    val shardName: String = doc.getString("shardName")
    val projection: String = doc.getString("projection")
    val year: Int = doc.getInteger("year")
    val month: Int = doc.getInteger("month")
    val gridCellMinX: Long = doc.getLong("gridCellMinX")
    val gridCellMaxX: Long = doc.getLong("gridCellMaxX")
    val gridCellMinY: Long = doc.getLong("gridCellMinY")
    val gridCellMaxY: Long = doc.getLong("gridCellMaxY")
    val gridCellSize: Long = doc.getLong("gridCellSize")
    val minX: Double = doc.getDouble("minX")
    val maxX: Double = doc.getDouble("maxX")
    val minY: Double = doc.getDouble("minY")
    val maxY: Double = doc.getDouble("maxX")
    val minLat: Double = doc.getDouble("minLat")
    val maxLat: Double = doc.getDouble("maxLat")
    val minLon: Double = doc.getDouble("minLon")
    val maxLon: Double = doc.getDouble("maxLon")
    val minTime: Double = doc.getDouble("minTime")
    val maxTime: Double = doc.getDouble("maxTime")
    val count: Long = doc.getLong("count")

    CatalogueElement(dsName
      , region
      , shardName
      , projection
      , year
      , month
      , gridCellMinX
      , gridCellMaxX
      , gridCellMinY
      , gridCellMaxY
      , gridCellSize
      , minX
      , maxX
      , minY
      , maxY
      , minLat
      , maxLat
      , minLon
      , maxLon
      , minTime
      , maxTime
      , count
      )
  }

  def toDocument( element : CatalogueElement) : Document ={

    Document(
      "dsName" -> element.dsName,
      "region" -> element.region,
      "shardName" -> element.shardName,
      "projection" -> element.projection,
      "month" -> element.month,
      "year" -> element.year,
      "gridCellMinX" -> element.gridCellMinX,
      "gridCellMaxX" -> element.gridCellMaxX,
      "gridCellMinY" -> element.gridCellMinY,
      "gridCellMaxY" -> element.gridCellMaxY,
      "gridCellSize" -> element.gridCellSize,
      "minX" -> element.minX,
      "maxX" -> element.maxX,
      "minY" -> element.minY,
      "maxY" -> element.maxY,
      "minLat" -> element.minLat,
      "maxLat" -> element.maxLat,
      "minLon" -> element.minLon,
      "maxLon" -> element.maxLon,
      "minTime" -> element.minTime ,
      "maxTime" -> element.maxTime,
      "count" -> element.count)
  }
}
