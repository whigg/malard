package com.earthwave.catalogue.impl

import java.time.{Instant, ZoneId, ZoneOffset}
import java.util.Date

import com.earthwave.catalogue.api.{GridCell, GridCellFile, SwathDetail}
import org.bson.BsonArray
import org.mongodb.scala.Document

import scala.collection.mutable.ListBuffer

object SwathDetailImpl {

  def fromDocument( doc : Document ): SwathDetail = {
    val gcsDocs: BsonArray = doc.get("gridCells").get.asArray()

    val buffer = new ListBuffer[GridCellFile]()

    for (i <- 0 until gcsDocs.size()) {
      val doc = gcsDocs.get(i).asDocument()
      buffer.append(GridCellFile(doc.getInt64("t").longValue()
                                , doc.getString("projection").getValue
                                , doc.getInt64("x").longValue()
                                , doc.getInt64("y").longValue()
                                , doc.getInt64("pointCount").longValue()
                                , doc.getString("intermediateName").getValue))
    }

    SwathDetail(  doc.getString("datasetName")
      , doc.getString("region")
      , doc.getString("catalogueName")
      , doc.getString("swathName")
      , doc.getInteger("swathId")
      , doc.getInteger("year")
      , doc.getInteger("month")
      , doc.getInteger("swathPointCount")
      , doc.getInteger("filteredSwathPointCount")
      , Instant.ofEpochMilli(  doc.getDate("insertTime").getTime ).atZone(ZoneId.of("UTC")).toLocalDateTime
      , buffer.toList)
  }

  def toDocument(s : SwathDetail) : Document ={
    val gridCells = s.gridCells.map( g => Document( "projection" -> g.projection,
      "x" -> g.x,
      "y" -> g.y,
      "pointCount" -> g.pointCount,
      "t" -> g.t,
      "intermediateName" -> g.fileName) )

    val doc = Document( "datasetName" -> s.datasetName,
      "region" -> s.region,
      "catalogueName" -> s.catalogueName,
      "swathName" -> s.swathName,
      "swathId" -> s.swathId,
      "year" -> s.year,
      "month" -> s.month,
      "swathPointCount" -> s.swathPointCount,
      "filteredSwathPointCount" -> s.filteredSwathPointCount,
      "insertTime" -> new Date( 1000* s.insertTime.toEpochSecond(ZoneOffset.UTC )),
      "gridCells" -> gridCells)

    doc
  }
}
