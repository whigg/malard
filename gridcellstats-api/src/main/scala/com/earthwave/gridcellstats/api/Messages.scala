package com.earthwave.gridcellstats.api

import com.earthwave.mask.api.GridCell
import play.api.libs.json.{Format, Json}

case class GridCellStatistics( gridCell : GridCell, statistics : Map[String,Double] )

object GridCellStatistics
{
  implicit val format : Format[GridCellStatistics] = Json.format[GridCellStatistics]
}

