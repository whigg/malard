package com.earthwave.mask.api

import play.api.libs.json.{Format, Json}

case class GridCell(minX : Long, minY : Long, size : Long)

object GridCell
{
  implicit val format : Format[GridCell] = Json.format[GridCell]
}

case class GridCellMask( gridCell : GridCell, shapeFile : String )

object GridCellMask
{
  implicit val format : Format[GridCellMask] = Json.format[GridCellMask]
}

case class MaskFile( sourceFilePath : String, fileName : String, gridCell : GridCell )

object MaskFile
{
  implicit val format : Format[MaskFile] = Json.format[MaskFile]
}

case class Mask( `type` : String, region : String )

object Mask
{
  implicit val format : Format[Mask] = Json.format[Mask]
}

