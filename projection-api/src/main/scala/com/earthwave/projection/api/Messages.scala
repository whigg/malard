package com.earthwave.projection.api

import play.api.libs.json.{Format, Json}

case class Projection( shortName : String, proj4 : String  )

object Projection
{
  implicit val format : Format[Projection] = Json.format[Projection]
}