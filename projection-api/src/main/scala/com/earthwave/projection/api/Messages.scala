package com.earthwave.projection.api

import play.api.libs.json.{Format, Json}

case class Projection( shortName : String, proj4 : String  )

object Projection
{
  implicit val format : Format[Projection] = Json.format[Projection]
}

case class ProjectionMapping( parentDataSetName : String, region : String, shortName : String)

object ProjectionMapping
{
  implicit  val format : Format[ProjectionMapping] = Json.format[ProjectionMapping]
}