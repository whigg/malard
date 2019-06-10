package com.earthwave.environment.api

import play.api.libs.json.{Format, Json}

/**
  * The CatalogueFilter message class.
  */
case class Environment( name : String, maskPublisherPath : String, cacheCdfPath : String, pointCdfPath : String )

object Environment
{
  implicit val format : Format[Environment] = Json.format[Environment]
}
