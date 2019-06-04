package com.earthwave.environment.api

import play.api.libs.json.{Format, Json}

/**
  * The CatalogueFilter message class.
  */
case class Environment( name : String, outputCdfPath : String, publisherPath : String )

object Environment
{
  implicit val format : Format[Environment] = Json.format[Environment]
}
