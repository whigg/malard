package com.earthwave.environment.api

import play.api.libs.json.{Format, Json}

/**
  * The CatalogueFilter message class.
  */
case class Environment( name : String
                        , maskPublisherPath : String
                        , cacheCdfPath : String
                        , pointCdfPath : String
                        , mongoConnection : String
                        , swathIntermediatePath : String
                        , deflateLevel : Int
                        , serverVersion : String
                        )

object Environment
{
  implicit val format : Format[Environment] = Json.format[Environment]
}
