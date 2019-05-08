package com.earthwave.point.api

import com.earthwave.catalogue.api._
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}
import play.api.libs.json._

/**
  * The Point service interface.
  * <p>
  * This describes everything that Lagom needs to know about how to serve and
  * consume the PointService.
  */
trait PointService extends Service {

  def getGeoJson( parentDSName : String, dsName : String ) : ServiceCall[BoundingBoxFilter,FeatureCollection]

  def getNetCdfFile( parentDsName : String, dsName : String ) : ServiceCall[BoundingBoxFilter,String]

  def getDataSetColumns(parentDsName: String, dsName: String): ServiceCall[BoundingBoxFilter, Messages.Columns]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("point")
      .withCalls(
        pathCall("/point/getgeojson/:parent/:dsname", getGeoJson _ ),
        pathCall("/point/netcdffile/:parent/:dsname", getNetCdfFile _ ),
        pathCall("/point/datasetcolumns/:parent/:dsname", getDataSetColumns _ )
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

case class Geometry(`type` : String, coordinates : List[Double] )


case class Feature(`type` : String, geometry : Geometry, properties: Map[String,Double]  )

case class FeatureCollection(`type` : String, features: List[Feature] )

object Geometry{
  implicit val format : Format[Geometry] = Json.format[Geometry]
}

object Feature{
  implicit val format : Format[Feature] = Json.format[Feature]
}

object FeatureCollection{
  implicit val format : Format[FeatureCollection] = Json.format[FeatureCollection]
}


