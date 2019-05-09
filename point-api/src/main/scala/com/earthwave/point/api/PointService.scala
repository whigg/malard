package com.earthwave.point.api

import com.earthwave.catalogue.api._
import com.earthwave.point.api.Messages.Query
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

/**
  * The Point service interface.
  * <p>
  * This describes everything that Lagom needs to know about how to serve and
  * consume the PointService.
  */
trait PointService extends Service {

  def getGeoJson( parentDSName : String, dsName : String ) : ServiceCall[BoundingBoxFilter,Messages.FeatureCollection]

  def getNetCdfFile( parentDsName : String, dsName : String ) : ServiceCall[BoundingBoxFilter,String]

  def executeQuery( parentDsName : String, dsName : String) : ServiceCall[Query,String]

  def getDataSetColumns(parentDsName: String, dsName: String): ServiceCall[BoundingBoxFilter, Messages.Columns]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("point")
      .withCalls(
        pathCall("/point/getgeojson/:parent/:dsname", getGeoJson _ ),
        pathCall("/point/netcdffile/:parent/:dsname", getNetCdfFile _ ),
        pathCall("/point/datasetcolumns/:parent/:dsname", getDataSetColumns _ ),
        pathCall("/point/query/:parent/:dsname", executeQuery _)
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}




