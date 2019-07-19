package com.earthwave.point.api

import akka.NotUsed
import com.earthwave.catalogue.api._
import com.earthwave.point.api.Messages.{Cache, GridCellPoints, Query}
import com.lightbend.lagom.scaladsl.api.Service.pathCall
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

/**
  * The Point service interface.
  * <p>
  * This describes everything that Lagom needs to know about how to serve and
  * consume the PointService.
  */
trait PointService extends Service {

  def getGeoJson( parentDSName : String, dsName : String, region : String ) : ServiceCall[BoundingBoxFilter,Messages.FeatureCollection]

  def getNetCdfFile( envName : String, parentDsName : String, dsName : String, region : String ) : ServiceCall[BoundingBoxFilter,String]

  def executeQuery( envName : String, parentDsName : String, dsName : String, region : String) : ServiceCall[Query,String]

  def releaseCache() : ServiceCall[Cache,String]

  def getDataSetColumns(parentDsName: String, dsName: String, region : String): ServiceCall[BoundingBoxFilter, Messages.Columns]

  def publishGridCellPoints( envName : String, parentDsName : String, dsName : String, region : String ) : ServiceCall[GridCellPoints, String]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("point")
      .withCalls(
        pathCall("/point/getgeojson/:parent/:dsname/:region", getGeoJson _ ),
        pathCall("/point/netcdffile/:envName/:parent/:dsname/:region", getNetCdfFile _ ),
        pathCall("/point/datasetcolumns/:parent/:dsname/:region", getDataSetColumns _ ),
        pathCall("/point/query/:envName/:parent/:dsname/:region", executeQuery _),
        pathCall("/point/releasecache", releaseCache()),
        pathCall("/point/publishgridcellpoints/:envName/:parent/:dsname/:region", publishGridCellPoints _ )
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}




