package com.earthwave.projection.api

import akka.NotUsed
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

/**
  * The Projection service interface.
  * <p>
  * This describes everything that Lagom needs to know about how to serve and
  * consume the CatalogueService.
  */
trait ProjectionService extends Service {

  def getProjectionFromShortName( envName : String , shortName : String ) : ServiceCall[NotUsed, Projection]

  def getProjection( envName : String , parentDsName : String, region : String ) : ServiceCall[NotUsed, Projection]

  def publishProjection( envName : String) : ServiceCall[Projection, String]

  def publishRegionMapping( envName : String ): ServiceCall[ProjectionMapping, String]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("projection")
      .withCalls(
        pathCall("/projection/getprojectionfromshortname/:envName/:shortName", getProjectionFromShortName _ ),
        pathCall("/projection/getprojection/:envName/:parentDsName/:region", getProjection _ ),
        pathCall("/projection/publishprojection/:envName", publishProjection _ ),
        pathCall("/projection/publishregionmapping/:envName", publishRegionMapping _ ))
      .withAutoAcl(true)
    // @formatter:on
  }
}

