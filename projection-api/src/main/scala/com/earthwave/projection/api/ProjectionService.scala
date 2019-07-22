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

  def getProjection( envName : String , shortName : String ) : ServiceCall[NotUsed, Projection]
  def publishProjection( envName : String) : ServiceCall[Projection, String]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("projection")
      .withCalls(
        pathCall("/projection/getprojection/:envName/:shortName", getProjection _ ),
        pathCall("/projection/publishprojection/:envName", publishProjection _ )
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

