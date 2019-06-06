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

  def getProjection( shortName : String ) : ServiceCall[NotUsed, Projection]
  def publishProjection() : ServiceCall[Projection, String]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("projection")
      .withCalls(
        pathCall("/projection/getprojection/:shortName", getProjection _ ),
        pathCall("/projection/publishprojection", publishProjection _ )
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

