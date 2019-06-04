package com.earthwave.environment.api

import akka.NotUsed
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

/**
  * The Catalogue service interface.
  * <p>
  * This describes everything that Lagom needs to know about how to serve and
  * consume the CatalogueService.
  */
trait EnvironmentService extends Service {

  def setEnvironment() : ServiceCall[Environment,String]

  def getEnvironment() : ServiceCall[NotUsed,Environment]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("environment")
      .withCalls(
        pathCall("/env/setenvironment", setEnvironment _),
        pathCall("/env/getenvironment", getEnvironment _)
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

