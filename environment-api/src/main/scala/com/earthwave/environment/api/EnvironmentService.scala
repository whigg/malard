package com.earthwave.environment.api

import akka.NotUsed
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

/**
  * The Environment service interface.
  * <p>
  * This describes everything that Lagom needs to know about how to serve and
  * consume the EnvironmentService.
  */
trait EnvironmentService extends Service {

  def createEnvironment( envName : String ) : ServiceCall[Environment,String]

  def getEnvironment(name : String) : ServiceCall[NotUsed,Environment]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("environment")
      .withCalls(
        pathCall("/env/createenvironment/:envname", createEnvironment _),
        pathCall("/env/getenvironment/:name", getEnvironment _)
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

