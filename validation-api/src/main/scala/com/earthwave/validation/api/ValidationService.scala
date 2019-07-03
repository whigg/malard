package com.earthwave.validation.api

import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}


/**
  * The Projection service interface.
  * <p>
  * This describes everything that Lagom needs to know about how to serve and
  * consume the CatalogueService.
  */
trait ValidationService extends Service {

  def validate() : ServiceCall[ValidationRequest, ValidationErrors]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("validation")
      .withCalls(
        pathCall("/validation/validate", validate _ )
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

