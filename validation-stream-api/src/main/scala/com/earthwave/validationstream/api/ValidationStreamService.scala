package com.earthwave.validationstream.api

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

/**
  * The Validation stream interface.
  *
  * This describes everything that Lagom needs to know about how to serve and
  * consume the ValidationStream service.
  */
trait ValidationStreamService extends Service {

  def validateNetCdfs: ServiceCall[ValidationRequest, Source[ValidationStatus, NotUsed]]

  override final def descriptor: Descriptor = {
    import Service._

    named("validation-stream")
      .withCalls(
        namedCall("validateasync", validateNetCdfs)
      ).withAutoAcl(true)
  }
}

