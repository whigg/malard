package com.earthwave.pointstream.api

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.earthwave.point.api.Messages.Query
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}


/**
  * The point stream interface.
  *
  * This describes everything that Lagom needs to know about how to serve and
  * consume the PointStream service.
  */
trait PointStreamService extends Service {

  def executeQuery( envName : String, parentDataSet : String, dataSetName : String ): ServiceCall[Source[Query,NotUsed], Source[String, NotUsed]]

  override final def descriptor: Descriptor = {
    import Service._

    named("point-stream")
      .withCalls(
        pathCall("/pointstream/query/:envName/:parent/:dsname", executeQuery _)
      ).withAutoAcl(true)
  }
}

