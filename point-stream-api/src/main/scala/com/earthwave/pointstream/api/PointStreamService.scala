package com.earthwave.pointstream.api

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

import scala.concurrent.Future


/**
  * The point stream interface.
  *
  * This describes everything that Lagom needs to know about how to serve and
  * consume the PointStream service.
  */
trait PointStreamService extends Service {

  def doQuery(): ServiceCall[StreamQuery, Source[QueryStatus,NotUsed]]

  def doPublishGridCellPoints() : ServiceCall[PublishRequest, Source[PublisherStatus,NotUsed]]

  def publishSwathToGridCells() : ServiceCall[SwathToGridCellsRequest, Source[SwathProcessorStatus, NotUsed]]

  def releaseCache() : ServiceCall[List[Cache], Source[String, NotUsed]]

  override final def descriptor: Descriptor = {
    import Service._

    named("point-stream")
      .withCalls(
        namedCall("query", doQuery),
        namedCall( "publish", doPublishGridCellPoints),
        namedCall( "publishswath", publishSwathToGridCells),
        namedCall("releasecache", releaseCache())

      ).withAutoAcl(true)
  }
}

