package com.earthwave.pointstream.impl

import java.io.File

import akka.NotUsed
import akka.actor.Status.Success
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.Source
import com.earthwave.catalogue.api.CatalogueService
import com.earthwave.environment.api.EnvironmentService
import com.earthwave.pointstream.api._
import com.lightbend.lagom.scaladsl.api.ServiceCall

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import com.earthwave.pointstream.impl.PublisherMessages.PublishGridCellPoints
import com.earthwave.pointstream.impl.QueryManagerMessages.{IsCompleted, ProcessQuery}


/**
  * Implementation of the PointStreamService.
  */
class PointStreamServiceImpl(catalogue : CatalogueService, env : EnvironmentService, implicit val system : ActorSystem) extends PointStreamService {

  val queryManager = system.actorOf(Props(new QueryManager(catalogue)), "QueryManager")
  val pointPublisher = system.actorOf(Props(new PointPublisher(catalogue)), "PointPublisher")


  implicit val ec = ExecutionContext.global

  def runQuery(ref: ActorRef, q: StreamQuery): Future[NotUsed] = {
    Future {
      implicit val timeout = Timeout(10 seconds)
      println(s"ParentDS=[${q.parentDSName}], DataSet=[${q.dsName}] MinTime=[${q.bbf.minT}] MaxTime=[${q.bbf.maxT}]")

      val outputPath = Await.result(env.getEnvironment(q.envName).invoke(), 10 seconds).cacheCdfPath

      println(s"From environment ${q.envName} output path is $outputPath.")

      val projection = q.projections.foldLeft[String]("")((x, y) => x + "_" + y)
      val filters = q.filters.foldLeft[String]("")((x, y) => x + "_" + y.column + y.op + y.threshold)
      val fileNameHash = s"${q.bbf.minX}_${q.bbf.maxX}_${q.bbf.minY}_${q.bbf.maxY}_${q.bbf.minT}_${q.bbf.maxT}${projection}${filters}".hashCode
      val fileName = s"${outputPath}${q.parentDSName}_${q.dsName}_${fileNameHash}.nc"
      val cacheCheck = new File(fileName)

      println(s"doQuery $fileName")

      if (!cacheCheck.exists()) {
        queryManager ! ProcessQuery(fileName, q)

        var completed = false
        while (!completed) {
          Thread.sleep(1000)
          val status = Await.result((queryManager ? IsCompleted(fileName)).mapTo[QueryStatus], 10 seconds)
          println(s"Sending status to client")
          ref ! status
          completed = status.completed
        }
      }
      else {
        ref ! QueryStatus(true, fileName, "Success")
      }
      ref ! Success()
      NotUsed
    }
  }


  def doQuery() = ServiceCall[StreamQuery, Source[QueryStatus, NotUsed]] { query =>
    implicit val mat = ActorMaterializer()
    val source: (ActorRef, Source[QueryStatus, NotUsed]) = Source.actorRef[QueryStatus](1000, OverflowStrategy.fail).preMaterialize()

    runQuery(source._1, query)

    Future.successful(source._2)
  }

  private def doPublish(resultStream: ActorRef, request: PublishRequest) = {
    Future {
      implicit val timeout = Timeout(10 seconds)

      val environment = Await.result( env.getEnvironment(request.envName).invoke(), 10 seconds)

      pointPublisher ! PublishGridCellPoints(request, environment)

      var completed = false
      while (!completed) {
        Thread.sleep(1000)
        val status = Await.result((pointPublisher ? PublisherMessages.IsCompleted(request)).mapTo[PublisherStatus], 10 seconds)
        println(s"Publish points sending status to client")
        resultStream ! status
        completed = status.completed
      }
      resultStream ! Success()
      NotUsed
    }
  }

  def doPublishGridCellPoints() = ServiceCall[PublishRequest, Source[PublisherStatus, NotUsed]] { request =>
    implicit val mat = ActorMaterializer()
    val source: (ActorRef, Source[PublisherStatus, NotUsed]) = Source.actorRef[PublisherStatus](1000, OverflowStrategy.fail).preMaterialize()

    doPublish(source._1, request)

    Future.successful(source._2)
  }


}
