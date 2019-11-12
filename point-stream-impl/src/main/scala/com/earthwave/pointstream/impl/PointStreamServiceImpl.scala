package com.earthwave.pointstream.impl

import java.io.File

import com.earthwave.catalogue.api.{BoundingBox, BoundingBoxFilter, CatalogueService, Shard, MaskFilter}
import com.earthwave.environment.api.EnvironmentService
import com.earthwave.pointstream.api._
import com.lightbend.lagom.scaladsl.api.ServiceCall

import akka.NotUsed
import akka.actor.Status.Success
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.Source
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import com.earthwave.pointstream.impl.PublisherMessages.PublishGridCellPoints
import com.earthwave.pointstream.impl.QueryManagerMessages.{IsCompleted, ProcessQuery}
import com.earthwave.pointstream.impl.SwathGridCellPublisher.WorkerSwathToGridCellRequest
import com.earthwave.projection.api.ProjectionService
import org.slf4j.LoggerFactory

import scala.util.Try
import org.gdal.ogr.{Layer, ogr}

/**
  * Implementation of the PointStreamService.
  */
class PointStreamServiceImpl(catalogue : CatalogueService, env : EnvironmentService, projectionService : ProjectionService, implicit val system : ActorSystem) extends PointStreamService {

  ogr.RegisterAll()

  val queryManager = system.actorOf(Props(new QueryManager(catalogue, system)), "QueryManager")
  val pointPublisher = system.actorOf(Props(new PointPublisher(catalogue, system)), "PointPublisher")
  val swathProcessor = system.actorOf(Props(new SwathProcessor(catalogue, system)), "SwathProcessor")

  private val log = LoggerFactory.getLogger(PointStreamServiceImpl.super.getClass)

  implicit val ec = ExecutionContext.global

  def runQuery(ref: ActorRef, q: StreamQuery): Future[NotUsed] = {
    Future {
      implicit val timeout = Timeout(10 seconds)
      log.info(s"ParentDS=[${q.parentDSName}], DataSet=[${q.dsName}] MinTime=[${q.bbf.minT}] MaxTime=[${q.bbf.maxT}]")

      try {
        val outputPath = Await.result(env.getEnvironment(q.envName).invoke(), 10 seconds).cacheCdfPath

        log.info(s"From environment ${q.envName} output path is $outputPath.")


        log.info(s"BB passed in MinX=${q.bbf.minX} MaxX=${q.bbf.maxX} MinY=${q.bbf.minY} MaxX=${q.bbf.maxY}")

        val mask = Mask.getMask(q.bbf )

        val extent = mask.getExtent()

        log.info(s"Extent MinX=${extent._1} MaxX=${extent._2} MinY=${extent._3} MaxY=${extent._4}")

        mask.close()

        val bbf = BoundingBoxFilter(extent._1, extent._2, extent._3, extent._4 ,q.bbf.minT,q.bbf.maxT, q.bbf.xCol, q.bbf.yCol, q.bbf.extentFilter, q.bbf.maskFilters)


        val projection = q.projections.foldLeft[String]("")((x, y) => x + "_" + y)
        val filters = q.filters.foldLeft[String]("")((x, y) => x + "_" + y.column + y.op + y.threshold)
        val fileNameHash = s"${bbf.minX}_${bbf.maxX}_${bbf.minY}_${bbf.maxY}_${q.bbf.minT}_${q.bbf.maxT}${projection}${filters}${q.bbf.maskFilters}${q.bbf.xCol}${q.bbf.yCol}".hashCode
        val fileName = s"${outputPath}${q.parentDSName}_${q.dsName}_${fileNameHash}.nc"
        val cacheCheck = new File(fileName)

        val future = catalogue.shards(q.envName, q.parentDSName, q.dsName, q.region).invoke(bbf)
        val shards = Await.result(future, 10 seconds)

        log.info(s"doQuery $fileName")

        if (!cacheCheck.exists() && !shards.isEmpty) {

          val qMod = StreamQuery(q.envName,q.parentDSName,q.dsName,q.region,bbf,q.projections,q.filters)

          queryManager ! ProcessQuery(fileName, qMod, shards)

          var completed = false
          while (!completed) {
            Thread.sleep(1000)
            val status = Await.result((queryManager ? IsCompleted(fileName)).mapTo[QueryStatus], 10 seconds)
            log.info(s"Sending status to client [$fileName] Status=[${status.status}] ")
            ref ! status
            completed = status.completed
          }
        }
        else if (shards.isEmpty) {
          val msg = s"No shards found for bounding box ParentDS=[${q.parentDSName}], DataSet=[${q.dsName}] X=[${q.bbf.minX}] Y=[${q.bbf.minY}] MinTime=[${q.bbf.minT}] MaxTime=[${q.bbf.maxT}]"
          log.error( msg )
          ref ! QueryStatus(true, "Error: No file", "Error", msg )
        }
        else {
          ref ! QueryStatus(true, fileName, "Success","Published Successfully")
        }
      }
      catch {
        case e : Exception => {
          val msg = s"Error querying bounding box ${e.getMessage}"
          log.error( msg )
          ref ! QueryStatus(true, "Error: No file", "Error", msg)
        }
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

      try {
        val environment = Await.result(env.getEnvironment(request.envName).invoke(), 10 seconds)

        pointPublisher ! PublishGridCellPoints(request, environment)

        var completed = false
        while (!completed) {
          Thread.sleep(1000)
          val status = Await.result((pointPublisher ? PublisherMessages.IsCompleted(request)).mapTo[PublisherStatus], 10 seconds)
          println(s"Publish points sending status to client")
          resultStream ! status
          completed = status.completed
        }
      }
      catch {
        case e : Exception => {
          val msg = s"Publish of ${request.gcps.fileName.head} failed with ${e.getMessage}"
          log.error(msg)
          resultStream ! PublisherStatus(true, msg, "Failure", request.hash)
        }
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

  override def publishSwathToGridCells(): ServiceCall[SwathToGridCellsRequest, Source[SwathProcessorStatus, NotUsed]] = { request =>

    implicit val mat = ActorMaterializer()
    val source: (ActorRef, Source[SwathProcessorStatus, NotUsed]) = Source.actorRef[SwathProcessorStatus](1000, OverflowStrategy.fail).preMaterialize()

    doPublishSwathToGridCells(source._1, request)

    Future.successful(source._2)
  }

  def doPublishSwathToGridCells( resultStream : ActorRef, request : SwathToGridCellsRequest ) = {
    Future{
      implicit val timeout = Timeout(10 seconds)

      try {
        val environment = Await.result(env.getEnvironment(request.envName).invoke(), 10 seconds)

        val projections = Await.result(projectionService.getProjection(request.envName, request.parentDataSet, request.region).invoke(), 10 seconds)

        val fileId = request.inputFileName.hashCode()

        val workerRequest = WorkerSwathToGridCellRequest(fileId, request, environment, List(projections))

        log.info(s"Creating worker request for ${request.inputFileName} with fileId ${fileId}")

        swathProcessor ! workerRequest

        var completed = false
        while (!completed) {
          Thread.sleep(1000)
          log.info("Checking for status")
          val status = Await.result((swathProcessor ? SwathProcessorMessages.IsCompleted(workerRequest.userRequest.inputFileName, workerRequest.userRequest.hash)).mapTo[SwathProcessorStatus], 10 seconds)
          log.info(s"Swath Processor sending status to client ${workerRequest.userRequest.inputFileName} [${status.status}] [${status.message}] ")
          if (status.completed && status.status.contentEquals("Success")) {
            val catalogueWrite = Try(Await.result(catalogue.publishSwathDetails(request.envName, request.parentDataSet).invoke(status.swathDetails.head), 10 seconds))

            catalogueWrite match {
              case scala.util.Success(_) => {
                log.info(s"Swath Details published to Mongo successfully for ${request.inputFileName}.")
                resultStream ! status
              }
              case scala.util.Failure(e) => {
                val msg = s"Swath details failed to publish to Mongo for ${request.inputFileName} with error ${e.getMessage}."
                log.error(s"Swath details failed to publish to Mongo for ${request.inputFileName} with error ${e.getMessage}.")
                resultStream ! SwathProcessorStatus(true, request.inputFileName, "Error", msg, None, request.hash)
              }
            }
          }
          else {
            resultStream ! status
          }
          completed = status.completed
        }
      }
      catch {
        case e : Exception => {
          log.error(s"Exception occurred processing file ${request.inputFileName} ${e.getMessage}")
          resultStream ! SwathProcessorStatus(true, request.inputFileName, "Error", e.getMessage, None, request.hash)
        }
      }

      log.info(s"Completing stream for ${request.inputFileName}.")
      resultStream ! Success()
      NotUsed
    }
  }

  override def filterShards() : ServiceCall[StreamQuery, Source[Shard , NotUsed]] = { query =>

    Future.successful(Source.apply(getShards(query)).mapAsync(8)( s => Future{s} ))

  }

  override def filterGridCells(): ServiceCall[StreamQuery, Source[BoundingBox, NotUsed]] = { query =>

    Future.successful(Source.apply(getGridCells(query)).mapAsync(8)( gc => Future{gc} ))
  }

  override def releaseCache() : ServiceCall[List[Cache], Source[String, NotUsed]] = { c =>

    def deleteFile( fileName : String ) = {
      Future {
        val file = new java.io.File(fileName)

        if (file.exists()) {
          println(s"Attempting to delete ${fileName}")
          file.delete()
        }
        s"File deleted successfully [${fileName}]"
      }
    }

    Future.successful(Source.apply(c).mapAsync(8)( f => deleteFile(f.handle) ))
  }

  private def getShards( q : StreamQuery ) : List[Shard] = {

    val mask = Mask.getMask(q.bbf)

    val extent = mask.getExtent()

    log.info(s"Extent MinX=${extent._1} MaxX=${extent._2} MinY=${extent._3} MaxY=${extent._4}")

    val bbf = BoundingBoxFilter(extent._1, extent._2, extent._3,extent._4 ,q.bbf.minT,q.bbf.maxT, q.bbf.xCol, q.bbf.yCol, q.bbf.extentFilter, q.bbf.maskFilters)

    val future = catalogue.shards(q.envName, q.parentDSName, q.dsName, q.region).invoke(bbf)
    val shards : List[Shard] = Await.result(future, 10 seconds)

    val filteredShards = shards.filter( s => inMask(mask, s.minX, s.maxX, s.minY, s.maxY))

    log.info(s"After filtering shards: ${filteredShards.length}")

    mask.close()

    filteredShards
  }

  private def getGridCells( q : StreamQuery ) : List[BoundingBox] = {

    val mask = Mask.getMask(q.bbf)

    val extent = mask.getExtent( )

    val bbf = BoundingBoxFilter(extent._1, extent._2, extent._3,extent._4 ,q.bbf.minT,q.bbf.maxT, q.bbf.xCol, q.bbf.yCol, q.bbf.extentFilter, q.bbf.maskFilters)

    val future = catalogue.boundingBoxQuery(q.envName, q.parentDSName, q.dsName, q.region).invoke(bbf)
    val gridcells : List[BoundingBox] = Await.result(future, 10 seconds)

    val filteredGCs = gridcells.filter( s => inMask(mask, s.gridCellMinX.toDouble, s.gridCellMaxX.toDouble, s.gridCellMinY.toDouble, s.gridCellMaxY.toDouble))

    log.info(s"After filtering grid cells: ${filteredGCs.length}")

    mask.close()

    filteredGCs
  }

  private def inMask(mask : Mask, minX : Double, maxX : Double, minY : Double, maxY : Double) : Boolean = {

    val numPoints = 10
    val width = (maxX - minX)/numPoints
    val height = (maxY - minY)/numPoints
    val start_x = 0.5 * width + minX
    val start_y = 0.5 * height + minY

    val x_range = Array.range(start_x.toInt, maxX.toInt, width.toInt)
    val y_range = Array.range(start_y.toInt, maxY.toInt, height.toInt)

    if( mask.checkInMask( minX, minY )){ return true }
    if( mask.checkInMask( minX, maxY )){return true}
    if( mask.checkInMask( maxX, maxY )){return true}
    if( mask.checkInMask( maxX, minY )){return true}

    x_range.foreach( x => y_range.foreach( y =>
                                                {
                                                  if( mask.checkInMask(x,y))
                                                  {
                                                    return true
                                                  }
                                                }) )
    return false
  }

  override def filterGriddedPoints() : ServiceCall[GridCellPointRequest, Source[PointInMask, NotUsed]] ={ gcpr =>

    val mask = Mask.getMask( BoundingBoxFilter(   gcpr.minX
                                                , gcpr.maxX
                                                , gcpr.minY
                                                , gcpr.maxY
                                                , 0
                                                , 0
                                                , "x"
                                                , "y"
                                                , MaskFilter("","",false), gcpr.maskFilters)
                                                , driver, inmemDriver)

    def getPoints(): List[(Double,Double)] ={
      val start_x = 0.5 * gcpr.resolution + gcpr.minX
      val start_y = 0.5 * gcpr.resolution + gcpr.minY

      val x_range = Array.range(start_x.toInt, gcpr.maxX.toInt, gcpr.resolution)
      val y_range = Array.range(start_y.toInt, gcpr.maxY.toInt, gcpr.resolution)

      x_range.map( x => y_range.map(y => (x.toDouble,y.toDouble)) ).toList.flatten
    }

    def isPointinMask( point : (Double, Double) ) : Future[PointInMask] = {
      Future {
        val inmask = mask.checkInMask(point._1, point._2)
        PointInMask(point._1, point._2, inmask)
      }
    }

    Future.successful(Source.apply(getPoints()).mapAsync(1)( p => isPointinMask(p) ))

  }
}
