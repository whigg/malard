package com.earthwave.pointstream.impl

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}

import akka.actor.{Actor, ActorPath, ActorSystem, Props, Terminated}
import com.earthwave.catalogue.api.{CatalogueElement, CatalogueService}
import com.earthwave.environment.api.Environment
import com.earthwave.pointstream.api.{GridCellPoints, PublishRequest, PublisherStatus}
import com.earthwave.pointstream.impl.Messages.InitiatingConnection
import com.earthwave.pointstream.impl.PublisherMessages.PublishGridCellPoints
import com.earthwave.pointstream.impl.WriterColumn.Column
import ucar.ma2.DataType
import ucar.nc2.Variable
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object PublisherMessages
{
  case class PublishGridCellPoints( publishRequest : PublishRequest, environment: Environment )
  case class IsCompleted( publishRequest : PublishRequest  )
  case class Completed( s : PublisherStatus, publishRequest: PublishRequest, catalogueElement: CatalogueElement)
}

class PointPublisher( catalogueService: CatalogueService, system : ActorSystem) extends Actor {

  private val log = LoggerFactory.getLogger(PointPublisher.super.getClass)

  val publisherProcesses = List.range[Int](1,5,1).map( x => system.actorOf(Props(new Publisher(x)),s"PublisherProcessor_$x"))

  publisherProcesses.foreach( pp => context.watch(pp) )

  publisherProcesses.foreach(a => a ! InitiatingConnection())

  var availableProcessors = publisherProcesses

  var retries = Map[String,Int]()

  var queuedRequests = List[PublishGridCellPoints]()
  var processingRequests = List[(PublishGridCellPoints,ActorPath)]()
  var completedRequests = List[PublisherStatus]()
  override def receive ={
    case Messages.WorkerConnected() => {
      if( retries.contains( sender.path.name))
      {
        retries.-=(sender.path.name)
      }
    }
    case q : PublishGridCellPoints => {
      log.info(s"PointPublisher Queued Requests [${queuedRequests.length}] Available Processors [${availableProcessors.length}] ProcessingRequests [${processingRequests.length}] Completed Requests [${completedRequests.length}]  ")

      if( availableProcessors.isEmpty )
      {
        queuedRequests = q :: queuedRequests
      }
      else
      {
        val processor = availableProcessors.head
        availableProcessors = availableProcessors.tail
        processingRequests = (q,processor.path) :: processingRequests
        processor ! q
      }
    }
    case c : PublisherMessages.IsCompleted => {
      val gcp = c.publishRequest.gcps
      val req = c.publishRequest

      log.info("Received is completed request")
      log.info( s"Workers available:[${availableProcessors.length}] Processing Requests: [${processingRequests.length}]" )
      if (availableProcessors.isEmpty && processingRequests.isEmpty)
      {
        //Bad news all the workers have stopped and are not available.
        sender ! PublisherStatus(true, "No available workers", s"Error", c.publishRequest.hash)
      }
      else if( !queuedRequests.filter(p => c.publishRequest.hash == p.publishRequest.hash).isEmpty )
      {
        val message = s"PDS=${req.parentDsName} DS=${req.dsName} Region=${req.region} X=${gcp.minX}, Y=${gcp.minY} T=${gcp.minT}. ${c.publishRequest.hash}"
        log.info(s"$message is queued.")
        sender ! PublisherStatus(false, message, s"Queued", c.publishRequest.hash )

        if( !availableProcessors.isEmpty)
        {
          val processor = availableProcessors.head
          val request = queuedRequests.filter(p => c.publishRequest.hash == p.publishRequest.hash).head
          queuedRequests = queuedRequests.filterNot(p => c.publishRequest.hash == p.publishRequest.hash)
          availableProcessors = availableProcessors.tail
          processingRequests = (request, processor.path) :: processingRequests

          processor ! request
        }
      }
      else if( !processingRequests.filter(p => c.publishRequest.hash == p._1.publishRequest.hash).isEmpty )
      {
        val message = s"PDS=${req.parentDsName} DS=${req.dsName} Region=${req.region} X=${gcp.minX}, Y=${gcp.minY} T=${gcp.minT}. ${c.publishRequest.hash}"
        log.info(s"$message is processing.")

        sender ! PublisherStatus(false, message , "Processing.", c.publishRequest.hash)
      }
      else if( !completedRequests.filter(p =>c.publishRequest.hash == p.hash).isEmpty )
      {
        val message = s"PDS=${req.parentDsName} DS=${req.dsName} Region=${req.region} X=${gcp.minX}, Y=${gcp.minY} T=${gcp.minT}. ${c.publishRequest.hash}"
        log.info(s"${message} is completed")
        val publisherStatus = completedRequests.filter(p => c.publishRequest.hash == p.hash).head
        sender ! publisherStatus
      }

    }
    case c : PublisherMessages.Completed => {
      Await.result(catalogueService.publishCatalogueElement(c.publishRequest.envName, c.publishRequest.parentDsName,c.publishRequest.dsName).invoke(c.catalogueElement), 10 seconds )
      log.info(s"Completed ${c.s.message}")
      log.info(s"PointPublisher Queued Requests [${queuedRequests.length}] Available Processors [${availableProcessors.length}] ProcessingRequests [${processingRequests.length}] Completed Requests [${completedRequests.length}]  ")

      completedRequests = c.s :: completedRequests
      processingRequests = processingRequests.filterNot(r => r._1.publishRequest.hash == c.publishRequest.hash )

      if( !queuedRequests.isEmpty )
      {
        val nextRequest = queuedRequests.head
        queuedRequests = queuedRequests.tail
        processingRequests = (nextRequest, sender.path) :: processingRequests
        sender ! nextRequest
      }
      else
      {
        availableProcessors = sender :: availableProcessors
      }
    }
    case Terminated( pp ) =>{
      log.error(s"Detected termination of ${pp.path.name}. ")

      availableProcessors = availableProcessors.filterNot(f => f.path == pp.path)
      log.info(s"Number of available processors ${availableProcessors.length}.")

      val requests = processingRequests.filter( f => f._2 == pp.path )

      if( !requests.isEmpty )
      {
        val request = requests.head._1
        log.info(s"Request ${request.publishRequest.hash} is going to be queued because the process whas terminated.")
        queuedRequests = request :: queuedRequests
      }

      processingRequests = processingRequests.filterNot(f => f._2 == pp.path)

      implicit val executionContext = ExecutionContext.global

      Future {
        Thread.sleep(60 * 1000)
        val retryCount = retries.getOrElse(pp.path.name, 0)
        if (retryCount <= Messages.maxRetryCount) {

          val instance = pp.path.name.split("_")(1)
          val actor = system.actorOf(Props(new Publisher(instance.toInt)), pp.path.name)
          log.info(s"Creating actor [${pp.path.name}] Instance [$instance].")
          context.watch(actor)
          actor ! Messages.InitiatingConnection()
          availableProcessors = actor :: availableProcessors

          retries.+=((actor.path.name,retryCount+1))
        }
      }
    }
    case _ => { log.error("Unsupported message type received.") }
  }
}

object Helper
{
  def max( lhs: Double, rhs : Double ):Double={
    if(lhs.isNaN){ rhs }else{ Math.max(lhs,rhs)}
  }

  def min( lhs: Double, rhs : Double ):Double={
    if(lhs.isNaN){ rhs }else{ Math.min(lhs,rhs)}
  }
}

class Publisher( val processNr : Int) extends Actor {

  private val log = LoggerFactory.getLogger(Publisher.super.getClass)

  override def receive ={

    case Messages.InitiatingConnection() =>
    {
      log.info(s"Worker [$processNr] Received Initiating connection.")
      sender ! Messages.WorkerConnected()
    }
    case p : PublishGridCellPoints =>{
      val gcp = p.publishRequest.gcps
      val message = s"X=[${gcp.minX}] Y=[${gcp.minY}] Size=[${gcp.size}, FileNames=${gcp.fileName.foldLeft("")((x,y)=> s"$x $y" )}"
      log.info(s"Received publishGridCellPoints request $message")

      val outputBasePath = p.environment.pointCdfPath
      log.info(s"PointCdf output base path for ${p.environment.name} is $outputBasePath ")

      val date = LocalDateTime.ofEpochSecond(p.publishRequest.gcps.minT, 0, ZoneOffset.UTC)

      val now = LocalDateTime.now(ZoneOffset.UTC)
      // LocalDateTime to epoch seconds
      val seconds = now.atZone(ZoneOffset.UTC).toEpochSecond()
      val shardPath = s"${outputBasePath}/${p.publishRequest.dsName}/gridcell/y${date.getYear}/m${date.getMonthValue}/cell_${gcp.projection}_${gcp.minX}_${gcp.minY}/"
      val shardName = s"${shardPath}GridCell_${processNr}_$seconds.nc"


      def createDir( path : String) : Unit=
      {
        val dir = new File(path)
        if(!dir.exists()){
          dir.mkdirs()
        }
      }
      createDir(shardPath)

      val schema = Map[String, DataType]( "lon" ->	DataType.DOUBLE,
        "lat" ->	DataType.DOUBLE,
        "elev" -> DataType.FLOAT,
        "heading" ->	DataType.FLOAT,
        "demDiff" ->	DataType.FLOAT,
        "demDiffMad" -> 	DataType.FLOAT,
        "demDiffMad2" ->	DataType.FLOAT,
        "phaseAmb" -> DataType.SHORT,
        "meanDiffSpread" -> 	DataType.FLOAT,
        "wf_number" -> 	DataType.SHORT,
        "sampleNb" -> 	DataType.SHORT,
        "power"	 -> DataType.FLOAT,
        "powerdB" ->	DataType.FLOAT,
        "phase"	-> DataType.FLOAT,
        "phaseS" -> 	DataType.FLOAT,
        "phaseSSegment" ->	DataType.FLOAT,
        "phaseConfidence" ->	DataType.FLOAT,
        "coh"	-> DataType.FLOAT,
        "x"	->DataType.DOUBLE,
        "y"	->DataType.DOUBLE,
        "time"	-> DataType.LONG,
        "index" -> DataType.LONG,
        "swathFileId" ->	DataType.INT
      )

      val tmpreader = new NetCdfReader(gcp.fileName.head, Set[String]())
      val writer = new NetCdfWriter(shardName, tmpreader.getVariables().map( x => WriterColumn.Column(x.getShortName, 0, x.getDataType)),List[Column](),List[Column](),schema)
      tmpreader.close()

      val readers = gcp.fileName.map( file => new NetCdfReader(file, Set[String]()) )

      val catalogueAndData = readers.map( r => {
                                                  val vars = r.getVariables().map(v => (v, v.read()))
                                                  val ce = createCatalogueElement( vars, shardName, date, gcp, p.publishRequest.dsName, p.publishRequest.region )
                                                  (vars,ce)
                                                }
                                                )

      catalogueAndData.foreach( data => writer.write( data._1))

      val catalogueElement = catalogueAndData.foldLeft[CatalogueElement](createEmptyCatalogueElement(catalogueAndData.head._2))((ce,curr) => {
                                                                                                                   val currCe = curr._2
                                                                                                                   CatalogueElement(ce.dsName
                                                                                                                                    , ce.region
                                                                                                                                    , ce.shardName
                                                                                                                                    , ce.projection
                                                                                                                                    , ce.year
                                                                                                                                    , ce.month
                                                                                                                                    , ce.gridCellMinX
                                                                                                                                    , ce.gridCellMaxX
                                                                                                                                    , ce.gridCellMinY
                                                                                                                                    , ce.gridCellMaxY
                                                                                                                                    , ce.gridCellSize
                                                                                                                                    , Helper.min(ce.minX, currCe.minX)
                                                                                                                                    , Helper.max(ce.maxX, currCe.maxX)
                                                                                                                                    , Helper.min(ce.minY, currCe.minY)
                                                                                                                                    , Helper.max(ce.maxX, currCe.maxX )
                                                                                                                                    , Helper.min(ce.minLat, currCe.minLat)
                                                                                                                                    , Helper.max(ce.maxLat, currCe.maxLat)
                                                                                                                                    , Helper.min(ce.minLon, currCe.minLon )
                                                                                                                                    , Helper.max(ce.maxLon, currCe.maxLon)
                                                                                                                                    , Helper.min(ce.minTime, currCe.minTime )
                                                                                                                                    , Helper.max(ce.maxTime, currCe.maxTime)
                                                                                                                                    , ce.count + currCe.count)
                                                                                                                })
      readers.foreach(reader => reader.close())
      writer.close()

      sender ! PublisherMessages.Completed(PublisherStatus(true, message, s"Successfully published.", p.publishRequest.hash ), p.publishRequest, catalogueElement)
    }
  }

  private def createCatalogueElement( variablesAndData : List[(Variable, ucar.ma2.Array)], shardName : String, date : LocalDateTime, gcp : GridCellPoints, dsName : String, region : String ): CatalogueElement={

    def getDataForVariable( name : String ): ucar.ma2.Array = {

      val ret = variablesAndData.filter( x => x._1.getShortName() == name  ).headOption

      ret.getOrElse(throw new Exception(s"Mandatory category column $name is missing from inputCdf"))._2
    }

    val x = getDataForVariable( "x" )
    val y = getDataForVariable("y")
    val lat = getDataForVariable("lat")
    val lon = getDataForVariable("lon")
    val t = getDataForVariable("time")

    var xMin,  yMin,  latMin, lonMin = Double.NaN
    var xMax, yMax, latMax, lonMax = Double.NaN
    var tMin, tMax = Double.NaN

    val count = x.getSize.toInt

    log.info("Starting to calc maxes and mins")

    for( i <- 0 until count)
    {
      xMin = Helper.min(xMin, x.getDouble(i))
      xMax = Helper.max(xMax, x.getDouble(i))
      yMin = Helper.min(yMin, y.getDouble(i))
      yMax = Helper.max(yMax, y.getDouble(i))
      latMin = Helper.min(latMin, lat.getDouble(i))
      latMax = Helper.max(latMax, lat.getDouble(i))
      lonMin = Helper.min(lonMin, lon.getDouble(i))
      lonMax = Helper.max(lonMax, lon.getDouble(i))
      tMin = Helper.min(tMin, t.getDouble(i))
      tMax = Helper.max(tMax, t.getDouble(i))
    }

    log.info("Completed calculating maxes and mins")

    CatalogueElement( dsName,
      region,
      shardName,
      gcp.projection,
      date.getYear,
      date.getMonthValue,
      gcp.minX,
      gcp.minX + gcp.size,
      gcp.minY,
      gcp.minY + gcp.size,
      gcp.size,
      xMin,
      xMax,
      yMin,
      yMax,
      latMin,
      latMax,
      lonMin,
      lonMax,
      tMin,
      tMax,
      count
    )
  }

  def createEmptyCatalogueElement( ce: CatalogueElement) : CatalogueElement={
    CatalogueElement( ce.dsName,
      ce.region,
      ce.shardName,
      ce.projection,
      ce.year,
      ce.month,
      ce.gridCellMinX,
      ce.gridCellMaxX,
      ce.gridCellMinY,
      ce.gridCellMaxY,
      ce.gridCellSize,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      0
    )

  }

}
