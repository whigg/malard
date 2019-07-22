package com.earthwave.pointstream.impl

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}

import akka.actor.{Actor, Props}
import com.earthwave.catalogue.api.{CatalogueElement, CatalogueService}
import com.earthwave.environment.api.{Environment}
import com.earthwave.point.api.Messages.GridCellPoints
import com.earthwave.point.impl.{NetCdfReader, NetCdfWriter, WriterColumn}
import com.earthwave.pointstream.api.{PublishRequest, PublisherStatus}
import com.earthwave.pointstream.impl.PublisherMessages.PublishGridCellPoints
import ucar.ma2.DataType
import ucar.nc2.Variable

import scala.concurrent.Await
import scala.concurrent.duration._

object PublisherMessages
{
  case class PublishGridCellPoints( publishRequest : PublishRequest, environment: Environment )
  case class IsCompleted( publishRequest : PublishRequest  )
  case class Completed( s : PublisherStatus)
}

class PointPublisher( catalogueService: CatalogueService) extends Actor {

  val publisherProcesses = List.range[Int](0,4,1).map( x => context.actorOf(Props(new Publisher(catalogueService, x)),s"PublisherProcessor_$x"))

  var availableProcessors = publisherProcesses

  var queuedRequests = List[PublishGridCellPoints]()
  var processingRequests = List[PublishGridCellPoints]()
  var completedRequests = List[PublisherStatus]()
  override def receive ={
      case q : PublishGridCellPoints => {
        if( availableProcessors.isEmpty )
        {
          queuedRequests = q :: queuedRequests
        }
        else
        {
          val processor = availableProcessors.head
          availableProcessors = availableProcessors.tail
          processingRequests = q :: processingRequests
          processor ! q
        }
      }
      case c : PublisherMessages.IsCompleted => {
        println("Received is completed request")
        if( !queuedRequests.filter(p => c.publishRequest.gcps.fileName == p.publishRequest.gcps.fileName).isEmpty )
        {
          println(s"${c.publishRequest.gcps.fileName} is queued.")
          sender ! PublisherStatus(false, c.publishRequest.gcps.fileName, s"Queued")
        }
        else if( !processingRequests.filter(p => c.publishRequest.gcps.fileName == p.publishRequest.gcps.fileName).isEmpty )
        {
          println(s"${c.publishRequest.gcps.fileName} is processing.")
          sender ! PublisherStatus(false, c.publishRequest.gcps.fileName , "Processing.")
        }
        else if( !completedRequests.filter(p =>c.publishRequest.gcps.fileName == p.fileName).isEmpty )
        {
          println(s"${c.publishRequest.gcps.fileName} is completed")
          val publisherStatus = completedRequests.filter(p => c.publishRequest.gcps.fileName == p.fileName).head
          sender ! publisherStatus
        }

      }
      case c : PublisherMessages.Completed => {
        println(s"Completed ${c.s.fileName}")
        completedRequests = c.s :: completedRequests
        processingRequests = processingRequests.filterNot(r => r.publishRequest.gcps.fileName == c.s.fileName )

        if( !queuedRequests.isEmpty )
        {
          val nextRequest = queuedRequests.head
          queuedRequests = queuedRequests.tail
          sender ! nextRequest
        }
        else
        {
          availableProcessors = sender :: availableProcessors
        }
      }
      case _ => { println("ERROR ERROR ERROR") }
    }
}

class Publisher( catalogueService: CatalogueService, val processNr : Int) extends Actor {


  override def receive ={
    case p : PublishGridCellPoints =>{
      val gcp = p.publishRequest.gcps
      println(s"Received publishGridCellPoints request X=[${gcp.minX}] Y=[${gcp.minY}] Size=[${gcp.size}, FileName=${gcp.fileName}]")

      val outputBasePath = p.environment.pointCdfPath
      println(s"PointCdf output base path for ${p.environment.name} is $outputBasePath ")

      val reader = new NetCdfReader(gcp.fileName, Set[String]())

      val variablesAndData = reader.getVariables().map(v => (v, v.read()))

      val catalogueElement = createCatalogueElement( variablesAndData, outputBasePath, gcp, p.publishRequest.parentDsName, p.publishRequest.dsName, p.publishRequest.region )

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

      val writer = new NetCdfWriter(catalogueElement.shardName, reader.getVariables().map( x => WriterColumn.Column(x.getShortName, 0, x.getDataType)), schema)

      writer.write( variablesAndData )

      reader.close()
      writer.close()

      Await.result(catalogueService.publishCatalogueElement(p.publishRequest.envName, p.publishRequest.parentDsName,p.publishRequest.dsName).invoke(catalogueElement), 10 seconds )

      sender ! PublisherMessages.Completed(PublisherStatus(true, p.publishRequest.gcps.fileName, s"Successfully published: [${catalogueElement.shardName}] " ))
    }
  }

  private def createCatalogueElement( variablesAndData : List[(Variable, ucar.ma2.Array)], basePath : String, gcp : GridCellPoints, parentDsName : String, dsName : String, region : String ): CatalogueElement={

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

    def max( lhs: Double, rhs : Double ):Double={
      if(lhs.isNaN){ rhs }else{ Math.max(lhs,rhs)}
    }

    def maxL( lhs : Long, rhs : Long):Long={

      if(lhs == Long.MaxValue){ return rhs  }else{Math.max(lhs,rhs)}

    }
    def min( lhs: Double, rhs : Double ):Double={
      if(lhs.isNaN){ rhs }else{ Math.min(lhs,rhs)}
    }

    println("Starting to calc maxes and mins")

    for( i <- 0 until count)
    {
      xMin = min(xMin, x.getDouble(i))
      xMax = max(xMax, x.getDouble(i))
      yMin = min(yMin, y.getDouble(i))
      yMax = max(yMax, y.getDouble(i))
      latMin = min(latMin, lat.getDouble(i))
      latMax = max(latMax, lat.getDouble(i))
      lonMin = min(lonMin, lon.getDouble(i))
      lonMax = max(lonMax, lon.getDouble(i))
      tMin = min(tMin, t.getDouble(i))
      tMax = max(tMax, t.getDouble(i))
    }

    println("Completed calculating maxes and mins")

    val date = LocalDateTime.ofEpochSecond(tMin.toLong, 0, ZoneOffset.UTC)

    val now = LocalDateTime.now(ZoneOffset.UTC)
    // LocalDateTime to epoch seconds
    val seconds = now.atZone(ZoneOffset.UTC).toEpochSecond()

    val shardPath = s"${basePath}/${dsName}/gridcell/y${date.getYear}/m${date.getMonthValue}/cell_${gcp.projection}_${gcp.minX}_${gcp.minY}/"
    val shardName = s"${shardPath}GridCell_${processNr}_$seconds.nc"

    def createDir( path : String) : Unit=
    {
      val dir = new File(path)
      if(!dir.exists()){
        dir.mkdirs()
      }
    }

    createDir(shardPath)

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

}
