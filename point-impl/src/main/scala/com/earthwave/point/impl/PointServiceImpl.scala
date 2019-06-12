package com.earthwave.point.impl

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Date

import akka.actor.{ActorSystem, Props}
import com.earthwave.point.api._
import com.earthwave.catalogue.api._
import com.earthwave.point.impl.GeoJsonActor.GeoJson
import com.lightbend.lagom.scaladsl.api.ServiceCall

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import com.earthwave.environment.api.EnvironmentService
import com.earthwave.point.api.Messages._
import ucar.nc2.Variable

/**
  * Implementation of the PointService.
  */
class PointServiceImpl( catalogue : CatalogueService, env : EnvironmentService, system : ActorSystem) extends PointService {

  implicit val ec = ExecutionContext.global
  implicit val timeOut = Timeout(30 seconds)

  override def getGeoJson(  parentDSName : String, dsName : String ) : ServiceCall[BoundingBoxFilter,FeatureCollection] ={ bbf =>
      val future = catalogue.shards(parentDSName, dsName).invoke(bbf)

      val result = Await.result( future, 10 seconds  )
      val numberOfPoints = result.map(x => x.numberOfPoints).sum

      println(s"Number of points: $numberOfPoints")
      val geojsonactor = system.actorOf(Props(new GeoJsonActor( )))
      val featureCollection = (geojsonactor ? GeoJson(result, bbf)).mapTo[FeatureCollection]
      println(s"Actor processing complete")
      featureCollection
  }

  override def getDataSetColumns(parentDsName: String, dsName: String): ServiceCall[BoundingBoxFilter, Columns] = { bbf =>

    val future = catalogue.shards(parentDsName, dsName).invoke(bbf)

    val shards =  Await.result(future, 10 seconds)

    val reader = new NetCdfReader( shards.head.shardName, Set[String]() )

    val columns = reader.getVariables().map(c => Messages.Column( c.getShortName() ))

    Future.successful(Columns(columns))
  }

  override def getNetCdfFile( envName : String, parentDsName: String, dsName: String): ServiceCall[BoundingBoxFilter, String] = { bbf =>

    val outputPath = Await.result(env.getEnvironment(envName).invoke(), 10 seconds ).cacheCdfPath

    var fileName = s"${outputPath}${parentDsName}_${dsName}_${bbf.minX}_${bbf.maxX}_${bbf.minY}_${bbf.maxY}_${bbf.minT.getTime}_${bbf.maxT.getTime}.nc"
    println(s"Output filename: $fileName")
    val cacheCheck = new File(fileName)

    if( !cacheCheck.exists() ) {
      val future = catalogue.shards(parentDsName, dsName).invoke(bbf)

      val shards = Await.result(future, 10 seconds)
      if(!shards.isEmpty) {
        val numberOfPoints = shards.map(x => x.numberOfPoints).sum
        println(s"Number of points: $numberOfPoints")

        val tempReader = new NetCdfReader( shards.head.shardName, Set[String]() )
        val columns = tempReader.getVariables().map(x => WriterColumn.Column(x.getShortName, 0, x.getDataType))
        tempReader.close()

        val writer = new NetCdfWriter(fileName, columns)

        try {
          shards.foreach(x => {
            val reader = new NetCdfReader(x.shardName, Set[String]())
            try {
              val data = reader.getVariablesAndData(Query(bbf, List[String](), List[Messages.Filter]()))
              if (data._2.length != 0) {
                writer.writeWithFilter(data._1, data._2)
              }
            }
            finally {
              reader.close()
            }
          })
        }
        finally
        {
          writer.close()
        }
      }
      else
      {
        fileName = "Error: Empty resultset."
      }
    }
    Future.successful(fileName)
  }

  override def executeQuery(envName : String, parentDSName : String, dsName : String ) : ServiceCall[Query, String] = { q =>

    val outputPath = Await.result(env.getEnvironment(envName).invoke(), 10 seconds ).cacheCdfPath

    val projection = q.projection.foldLeft[String]("")( (x,y) => x + "_" + y )
    val filters = q.filters.foldLeft[String]("")( (x,y) => x + "_" + y.column + y.op + y.threshold )
    val fileNameHash = s"${q.bbf.minX}_${q.bbf.maxX}_${q.bbf.minY}_${q.bbf.maxY}_${q.bbf.minT.getTime}_${q.bbf.maxT.getTime}${projection}${filters}".hashCode
    var fileName = s"${outputPath}${parentDSName}_${dsName}_${fileNameHash}.nc"
    val cacheCheck = new File(fileName)

    if( !cacheCheck.exists() ) {
      val future = catalogue.shards(parentDSName, dsName).invoke(q.bbf)

      val shards = Await.result(future, 10 seconds)

      val cols = if(q.projection.isEmpty){ scala.collection.mutable.Set() }else {
        val c = scala.collection.mutable.Set("x", "y", "time")
        q.projection.foreach(p => c.+=(p))
        c
      }

      if (!shards.isEmpty) {
        val tempReader = new NetCdfReader(shards.head.shardName, cols.toSet)
        val columns = tempReader.getVariables().map(x => WriterColumn.Column(x.getShortName, 0, x.getDataType))
        tempReader.close()

        val writer = new NetCdfWriter(fileName, columns)

        try {
          shards.foreach(x => {
            val reader = new NetCdfReader(x.shardName, cols.toSet)
            try {
              val data = reader.getVariablesAndData(q)
              if (data._2.length != 0) {
                writer.writeWithFilter(data._1, data._2)
              }
            }
            finally {
              reader.close()
            }
          })
        }
        finally
        {
          writer.close()
        }
      }
      else {
        fileName = "Error: Empty resultset."
      }
    }
    Future.successful(fileName)
  }

  override def releaseCache() : ServiceCall[Cache, String] = { c => {
    val file = new java.io.File(c.handle)

    if (file.exists()) {
      println(s"Attempting to delete ${c.handle}")
      file.delete()
    }

    Future.successful(s"Released cache file ${c.handle} ")
    }
  }

  override def publishGridCellPoints(envName : String, parentDsName: String, dsName: String): ServiceCall[Messages.GridCellPoints, String] = { gcp =>

    println(s"Received publishGridCellPoints request X=[${gcp.minX}] Y=[${gcp.minY}] Size=[${gcp.size}, FileName=${gcp.fileName}]")

    val outputBasePath = Await.result(env.getEnvironment(envName).invoke(), 10 seconds ).pointCdfPath
    println(s"PointCdf output base path for $envName is $outputBasePath ")

    val reader = new NetCdfReader(gcp.fileName, Set[String]())

    val variablesAndData = reader.getVariables().map(v => (v, v.read()))

    val catalogueElement = createCatalogueElement( variablesAndData, outputBasePath, gcp, parentDsName, dsName )

    val writer = new NetCdfWriter(catalogueElement.shardName, reader.getVariables().map( x => WriterColumn.Column(x.getShortName, 0, x.getDataType)))

    writer.write( variablesAndData )

    reader.close()
    writer.close()

    Await.result(catalogue.publishCatalogueElement(parentDsName,dsName).invoke(catalogueElement), 10 seconds )

    Future.successful(s"Published grid cell points for input ${gcp.fileName} to ${catalogueElement.shardName}")
  }

  private def createCatalogueElement( variablesAndData : List[(Variable, ucar.ma2.Array)], basePath : String, gcp : GridCellPoints, parentDsName : String, dsName : String ): CatalogueElement={

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
    var tMin, tMax = Long.MaxValue

    val qualityCount = 0
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

    def minL( lhs : Long, rhs : Long):Long={

      if(lhs == Long.MaxValue){ return rhs  }else{Math.min(lhs,rhs)}

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
      tMin = minL(tMin, t.getLong(i))
      tMax = maxL(tMax, t.getLong(i))
    }

    println("Completed calculating maxes and mins")

    val date = LocalDateTime.ofEpochSecond(tMin, 0, ZoneOffset.UTC)

    val now = LocalDateTime.now(ZoneOffset.UTC)
    // LocalDateTime to epoch seconds
    val seconds = now.atZone(ZoneOffset.UTC).toEpochSecond()

    val shardPath = s"${basePath}/${dsName}/gridcell/y${date.getYear}/m${date.getMonthValue}/cell_${gcp.projection}_${gcp.minX}_${gcp.minY}/"
    val shardName = s"${shardPath}GridCell_$seconds.nc"

    def createDir( path : String) : Unit=
    {
      val dir = new File(path)
      if(!dir.exists()){
        dir.mkdirs()
      }
    }

    createDir(shardPath)

    CatalogueElement( dsName,
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
      new Date( 1000 * tMin),
      new Date(1000 * tMax),
      count,
      qualityCount
    )
  }
}
