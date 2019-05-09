package com.earthwave.point.impl

import java.io.File

import akka.actor.{ActorSystem, Props}
import com.earthwave.point.api._
import com.earthwave.catalogue.api._
import com.earthwave.point.impl.GeoJsonActor.{GeoJson}
import com.lightbend.lagom.scaladsl.api.ServiceCall

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import com.earthwave.core.{Column, NetCdfReader, NetCdfWriter}
import com.earthwave.environment.api.EnvironmentService
import com.earthwave.point.api.Messages.{Columns, FeatureCollection, Query}

/**
  * Implementation of the PointService.
  */
class PointServiceImpl( catalogue : CatalogueService, env : EnvironmentService, system : ActorSystem) extends PointService {

  implicit val ec = ExecutionContext.global
  implicit val timeOut = Timeout(30 seconds)

  override def getGeoJson(  parentDSName : String, dsName : String ) : ServiceCall[BoundingBoxFilter,FeatureCollection] ={ bbf =>
      val future = catalogue.shards(parentDSName, dsName).invoke(bbf)

      val result = Await.result( future, 10 seconds  )
      val numberOfPoints = result.shards.map(x => x.numberOfPoints).sum

      println(s"Number of points: $numberOfPoints")
      val geojsonactor = system.actorOf(Props(new GeoJsonActor( )))
      val featureCollection = (geojsonactor ? GeoJson(result, bbf)).mapTo[FeatureCollection]
      println(s"Actor processing complete")
      featureCollection
  }

  override def getDataSetColumns(parentDsName: String, dsName: String): ServiceCall[BoundingBoxFilter, Columns] = { bbf =>

    val future = catalogue.shards(parentDsName, dsName).invoke(bbf)

    val shards =  Await.result(future, 10 seconds)

    val reader = new NetCdfReader( shards.shards.head.shardName, Set[String]() )

    val columns = reader.getVariables().map(c => Messages.Column( c.getShortName() ))

    Future.successful(Columns(columns))
  }

  override def getNetCdfFile( parentDsName: String, dsName: String): ServiceCall[BoundingBoxFilter, String] = { bbf =>

    val outputPath = Await.result(env.getEnvironment().invoke(), 10 seconds ).outputCdfPath

    val fileName = s"${outputPath}${parentDsName}_${dsName}_${bbf.minX}_${bbf.maxX}_${bbf.minY}_${bbf.maxY}_${bbf.minT.getTime}_${bbf.maxT.getTime}.nc"
    val cacheCheck = new File(fileName)

    if( !cacheCheck.exists() ) {
      val future = catalogue.shards(parentDsName, dsName).invoke(bbf)

      val shards = Await.result(future, 10 seconds)
      val numberOfPoints = shards.shards.map(x => x.numberOfPoints).sum
      println(s"Number of points: $numberOfPoints")

      val shardReaders = shards.shards.map(s => (s, new NetCdfReader(s.shardName,Set[String]())))

      val columns = shardReaders.head._2.getVariables().map(x => Column(x.getShortName, 0, x.getDataType))

      val writer = new NetCdfWriter(fileName, columns)

      shardReaders.foreach(x => { val data = x._2.getVariablesAndData(Query(bbf, List[String](), List[Messages.Filter]()))
                                  if(data._2.length != 0){writer.writeWithFilter(data._1, data._2 )}})

      writer.close()
      shardReaders.foreach(s => s._2.close())
    }
    Future.successful(fileName)
  }

  override def executeQuery( parentDSName : String, dsName : String ) : ServiceCall[Query, String] = { q =>

    val outputPath = Await.result(env.getEnvironment().invoke(), 10 seconds ).outputCdfPath

    val projection = q.projection.foldLeft[String]("")( (x,y) => x + "_" + y )
    val filters = q.filters.foldLeft[String]("")( (x,y) => x + "_" + y.column + y.op + y.threshold )
    val fileNameHash = s"${q.bbf.minX}_${q.bbf.maxX}_${q.bbf.minY}_${q.bbf.maxY}_${q.bbf.minT.getTime}_${q.bbf.maxT.getTime}${projection}${filters}".hashCode
    val fileName = s"${outputPath}${parentDSName}_${dsName}_${fileNameHash}.nc"
    val cacheCheck = new File(fileName)

    if( !cacheCheck.exists() ) {
      val future = catalogue.shards(parentDSName, dsName).invoke(q.bbf)

      val shards = Await.result(future, 10 seconds)

      var cols = scala.collection.mutable.Set("x", "y", "time")
      q.projection.foreach(p => cols.+=(p))

      val shardReaders = shards.shards.map(s => (s, new NetCdfReader(s.shardName, cols.toSet)))

      val columns = shardReaders.head._2.getVariables().map(x => Column(x.getShortName, 0, x.getDataType))

      val writer = new NetCdfWriter(fileName, columns)

      shardReaders.foreach(x => { val data = x._2.getVariablesAndData(q)
                                  if(data._2.length != 0){writer.writeWithFilter(data._1, data._2)}})

      writer.close()
      shardReaders.foreach(s => s._2.close())
    }
    Future.successful(fileName)
  }
}
