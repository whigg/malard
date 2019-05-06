package com.earthwave.point.impl

import akka.actor.{ActorSystem, Props}
import com.earthwave.point.api._
import com.earthwave.catalogue.api._
import com.earthwave.point.impl.GeoJsonActor.{GeoJson, GeoJsonFile}
import com.lightbend.lagom.scaladsl.api.ServiceCall

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import com.earthwave.core.{Column, NetCdfReader, NetCdfWriter}
import com.earthwave.environment.api.EnvironmentService

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

  override def getNetCdfFile( parentDsName: String, dsName: String): ServiceCall[BoundingBoxFilter, String] = { bbf =>

    val outputPath = Await.result(env.getEnvironment().invoke(), 10 seconds ).outputCdfPath

    val fileName = s"${outputPath}data.nc"

    val future = catalogue.shards(parentDsName, dsName).invoke(bbf)

    val shards = Await.result( future, 10 seconds  )
    val numberOfPoints = shards.shards.map(x => x.numberOfPoints).sum
    println(s"Number of points: $numberOfPoints")

    val shardReaders = shards.shards.map( s => (s, new NetCdfReader( s.shardName ) ) )

    val columns = shardReaders.head._2.getVariables().map( x => Column( x.getShortName, 0, x.getDataType ) )

    val writer = new NetCdfWriter(fileName, columns)

    shardReaders.foreach(x => writer.writeWithFilter( x._2.getVariablesAndData(), bbf)  )

    writer.close()

    Future.successful(fileName)
  }




}
