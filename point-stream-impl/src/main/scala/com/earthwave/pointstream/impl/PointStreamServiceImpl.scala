package com.earthwave.pointstream.impl

import java.io.File

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.earthwave.catalogue.api.CatalogueService
import com.earthwave.environment.api.EnvironmentService
import com.earthwave.point.api.Messages.Query
import com.earthwave.pointstream.api.{PointStreamService, StreamQuery}
import com.lightbend.lagom.scaladsl.api.ServiceCall
import com.earthwave.point.impl.{NetCdfReader, NetCdfWriter, WriterColumn}
import ucar.ma2.DataType

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
  * Implementation of the PointStreamService.
  */
class PointStreamServiceImpl(catalogue : CatalogueService, env : EnvironmentService, system : ActorSystem) extends PointStreamService {

  private def doQuery( q : StreamQuery): Future[String] =
  {

    println( s"ParentDS=[${q.parentDSName}], DataSet=[${q.dsName}] MinTime=[${q.bbf.minT}] MaxTime=[${q.bbf.maxT}]" )

    val outputPath = Await.result(env.getEnvironment(q.envName).invoke(), 10 seconds ).cacheCdfPath

    val projection = q.projections.foldLeft[String]("")( (x,y) => x + "_" + y )
    val filters = q.filters.foldLeft[String]("")( (x,y) => x + "_" + y.column + y.op + y.threshold )
    val fileNameHash = s"${q.bbf.minX}_${q.bbf.maxX}_${q.bbf.minY}_${q.bbf.maxY}_${q.bbf.minT}_${q.bbf.maxT}${projection}${filters}".hashCode
    var fileName = s"${outputPath}${q.parentDSName}_${q.dsName}_${fileNameHash}.nc"
    val cacheCheck = new File(fileName)

    if( !cacheCheck.exists() ) {
      val future = catalogue.shards(q.parentDSName, q.dsName).invoke(q.bbf)

      val shards = Await.result(future, 10 seconds)

      val cols = if(q.projections.isEmpty){ scala.collection.mutable.Set() }else {
        val c = scala.collection.mutable.Set("x", "y", "time")
        q.projections.foreach(p => c.+=(p))
        c
      }

      if (!shards.isEmpty) {
        val tempReader = new NetCdfReader(shards.head.shardName, cols.toSet)
        val columns = tempReader.getVariables().map(x => WriterColumn.Column(x.getShortName, 0, x.getDataType))
        tempReader.close()

        val writer = new NetCdfWriter(fileName, columns, Map[String,DataType]())
        try {
          shards.foreach(x => {
            val reader = new NetCdfReader(x.shardName, cols.toSet)
            try {
              val data = reader.getVariablesAndData(Query( q.bbf, q.projections, q.filters ))
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


  def doQuery() = ServiceCall[StreamQuery, Source[String,NotUsed]] { query =>
    Future.successful( Source.apply(List(query)).mapAsync(1)(q => doQuery(q)))
  }
}
