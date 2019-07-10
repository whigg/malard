package com.earthwave.pointstream.impl

import java.io.File

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.earthwave.catalogue.api.CatalogueService
import com.earthwave.environment.api.EnvironmentService
import com.earthwave.point.api.Messages.Query
import com.earthwave.pointstream.api.PointStreamService
import com.lightbend.lagom.scaladsl.api.ServiceCall
import com.earthwave.point.impl.{NetCdfReader, NetCdfWriter, WriterColumn}
import ucar.ma2.DataType

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
  * Implementation of the PointStreamService.
  */
class PointStreamServiceImpl(catalogue : CatalogueService, env : EnvironmentService, system : ActorSystem) extends PointStreamService {

  private def doQuery( q : Query, envName : String, parentDSName : String, dsName : String ): Future[String] =
  {
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

        val writer = new NetCdfWriter(fileName, columns, Map[String,DataType]())
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


  def executeQuery( envName : String, parentDataSet : String, dataSetName : String ) = ServiceCall[Source[Query,NotUsed], Source[String, NotUsed]] { query =>
    Future.successful(query.mapAsync(1)( q => doQuery(q, envName, parentDataSet, dataSetName)))
  }
}
