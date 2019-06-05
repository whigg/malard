package com.earthwave.gridcellstats.impl

import akka.NotUsed
import com.earthwave.gridcellstats.api.{GridCellStatistics, GridCellStatsService}
import com.earthwave.mask.api._
import com.lightbend.lagom.scaladsl.api.ServiceCall
import org.mongodb.scala.model.Accumulators.sum
import org.mongodb.scala.{Completed, Document, MongoClient, Observer}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * Implementation of the CatalogueService.
  */
class GridCellStatsServiceImpl() extends GridCellStatsService {

  private val client = MongoClient()
  implicit val ec = ExecutionContext.global

  override def publishGridCellStats( parentDataSet : String, runName : String ) : ServiceCall[GridCellStatistics, String] = { gcs =>

    val db = client.getDatabase(parentDataSet)
    val collection = db.getCollection("statistics" )

    val doc = Document( "runName" -> runName,
                        "gridCellMinX" -> gcs.gridCell.minX,
                        "gridCellMaxX" -> (gcs.gridCell.minX + gcs.gridCell.size),
                        "gridCellMinY" -> gcs.gridCell.minY,
                        "gridCellMaxY" -> (gcs.gridCell.minY + gcs.gridCell.size),
                        "size" -> gcs.gridCell.size,
                        "statistics" -> gcs.statistics.toList
                       )

    val obs = collection.insertOne(doc)

    obs.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"onNext: $result")

      override def onError(e: Throwable): Unit = throw new Exception("Error writing results to Mongo")

      override def onComplete(): Unit = {
       println(s"Completed writing Statistics for DataSet=[$parentDataSet],Run=[$runName],GridCell=[${gcs.gridCell.minX},${gcs.gridCell.minY},${gcs.gridCell.size}]")
      }})

    Future.successful(s"Published")
  }

  override def getAvailableStatistics(parentDataSet: String): ServiceCall[NotUsed, List[String]] = { _ =>

    val db = client.getDatabase(parentDataSet)
    val collection = db.getCollection("statistics" )

    val groupByCols = Document( "runName" -> "$runName" )

    val g = group( groupByCols, sum("runName", 1))

    val results = Await.result(collection.aggregate(List(g)).toFuture(), 10 seconds ).toList

    val runNames = results.map( d => { val id = d.get("_id").get.asDocument()
                                        id.getString("runName").getValue } )

    Future.successful(runNames)
  }

  override def getGridCellStatistics(parentDataSet: String, runName: String): ServiceCall[GridCell, Map[String, Double]] = { gc =>

    val db = client.getDatabase(parentDataSet)

    val collection = db.getCollection("statistics")

    val f = and(equal("runName", runName),
            and(equal("gridCellMinX", gc.minX),
            and(equal("gridCellMinY", gc.minY),
            and(equal("size", gc.size)))))

    val results = Await.result(collection.find( f ).toFuture(), 10 seconds)

    var statsMap = scala.collection.mutable.Map[String,Double]()

    val statsDoc = results.head.get("statistics").get.asDocument()

    statsDoc.forEach( (k,v) => statsMap.+=((k, v.asDouble().doubleValue())) )

    Future.successful(statsMap.toMap)

  }
}

