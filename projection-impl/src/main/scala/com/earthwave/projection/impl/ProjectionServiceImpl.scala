package com.earthwave.projection.impl

import akka.NotUsed
import com.earthwave.projection.api.{Projection, ProjectionService}
import com.lightbend.lagom.scaladsl.api.ServiceCall
import org.mongodb.scala.{Completed, Document, MongoClient, Observer}
import org.mongodb.scala.model.Filters._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * Implementation of the CatalogueService.
  */
class ProjectionServiceImpl() extends ProjectionService {

  private val client = MongoClient()
  implicit val ec = ExecutionContext.global

  override def getProjection(shortName: String): ServiceCall[NotUsed, Projection] = { _ =>

    val db = client.getDatabase("Configuration")
    val collection = db.getCollection("Projections")

    val filter = equal("shortName", shortName )

    val results = Await.result(collection.find(filter).toFuture, 10 seconds).toList

    if( results.length > 1 )
      throw new Exception(s"Duplicates found for shortName:[$shortName]")

    val proj4 = results.head.getString("proj4")

    Future.successful(Projection(shortName, proj4))
  }

  override def publishProjection(): ServiceCall[Projection, String] = { proj =>
    val db = client.getDatabase("Configuration")
    val collection = db.getCollection("Projections")

    val obs = collection.insertOne(Document( "shortName" -> proj.shortName,
                                             "proj4" -> proj.proj4 ))

    obs.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"onNext: $result")

      override def onError(e: Throwable): Unit = throw new Exception("Error writing results to Mongo")

      override def onComplete(): Unit = {
        println(s"Completed writing ShortName=[${proj.shortName}]")
      }})

    Future.successful(s"Published: ${proj.shortName}")
  }

}

