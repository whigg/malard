package com.earthwave.projection.impl

import akka.NotUsed
import com.earthwave.environment.api.{Environment, EnvironmentService}
import com.earthwave.projection.api.{Projection, ProjectionMapping, ProjectionService}
import com.lightbend.lagom.scaladsl.api.ServiceCall
import org.bson.BsonArray
import org.mongodb.scala.{Completed, Document, MongoClient, Observer}
import org.mongodb.scala.model.Filters._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.JavaConverters._
/**
  * Implementation of the CatalogueService.
  */
class ProjectionServiceImpl(env : EnvironmentService) extends ProjectionService {

  implicit val ec = ExecutionContext.global

  var envCache = scala.collection.mutable.Map[String,(Environment,MongoClient)]()

  private def getMongoClient( envName : String ) : MongoClient =
  {
    val client = envCache.getOrElse( envName, { val environment =  Await.result(env.getEnvironment(envName).invoke(), 10 seconds )
      val client = MongoClient(environment.mongoConnection)
      println(s"Projection Service MongoClient ${environment.mongoConnection} for ${envName}.")
      envCache = envCache.+=((envName, (environment, client)))
      ( environment, client)})

    client._2
  }

  override def getProjectionFromShortName(envName: String, shortName: String): ServiceCall[NotUsed, Projection] = { _ =>
    val client = getMongoClient(envName)
    val db = client.getDatabase("Configuration")
    val collection = db.getCollection("Projections")

    val filter = equal("shortName", shortName)

    val results = Await.result(collection.find(filter).toFuture, 10 seconds).toList

    if (results.length > 1)
      throw new Exception(s"Duplicates found for shortName:[$shortName]")

    val proj4 = results.head.getString("proj4")

    val conditionsArray: BsonArray = results.head.get("conditions").get.asArray()

    val conditions = conditionsArray.getValues.asScala.toList.map(v => v.asString().getValue)

    Future.successful(Projection(shortName, proj4, conditions))
  }

  override def getProjection(envName: String, parentDsName: String, region: String): ServiceCall[NotUsed, Projection] = {
    _ => {

      val client = getMongoClient(envName)
      val db = client.getDatabase(parentDsName)
      val collection = db.getCollection("ProjectionMappings")

      val filter = equal("region", region)

      val results = Await.result(collection.find(filter).toFuture, 10 seconds).toList

      if (results.length > 1)
        throw new Exception(s"Duplicates found for region:[$region]")

      val shortName = results.head.getString("shortName")

      val result: Projection = Await.result(getProjectionFromShortName(envName, shortName).invoke(), 10 seconds)

      Future.successful(result)
    }
  }


  override def publishProjection(envName: String): ServiceCall[Projection, String] = { proj =>
    val client = getMongoClient(envName)
    val db = client.getDatabase("Configuration")
    val collection = db.getCollection("Projections")

    val obs = collection.insertOne(Document("shortName" -> proj.shortName,
      "proj4" -> proj.proj4, "conditions" -> proj.conditions ))

    obs.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"onNext: $result")

      override def onError(e: Throwable): Unit = throw new Exception("Error writing results to Mongo")

      override def onComplete(): Unit = {
        println(s"Completed writing ShortName=[${proj.shortName}]")
      }
    })

    Future.successful(s"Published: ${proj.shortName}")
  }

  override def publishRegionMapping(envName: String): ServiceCall[ProjectionMapping, String] = { pm =>
    println(s"Received publish mapping request.")
    val client = getMongoClient(envName)
    val db = client.getDatabase(pm.parentDataSetName)
    val collection = db.getCollection("ProjectionMappings")

    val obs = collection.insertOne(Document("region" -> pm.region, "shortName" -> pm.shortName))

    obs.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"onNext: $result")

      override def onError(e: Throwable): Unit = throw new Exception("Error writing results to Mongo")

      override def onComplete(): Unit = {
        println(s"Completed writing Region=[${pm.region}] to ShortName=[${pm.shortName}]")
      }
    })

    Future.successful(s"Published: Region=[${pm.region}] ShortName=[${pm.shortName}]")
  }
}