package com.earthwave.environment.impl


import java.io.File

import akka.NotUsed
import com.earthwave.environment.api.{Environment, EnvironmentService}
import com.lightbend.lagom.scaladsl.api.ServiceCall
import org.mongodb.scala.MongoClient
import org.bson.BsonArray
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.{Completed, Document, MongoClient, Observer}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Accumulators._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
  * Implementation of the CatalogueService.
  */
class EnvironmentServiceImpl() extends EnvironmentService {

  val client = MongoClient()

  override def createEnvironment( name : String ): ServiceCall[Environment, String] = { x =>

    val insertCheck = equal("name", name )
    val db = client.getDatabase("Configuration")
    val collection = db.getCollection("Environment")

    if( Await.result(collection.find(insertCheck).toFuture(), 10 seconds).toList.length > 0 )
    {
      throw new Exception(s"Environment already exists $name.")
    }

    val doc = Document( "name" -> name
                      , "publisherPath" -> x.maskPublisherPath
                      , "outputCdfPath" -> x.cacheCdfPath
                      , "pointCdfPath" -> x.pointCdfPath)

    val obs = collection.insertOne(doc)

    obs.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"onNext: $result")

      override def onError(e: Throwable): Unit = throw new Exception("Error writing results to Mongo")

      override def onComplete(): Unit = {
        println(s"Published environment [$name]entry successfully to Mongo")
      }})

    def createDir( path : String) : Unit=
    {
      val dir = new File(path)
      if(!dir.exists()){
        dir.mkdirs()
      }
    }

    createDir(x.cacheCdfPath)

    Future.successful( s"Environment Created [${x.name}]" )
  }

  override def getEnvironment( name : String): ServiceCall[NotUsed, Environment] = { _ =>

    def getEnvFromDb(): Environment = {
      val db = client.getDatabase("Configuration")
      val collection = db.getCollection("Environment")

      val f = equal( "name", name )

      val doc = Await.result( collection.find(f).toFuture(), 10 seconds ).head

      Environment( doc.getString("name"), doc.getString("publisherPath"), doc.getString("outputCdfPath"), doc.getString("pointCdfPath") )
    }

    val res = getEnvFromDb()

    Future.successful( res )
  }

}
