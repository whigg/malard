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

  var mongoConnection = "mongodb://localhost:27018"

  var envCache = scala.collection.mutable.Map[String, Environment]()

  override def createEnvironment( name : String ): ServiceCall[Environment, String] = { x =>

    val client = MongoClient(mongoConnection)
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
                      , "pointCdfPath" -> x.pointCdfPath
                      , "mongoConnection" -> x.mongoConnection
                      , "swathIntermediatePath" -> x.swathIntermediatePath
                      , "holdingBaseDir" -> x.holdingBaseDir
                      , "dataBaseDir" -> x.dataBaseDir
                      , "deflateLevel" -> x.deflateLevel
                      , "serverVersion" -> x.serverVersion
                      )

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

      val connectionString = mongoConnection
      val client = MongoClient(connectionString)

      val db = client.getDatabase("Configuration")
      val collection = db.getCollection("Environment")

      val f = equal( "name", name )

      val docOption = Await.result( collection.find(f).toFuture(), 10 seconds ).headOption

      val doc = docOption.getOrElse( throw new Exception(s"Environment name ${name} does not exist."))

      val res = Environment( doc.getString("name")
                          , doc.getString("publisherPath")
                          , doc.getString("outputCdfPath")
                          , doc.getString("pointCdfPath")
                          , doc.getString("mongoConnection")
                          , doc.getString("swathIntermediatePath")
                          , doc.getString("holdingBaseDir")
                          , doc.getString("dataBaseDir")
                          , doc.getInteger("deflateLevel")
                          , doc.getString("serverVersion") )

      def createDir( path : String): Unit =
      {
        val dir = new File(path)
        if(!dir.exists()) {
          dir.mkdirs()
        }
      }

      createDir(res.pointCdfPath)
      createDir(res.maskPublisherPath)
      createDir(res.cacheCdfPath)

      envCache = envCache.+=((name, res))
      res
    }

    val res = envCache.getOrElse(name, getEnvFromDb())

    Future.successful( res )
  }

  override def exists(name: String): ServiceCall[NotUsed, Boolean] = { _ =>

    val connectionString = mongoConnection
    val client = MongoClient(connectionString)

    val db = client.getDatabase("Configuration")
    val collection = db.getCollection("Environment")

    val f = equal( "name", name )

    val docOption = Await.result( collection.find(f).toFuture(), 10 seconds )

    val exists = if( docOption.isEmpty ){  false }else{  true }

    Future.successful( exists )

  }

  override def setConnectionString() : ServiceCall[String,Boolean] = { x =>

    mongoConnection = x

    Future.successful(true)

  }

}
