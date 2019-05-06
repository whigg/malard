package com.earthwave.catalogue.impl

import java.util.Date

import akka.NotUsed
import com.earthwave.catalogue.api._
import com.lightbend.lagom.scaladsl.api.ServiceCall
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.{Document, MongoClient}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Accumulators._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * Implementation of the CatalogueService.
  */
class CatalogueServiceImpl() extends CatalogueService {

  private val client = MongoClient()
  implicit val ec = ExecutionContext.global
  private val ignoreDataSets = Set("admin","config","local")

  override def parentDataSets(): ServiceCall[NotUsed, DataSets] = { _ =>

    val obs = client.listDatabaseNames()

    val fut = obs.toFuture()

    val res = Await.result(fut, 10 seconds)

    Future.successful( DataSets(res.map(d=> DataSet(d)).filterNot( p => ignoreDataSets.contains(p.name) )) )
  }

  override def dataSets( parentName : String ): ServiceCall[NotUsed,DataSets] = {_ =>

    val mongoDb = client.getDatabase(parentName)

    val catalogue = mongoDb.getCollection("catalogue")

    val fut = catalogue.aggregate(List(group("$dsName", sum( "count", 1 ))) ).toFuture()

    val result = Await.result(fut, 10 seconds)

    val dataSets = result.map(d => DataSet(d.getString("_id") ))
    Future.successful(DataSets(dataSets))
  }


  override def filterCatalogue(): ServiceCall[CatalogueFilter, Catalogue] = { x =>

    val mongoDb = client.getDatabase(x.dbName)

    println(s"Connected to MongoDB with name ${x.dbName}.")
    println(s"Request has year: ${x.year.get}")
    val collection = mongoDb.getCollection("catalogue")

    val condition = and( equal( "year", x.year.get  ), equal("month", x.month.get))

    val obs = collection.find(condition)

    val fut = obs.toFuture()

    val results = Await.result(fut, 10 seconds)

    println(s"Number of rows retrieved: ${results.length}")

    val catalogue = results.map(d => ShardDetailImpl.fromDocument(d) )

    Future.successful( Catalogue(catalogue.toList) )
  }

  override def boundingBox( parentName : String, dsName : String  ) : ServiceCall[NotUsed, BoundingBox] = { _ => {

      println(s"Wiring check. Parent=$parentName, DataSet=$dsName")
      val mongoDb = client.getDatabase(parentName)
      val collection = mongoDb.getCollection("catalogue")

      val f = filter(equal("dsName", dsName ))
      val g = group( null, min("gridCellMinX",  "$gridCellMinX")
                    , max("gridCellMaxX", "$gridCellMaxX")
                    , min("gridCellMinY",  "$gridCellMinY")
                    , max("gridCellMaxY", "$gridCellMaxY")
                    , min("minTime", "$minTime" )
                    , max("maxTime","$maxTime")
                    , sum("numberOfPoints", "$count")
                    , sum("numberOfShards", 1))

      val obs = collection.aggregate(List(f,g))

      val fut = obs.toFuture()

      val results = Await.result(fut, 10 seconds)

      val doc = results.toList.head
      Future.successful(BoundingBox(doc.getLong("gridCellMinX")
                                    , doc.getLong("gridCellMaxX")
                                    , doc.getLong("gridCellMinY")
                                    , doc.getLong("gridCellMaxY")
                                    , doc.getDate("minTime")
                                    , doc.getDate("maxTime")
                                    , doc.getLong("numberOfPoints")
                                    , doc.getInteger("numberOfShards").toLong))
    }
  }

  override def boundingBoxQuery(parentDsName: String, dsName: String): ServiceCall[BoundingBoxFilter, BoundingBoxes] = { bbf =>
    {
      println(s"Wiring check. Parent=$parentDsName, DataSet=$dsName")
      val mongoDb = client.getDatabase(parentDsName)
      val collection = mongoDb.getCollection("catalogue")

      val groupByCols = Document( "gridCellMaxX" -> "$gridCellMaxX"
                                , "gridCellMinX" -> "$gridCellMinX"
                                , "gridCellMaxY" -> "$gridCellMaxY"
                                , "gridCellMinY" -> "$gridCellMinY")

      val f: Bson = filter( and(equal("dsName",dsName)
                  ,and(gte( "gridCellMaxX", bbf.minX )
                  ,and(lte( "gridCellMinX", bbf.maxX)
                  ,and(gte( "gridCellMaxY", bbf.minY)
                  ,and(lte("gridCellMinY", bbf.maxY)
                  ,and(gte( "maxTime", bbf.minT )
                  , lte( "minTime", bbf.maxT ))))))))

     val g = group( groupByCols
                    , min("minTime", "$minTime")
                    , max("maxTime", "$maxTime")
                    , sum("numberOfPoints", "$count")
                    , sum("numberOfShards", 1))


      val obs = collection.aggregate(List(f,g))

      val fut = obs.toFuture()

      val results: Seq[Document] = Await.result(fut, 10 seconds)

      val docs = results.toList.map( doc =>{ val id = doc.get("_id").get.asDocument()
                                                        BoundingBox( id.getInt64("gridCellMinX").longValue()
                                                        , id.getInt64("gridCellMaxX").longValue()
                                                        , id.getInt64("gridCellMinY").longValue()
                                                        , id.getInt64("gridCellMaxY").longValue()
                                                        , doc.getDate("minTime")
                                                        , doc.getDate("maxTime")
                                                        , doc.getLong("numberOfPoints")
                                                        , doc.getInteger("numberOfShards").toLong )})
      Future.successful(BoundingBoxes(docs))
    }
  }

  override def shards(parentDsName: String, dsName: String): ServiceCall[BoundingBoxFilter, Shards] = { bbf =>
    println(s"Wiring check. Parent=$parentDsName, DataSet=$dsName")
    val mongoDb = client.getDatabase(parentDsName)
    val collection = mongoDb.getCollection("catalogue")

    val f: Bson =  and(equal("dsName",dsName)
      ,and(gt( "gridCellMaxX", bbf.minX )
        ,and(lt( "gridCellMinX", bbf.maxX)
          ,and(gt( "gridCellMaxY", bbf.minY)
            ,and(lt("gridCellMinY", bbf.maxY)
              ,and(gte( "maxTime", bbf.minT )
                , lte( "minTime", bbf.maxT )))))))

    val obs = collection.find(f)

    val fut = obs.toFuture()

    val results: Seq[Document] = Await.result(fut, 10 seconds)

    println(s"Results count ${results.length} ")

    println(s"First one: ${results.head.toJson()} ")

    val docs = results.toList.map( doc => Shard(doc.getString("shardName"),
                                                doc.getDouble( "minX" ),
                                                doc.getDouble("maxX"),
                                                doc.getDouble("minY"),
                                                doc.getDouble("maxY"),
                                                doc.getDate("minTime"),
                                                doc.getDate("maxTime"),
                                                doc.getLong("count")
                                                    ) )
    Future.successful(Shards(docs))
  }
}

object ShardDetailImpl
{
  def fromDocument( doc : Document ): CatalogueElement= {

    val d = doc.filterNot(p => (p._1 == "_id") || (p._1 == "insertTime"))

    val dsName: String = doc.getString("dsName")
    val shardName: String = doc.getString("shardName")
    val projection: String = doc.getString("projection")
    val year: Int = doc.getInteger("year")
    val month: Int = doc.getInteger("month")
    val gridCellMinX: Long = doc.getLong("gridCellMinX")
    val gridCellMaxX: Long = doc.getLong("gridCellMaxX")
    val gridCellMinY: Long = doc.getLong("gridCellMinY")
    val gridCellMaxY: Long = doc.getLong("gridCellMaxY")
    val gridCellSize: Long = doc.getLong("gridCellSize")
    val minX: Double = doc.getDouble("minX")
    val maxX: Double = doc.getDouble("maxX")
    val minY: Double = doc.getDouble("minY")
    val maxY: Double = doc.getDouble("maxX")
    val minLat: Double = doc.getDouble("minLat")
    val maxLat: Double = doc.getDouble("maxLat")
    val minLon: Double = doc.getDouble("minLon")
    val maxLon: Double = doc.getDouble("maxLon")
    val minTime: Date = doc.getDate("minTime")
    val maxTime: Date = doc.getDate("maxTime")
    val count: Long = doc.getLong("count")
    val qualityCount: Long = doc.getLong("qualityCount")

    CatalogueElement(dsName
      , shardName
      , projection
      , year
      , month
      , gridCellMinX
      , gridCellMaxX
      , gridCellMinY
      , gridCellMaxY
      , gridCellSize
      , minX
      , maxX
      , minY
      , maxY
      , minLat
      , maxLat
      , minLon
      , maxLon
      , minTime
      , maxTime
      , count
      , qualityCount)
  }
}
