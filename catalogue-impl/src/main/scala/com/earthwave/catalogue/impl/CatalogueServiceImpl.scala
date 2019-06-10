package com.earthwave.catalogue.impl

import akka.NotUsed
import com.earthwave.catalogue.api._
import com.lightbend.lagom.scaladsl.api.ServiceCall
import org.bson.BsonArray
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.{Completed, Document, MongoClient, Observer}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Accumulators._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * Implementation of the CatalogueService.
  */
class CatalogueServiceImpl() extends CatalogueService {

  private val client = MongoClient()
  implicit val ec = ExecutionContext.global
  private val ignoreDataSets = Set("admin","config","local")

  override def parentDataSets(): ServiceCall[NotUsed, List[DataSet]] = { _ =>

    val obs = client.listDatabaseNames()

    val fut = obs.toFuture()

    val res = Await.result(fut, 10 seconds)

    Future.successful( res.map(d=> DataSet(d)).filterNot( p => ignoreDataSets.contains(p.name) ).toList )
  }

  override def dataSets( parentName : String ): ServiceCall[NotUsed,List[DataSet]] = {_ =>

    val mongoDb = client.getDatabase(parentName)

    val catalogue = mongoDb.getCollection("catalogue")

    val fut = catalogue.aggregate(List(group("$dsName", sum( "count", 1 ))) ).toFuture()

    val result = Await.result(fut, 10 seconds)

    val dataSets = result.map(d => DataSet(d.getString("_id") ))
    Future.successful((dataSets.toList))
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

  override def boundingBoxQuery(parentDsName: String, dsName: String): ServiceCall[BoundingBoxFilter, List[BoundingBox]] = { bbf =>
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
      Future.successful(docs)
    }
  }

  override def shards(parentDsName: String, dsName: String): ServiceCall[BoundingBoxFilter, List[Shard]] = { bbf =>
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

    val docs = results.toList.map( doc => Shard(doc.getString("shardName"),
                                                doc.getDouble( "minX" ),
                                                doc.getDouble("maxX"),
                                                doc.getDouble("minY"),
                                                doc.getDouble("maxY"),
                                                doc.getDate("minTime"),
                                                doc.getDate("maxTime"),
                                                doc.getLong("count")
                                                    ) )
    Future.successful(docs)
  }

  override def getSwathDetailsFromName( parentDsName : String, dsName : String, name : String ) : ServiceCall[NotUsed,SwathDetail] = { _ =>
    val f : Bson = and(equal("datasetName",dsName), equal("swathName", name))

    val results = getSwathDetailsWithFilter( parentDsName, f )

    if( results.isEmpty )
      throw new Exception(s"No results returned for name=[$name].")
    if( results.length > 1)
      throw new Exception(s"Duplicate swath details found for name=[$name].")

    Future.successful(results.head)
  }

  override def getSwathDetailsFromId( parentDsName : String, dsName : String, id : Long ) : ServiceCall[NotUsed,SwathDetail] = { _ =>
    val f : Bson = and(equal("datasetName",dsName), equal("swathId",id))

    val results = getSwathDetailsWithFilter( parentDsName, f )

    if( results.isEmpty )
      throw new Exception(s"No results returned for id=[$id].")
    if( results.length > 1)
      throw new Exception(s"Duplicate swath details found for id=[$id].")

    Future.successful(results.head)
  }

  override def getSwathDetails( parentDsName : String, dsName : String ) : ServiceCall[NotUsed,List[SwathDetail]] ={ _ =>

    val f : Bson = equal("datasetName",dsName)

    Future.successful(getSwathDetailsWithFilter( parentDsName, f))
  }

  private def getSwathDetailsWithFilter( parentDsName : String, filter : Bson  ) : List[SwathDetail] = {

    val mongoDb = client.getDatabase(parentDsName)
    val collection = mongoDb.getCollection("swathDetails")

    val obs = collection.find(filter).toFuture()

    val results : Seq[Document] = Await.result(obs, 10 seconds)

    val docs = results.toList

    def convertResults( doc : Document ) : SwathDetail = {
      val gcsDocs: BsonArray = doc.get("gridCells").get.asArray()

      val buffer = new ListBuffer[GridCell]()

      for (i <- 0 until gcsDocs.size()) {
        val doc = gcsDocs.get(i).asDocument()
        buffer.append(GridCell(doc.getString("projection").getValue, doc.getInt64("x").longValue(), doc.getInt64("y").longValue(), doc.getInt32("pointCount").longValue()))
      }

      SwathDetail(doc.getString("swathName"), doc.getInteger("pointCount").toLong, buffer.toList)
    }
    docs.map(d => convertResults(d))
  }

  override def publishCatalogueElement(parentDsName: String, dsName: String): ServiceCall[CatalogueElement, String] = { ce =>

    val db = client.getDatabase(parentDsName)
    val coll = db.getCollection(dsName)

    val doc = ShardDetailImpl.toDocument(ce)

    val obs = coll.insertOne(doc)

    obs.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"onNext: $result")

      override def onError(e: Throwable): Unit = throw new Exception("Error writing results to Mongo")

      override def onComplete(): Unit = {
        println(s"Published catalogue entry successfully to Mongo")
      }})

    Future.successful(s"Published catalogue element for GridCell X=[${ce.gridCellMinX}] Y=[${ce.gridCellMinY}]")
  }
}

