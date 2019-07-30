package com.earthwave.catalogue.impl

import akka.NotUsed
import com.earthwave.catalogue.api._
import com.earthwave.environment.api.{Environment, EnvironmentService}
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
class CatalogueServiceImpl(env : EnvironmentService) extends CatalogueService {

  implicit val ec = ExecutionContext.global
  private val ignoreDataSets = Set("admin","config","local","Configuration")

  var envCache = scala.collection.mutable.Map[String,(Environment,MongoClient)]()

  private def getMongoClient( envName : String ) : MongoClient =
  {
    val client = envCache.getOrElse( envName, { val environment =  Await.result(env.getEnvironment(envName).invoke(), 10 seconds )
                                                val client = MongoClient(environment.mongoConnection)
                                                envCache = envCache.+=((envName, (environment, client)))
                                                ( environment, client)})

    client._2
  }


  override def parentDataSets(envName : String): ServiceCall[NotUsed, List[DataSet]] = { _ =>

    val client = getMongoClient(envName)
    val obs = client.listDatabaseNames()

    val fut = obs.toFuture()

    val res = Await.result(fut, 10 seconds)

    Future.successful( res.map(d=> DataSet(d)).filterNot( p => ignoreDataSets.contains(p.name) ).toList )
  }

  override def dataSets( envName : String,  parentName : String): ServiceCall[NotUsed,List[DataSetRegion]] = {_ =>

    val client = getMongoClient(envName)

    val mongoDb = client.getDatabase(parentName)

    val catalogue = mongoDb.getCollection("catalogue")

    val groupByCols = Document( "dsName" -> "$dsName"
                              , "region" -> "$region" )

    val groupBy = group( groupByCols, sum( "count", 1 ) )

    val fut = catalogue.aggregate( List(groupBy) ).toFuture()

    val result = Await.result(fut, 10 seconds)

    val dataSets = result.map(d => {
      val id = d.get("_id").get.asDocument()
      DataSetRegion(id.getString("dsName").getValue, id.getString("region").getValue)
    })
    Future.successful((dataSets.toList))
  }

  override def boundingBox( envName : String, parentName : String, dsName : String, region : String  ) : ServiceCall[NotUsed, BoundingBox] = { _ => {

    val client = getMongoClient(envName)
      println(s"Wiring check. Parent=$parentName, DataSet=$dsName")
      val mongoDb = client.getDatabase(parentName)
      val collection = mongoDb.getCollection("catalogue")

      val f = filter(and(equal("dsName", dsName ),equal("region", region)))
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
                                    , doc.getDouble("minTime")
                                    , doc.getDouble("maxTime")
                                    , doc.getLong("numberOfPoints")
                                    , doc.getInteger("numberOfShards").toLong))
    }
  }

  override def boundingBoxQuery(envName : String, parentDsName: String, dsName: String, region : String): ServiceCall[BoundingBoxFilter, List[BoundingBox]] = { bbf =>
    {
      val client = getMongoClient(envName)
      println(s"Wiring check. Parent=$parentDsName, DataSet=$dsName")
      val mongoDb = client.getDatabase(parentDsName)
      val collection = mongoDb.getCollection("catalogue")

      val groupByCols = Document( "gridCellMaxX" -> "$gridCellMaxX"
                                , "gridCellMinX" -> "$gridCellMinX"
                                , "gridCellMaxY" -> "$gridCellMaxY"
                                , "gridCellMinY" -> "$gridCellMinY")

      val f: Bson = filter( and(equal("dsName",dsName)
                  ,and(equal("region", region)
                  ,and(gt( "gridCellMaxX", bbf.minX )
                  ,and(lt( "gridCellMinX", bbf.maxX - 1e-9)
                  ,and(gt( "gridCellMaxY", bbf.minY)
                  ,and(lt("gridCellMinY", bbf.maxY - 1e-9)
                  ,and(gt( "maxTime", bbf.minT )
                  , lte( "minTime", bbf.maxT )))))))))

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
                                                        , doc.getDouble("minTime")
                                                        , doc.getDouble("maxTime")
                                                        , doc.getLong("numberOfPoints")
                                                        , doc.getInteger("numberOfShards").toLong )})
      Future.successful(docs)
    }
  }

  override def shards( envName : String, parentDsName: String, dsName: String, region : String): ServiceCall[BoundingBoxFilter, List[Shard]] = { bbf =>

    val client = getMongoClient(envName)

    val mongoDb = client.getDatabase(parentDsName)
    val collection = mongoDb.getCollection("catalogue")


    println(s"Shard request [${parentDsName}] [$dsName] [${bbf.minX}] [${bbf.minY}] [${bbf.minT}] [${bbf.maxT}] ")

    val f: Bson =  and(equal("dsName",dsName)
      ,and(equal("region", region)
      ,and(gt( "gridCellMaxX", bbf.minX )
        ,and(lt( "gridCellMinX", bbf.maxX - 1e-9)
          ,and(gt( "gridCellMaxY", bbf.minY)
            ,and(lt("gridCellMinY", bbf.maxY - 1e-9)
              ,and(gte( "maxTime", bbf.minT )
                , lte( "minTime", bbf.maxT ))))))))

    val obs = collection.find(f)

    val fut = obs.toFuture()

    val results: Seq[Document] = Await.result(fut, 10 seconds)

    println(s"Found [${results.length}] shards")

    val docs = results.toList.map( doc => Shard(doc.getString("shardName"),
                                                doc.getDouble( "minX" ),
                                                doc.getDouble("maxX"),
                                                doc.getDouble("minY"),
                                                doc.getDouble("maxY"),
                                                doc.getDouble("minTime"),
                                                doc.getDouble("maxTime"),
                                                doc.getLong("count")
                                                    ) )
    Future.successful(docs)
  }

  override def getSwathDetailsFromName( envName : String, parentDsName : String, dsName : String, region : String, name : String ) : ServiceCall[NotUsed,SwathDetail] = { _ =>
    val f : Bson = and(equal("region",region),and(equal("datasetName",dsName), equal("swathName", name)))

    val results = getSwathDetailsWithFilter( parentDsName, f, envName )

    if( results.isEmpty )
      throw new Exception(s"No results returned for name=[$name].")
    if( results.length > 1)
      throw new Exception(s"Duplicate swath details found for name=[$name].")

    Future.successful(results.head)
  }

  override def getSwathDetailsFromId( envName : String, parentDsName : String, dsName : String, region : String, id : Long ) : ServiceCall[NotUsed,SwathDetail] = { _ =>
    println(s"SwathDetailsFromId $envName, $parentDsName, $dsName, $region $id")
    val f : Bson = and(equal("region",region),and(equal("datasetName",dsName), equal("swathId",id)))

    val results = getSwathDetailsWithFilter( parentDsName, f, envName )

    if( results.isEmpty )
      throw new Exception(s"No results returned for Env=[$envName] PDS=[$parentDsName] ds=[$dsName] region=[$region]  id=[$id]")
    if( results.length > 1)
      throw new Exception(s"Duplicate swath details found for Env=[$envName] PDS=[$parentDsName] ds=[$dsName] region=[$region]  id=[$id].")

    Future.successful(results.head)
  }

  override def getSwathDetails(envName : String , parentDsName : String, dsName : String,region : String) : ServiceCall[NotUsed,List[SwathDetail]] ={ _ =>
    println(s"SwathDetails $envName, $parentDsName, $dsName, $region")
    val f : Bson = and(equal("datasetName",dsName),equal("region", region))

    Future.successful(getSwathDetailsWithFilter( parentDsName, f, envName))
  }

  private def getSwathDetailsWithFilter( parentDsName : String, filter : Bson, envName : String  ) : List[SwathDetail] = {
    val client = getMongoClient(envName)
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

      SwathDetail(doc.getString("swathName"), doc.getInteger("pointCount").toLong, doc.getInteger("swathId"), buffer.toList)
    }
    docs.map(d => convertResults(d))
  }

  override def publishCatalogueElement( envName : String, parentDsName: String, dsName: String) : ServiceCall[CatalogueElement, String] = { ce =>

    val client = getMongoClient(envName)
    val db = client.getDatabase(parentDsName)
    val coll = db.getCollection("catalogue")

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

