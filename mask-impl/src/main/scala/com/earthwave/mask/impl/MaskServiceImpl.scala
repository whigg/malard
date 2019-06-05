package com.earthwave.mask.impl

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import akka.NotUsed
import com.earthwave.environment.api.EnvironmentService
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
class MaskServiceImpl( env : EnvironmentService) extends MaskService {

  private val client = MongoClient()
  implicit val ec = ExecutionContext.global

  override def publishMask( parentDataSet : String, `type` : String, region : String ) : ServiceCall[MaskFile, String] = { x =>

    val environment = Await.result(env.getEnvironment().invoke(), 10 seconds )

    val db = client.getDatabase(parentDataSet)
    val collection = db.getCollection("masks" )

    val outputDir = s"${environment.publisherPath}/${parentDataSet}/static/${`type`}/$region/cell_${x.gridCell.minX}_${x.gridCell.minY}/"

    val dir = new File(outputDir)
    if(!dir.exists()) {
      dir.mkdirs()
    }

    val outputPath = s"$outputDir/${x.shapeFile}"

    val inputFile = java.nio.file.Paths.get(s"${x.sourceFilePath}/${x.shapeFile}")
    val outputFile = java.nio.file.Paths.get(outputPath)

    Files.copy(inputFile, outputFile,StandardCopyOption.REPLACE_EXISTING )

    val doc = Document( "type" -> `type`,
                        "region" -> region,
                        "shapeFile" -> outputPath,
                        "gridCellMinX" -> x.gridCell.minX,
                        "gridCellMaxX" -> (x.gridCell.minX + x.gridCell.size),
                        "gridCellMinY" -> x.gridCell.minY,
                        "gridCellMaxY" -> (x.gridCell.minY + x.gridCell.size),
                        "size" -> x.gridCell.size
    )

    val obs = collection.insertOne(doc)

    obs.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"onNext: $result")

      override def onError(e: Throwable): Unit = throw new Exception("Error writing results to Mongo")

      override def onComplete(): Unit = {
       println(s"Completed writing DataSet=[$parentDataSet], Type=[${`type`}], Region=[$region]")
      }})

    Future.successful(s"Published: ${outputFile}")
  }

  override def getMasks(parentDataSet : String): ServiceCall[NotUsed,List[Mask]] = { _ =>

    val db = client.getDatabase(parentDataSet)
    val collection = db.getCollection("masks")

    val groupByCols = Document( "type" -> "$type"
                              , "region" -> "$region" )

    val g = group( groupByCols, sum("type", 1), sum("region", 1))

    val results = Await.result(collection.aggregate(List(g)).toFuture(), 10 seconds ).toList

    val masks = results.map( d => { val id = d.get("_id").get.asDocument()
                                    Mask( id.getString("type").getValue, id.getString("region").getValue  )
                                  })

    Future.successful(masks)
  }

  override def getGridCellMasks( parentDataSet : String, `type` : String, region : String ): ServiceCall[NotUsed,List[GridCellMask]] ={ _ =>

    val db = client.getDatabase(parentDataSet)
    val collection = db.getCollection("masks")

    val f = and(equal("type",`type`),equal("region",region))

    val results = Await.result(collection.find( f ).toFuture(), 10 seconds)

    val masks = results.toList.map(d => GridCellMask( GridCell(d.getLong("gridCellMinX"), d.getLong("gridCellMinY"),d.getLong("size")), d.getString("shapeFile") ))

    Future.successful(masks)
  }

  override def getGridCellMask( parentDataSet : String, `type` : String, region : String ) : ServiceCall[GridCell, GridCellMask] = { gc =>

    val db = client.getDatabase(parentDataSet)

    val collection = db.getCollection("masks")

    val f = and(equal("type",`type`),
            and(equal("region",region),
            and(equal("gridCellMinX", gc.minX),
            and(equal("gridCellMinY", gc.minY),
            and(equal("size", gc.size))))))

    val results = Await.result(collection.find( f ).toFuture(), 10 seconds)

    val mask = results.toList.map(d => GridCellMask( GridCell(d.getLong("gridCellMinX"), d.getLong("gridCellMinY"),d.getLong("size")), d.getString("shapeFile") )).head

    Future.successful(mask)

  }
}

