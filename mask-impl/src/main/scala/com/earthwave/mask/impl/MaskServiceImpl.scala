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

  override def publishMask( envName : String, parentDataSet : String, dataSet : String, `type` : String, region : String ) : ServiceCall[MaskFile, String] = { x =>

    val environment = Await.result(env.getEnvironment(envName).invoke(), 10 seconds )

    val db = client.getDatabase(parentDataSet)
    val collection = db.getCollection("masks" )

    val outputDir = s"${environment.maskPublisherPath}/${parentDataSet}/static/$dataSet/${`type`}/$region/cell_x${x.gridCell.minX}_y${x.gridCell.minY}_s${x.gridCell.size}/"

    val dir = new File(outputDir)
    if(!dir.exists()) {
      dir.mkdirs()
    }

    val outputPath = s"$outputDir${x.fileName}".replace("//","/")

    val inputFile = java.nio.file.Paths.get(s"${x.sourceFilePath}/${x.fileName}")
    val outputFile = java.nio.file.Paths.get(outputPath)

    Files.copy(inputFile, outputFile,StandardCopyOption.REPLACE_EXISTING )

    //check existence of output file
    val file = new File(outputPath)
    if( file.exists() )
    {
      println(s"File written successfully: ${outputPath}")
    }
    else
    {
      throw new Exception(s"Error copying inputFile $inputFile to $outputFile")
    }

    val doc = Document( "envName" -> envName,
                        "dataSet" -> dataSet,
                        "type" -> `type`,
                        "region" -> region,
                        "fileName" -> outputPath,
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

    println(s"Mask published for GridCell x=${x.gridCell.minX}, y=${x.gridCell.minY} and size=${x.gridCell.minY} to $outputPath")
    Future.successful(s"Published: ${outputFile}")
  }

  override def getMasks(envName : String, parentDataSet : String, dataSet : String): ServiceCall[NotUsed,List[Mask]] = { _ =>

    val db = client.getDatabase(parentDataSet)
    val collection = db.getCollection("masks")

    val f = filter(and(equal("dataSet",dataSet),equal("envName",envName)))

    val groupByCols = Document( "type" -> "$type"
                              , "region" -> "$region" )

    val g = group( groupByCols, sum("type", 1), sum("region", 1))

    val results = Await.result(collection.aggregate(List(f,g)).toFuture(), 10 seconds ).toList

    val masks = results.map( d => { val id = d.get("_id").get.asDocument()
                                    Mask( id.getString("type").getValue, id.getString("region").getValue  )
                                  })

    Future.successful(masks)
  }

  override def getGridCellMasks(envName : String, parentDataSet : String, dataSet : String, `type` : String, region : String ): ServiceCall[NotUsed,List[GridCellMask]] ={ _ =>

    val db = client.getDatabase(parentDataSet)
    val collection = db.getCollection("masks")

    val f = and(equal("dataSet",dataSet),and(equal("envName",envName),and(equal("type",`type`),equal("region",region))))

    val results = Await.result(collection.find( f ).toFuture(), 10 seconds)

    val masks = results.toList.map(d => GridCellMask( GridCell(d.getLong("gridCellMinX"), d.getLong("gridCellMinY"),d.getLong("size")), d.getString("fileName") ))

    Future.successful(masks)
  }

  override def getGridCellMask(envName : String, parentDataSet : String, dataSet : String, `type` : String, region : String ) : ServiceCall[GridCell, GridCellMask] = { gc =>

    val db = client.getDatabase(parentDataSet)

    val collection = db.getCollection("masks")

    val f = and(equal("envName",envName),
            and(equal("dataSet", dataSet),
            and(equal("type",`type`),
            and(equal("region",region),
            and(equal("gridCellMinX", gc.minX),
            and(equal("gridCellMinY", gc.minY),
            and(equal("size", gc.size))))))))

    val results = Await.result(collection.find( f ).toFuture(), 10 seconds)

    val mask = results.toList.map(d => GridCellMask( GridCell(d.getLong("gridCellMinX"), d.getLong("gridCellMinY"),d.getLong("size")), d.getString("fileName") ))

    val res: GridCellMask = if( mask.isEmpty ){ GridCellMask(gc, "NoMask") }else{mask.head}

    Future.successful(res)

  }
}

