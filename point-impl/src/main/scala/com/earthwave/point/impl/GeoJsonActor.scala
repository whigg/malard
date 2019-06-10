package com.earthwave.point.impl

import java.io.{BufferedWriter, File, FileWriter}
import java.util.Date

import akka.actor.Actor
import com.earthwave.catalogue.api.{BoundingBoxFilter, Shard}
import com.earthwave.point.api.Messages.{Feature, FeatureCollection, Geometry}
import com.earthwave.point.impl.GeoJsonActor.{GeoJson, GeoJsonFile}

import scala.collection.mutable.{ListBuffer, Map}

object GeoJsonActor
{
  case class GeoJson( shards : List[Shard], bbf : BoundingBoxFilter )

  case class GeoJsonFile(shards : List[Shard], bbf : BoundingBoxFilter, fileName : String)
}

class GeoJsonActor() extends Actor{

  override def receive ={

    case GeoJson(s, bbf ) => { val featureCollection = createGeoJson(s, bbf)
                                sender ! featureCollection }

    case GeoJsonFile(s, bbf, fileName ) => { val featureCollection = createGeoJson(s, bbf)
                                              val writer = new BufferedWriter(new FileWriter(new File(fileName)))
                                              writer.write(FeatureCollection.format.writes(featureCollection).toString())
                                              writer.close()}

  }


  private def createGeoJson( shards : List[Shard], bbf : BoundingBoxFilter ) : FeatureCollection = {

    println(s"Received ${shards.length} to process" )
    val readers = shards.map( s => new NetCdfReader( s.shardName, Set[String]() ) )

    val buffer = new ListBuffer[Feature]()

    for(reader <- readers) {
      println(s"Processing shard ${reader.fileName}")
      val xArr = reader.getVariableByName("x").read()
      val yArr = reader.getVariableByName("y").read()
      val tArr = reader.getVariableByName("time").read()

      val vars = reader.getVariablesAndData(Set("coh", "elev"))

      for (i <- 0 until xArr.getSize.toInt) {
        val x = xArr.getDouble(i)
        val y = yArr.getDouble(i)
        val t = new Date(tArr.getLong(i)*1000)

        if(i%10000==0){println(s"Processed $i rows.")}
        if( x <= bbf.maxX && x >= bbf.minX && y <= bbf.maxY && y >= bbf.minY && t.compareTo(bbf.minT) >= 0 && t.compareTo(bbf.maxT) <=0  ) {
          val properties = Map[String, Double]("id" -> i)

          def nanAsNull(arr: ucar.ma2.Array, i: Int): Double = {
            val res = arr.getDouble(i)
            if (res.isNaN) {
              0.0
            } else {
              res
            }
          }

          vars.foreach(v => properties.+=((v._1.getShortName(), nanAsNull(v._2, i))))

          buffer.append(Feature("Feature", Geometry("Point", List(x, y)), properties.toMap))

        }
      }
      println(s"Completed ${reader.fileName}.")
    }
    readers.foreach( x => x.close() )

    println("Closed all readers")

    FeatureCollection("FeatureCollection", buffer.toList )
  }
}
