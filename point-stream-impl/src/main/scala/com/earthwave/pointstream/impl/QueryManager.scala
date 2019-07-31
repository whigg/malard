package com.earthwave.pointstream.impl

import akka.actor.{Actor, Props}
import com.earthwave.catalogue.api.CatalogueService
import com.earthwave.point.api.Messages.Query
import com.earthwave.point.impl.{NetCdfReader, NetCdfWriter, WriterColumn}
import com.earthwave.pointstream.api.{QueryStatus, StreamQuery}
import com.earthwave.pointstream.impl.QueryManagerMessages.{Completed, IsCompleted, ProcessQuery}
import ucar.ma2.DataType

import scala.concurrent.Await
import scala.concurrent.duration._

object QueryManagerMessages
{
  case class ProcessQuery( cacheName : String, streamQuery : StreamQuery )
  case class IsCompleted( cacheName : String  )
  case class Completed( s : QueryStatus)
}

class QueryManager( catalogueService : CatalogueService) extends Actor {

  val queryProcessors = List.range[Int](0,1,1).map( x => context.actorOf(Props(new QueryProcessor(catalogueService)),s"Processor_$x"))

  var availableProcessors = queryProcessors

  var queuedRequests = List[ProcessQuery]()
  var processingRequests = List[ProcessQuery]()
  var completedRequests = List[QueryStatus]()


  override def receive={
    case q : ProcessQuery => {
      if( availableProcessors.isEmpty )
      {
        queuedRequests = q :: queuedRequests
      }
      else
      {
        val processor = availableProcessors.head
        availableProcessors = availableProcessors.tail
        processingRequests = q :: processingRequests
        processor ! q
      }
    }
    case c : IsCompleted => {
      println("Received is completed request")
      if( !queuedRequests.filter(p => c.cacheName == p.cacheName).isEmpty )
      {
        println(s"${c.cacheName} is queued.")
        sender ! QueryStatus(false, c.cacheName, s"Queued")
      }
      else if( !processingRequests.filter(p => c.cacheName == p.cacheName).isEmpty )
      {
        println(s"${c.cacheName} is processing.")
        sender ! QueryStatus(false, c.cacheName , "Processing.")
      }
      else if( !completedRequests.filter(p => c.cacheName == p.cacheName).isEmpty )
      {
        println(s"${c.cacheName} is completed")
        val queryStatus = completedRequests.filter(p => c.cacheName == p.cacheName).head
        sender ! queryStatus
      }

    }
    case c : Completed => {
      println(s"Completed ${c.s.cacheName}")
      completedRequests = c.s :: completedRequests
      processingRequests = processingRequests.filterNot(r => r.cacheName == c.s.cacheName )

      if( !queuedRequests.isEmpty )
      {
        val nextRequest = queuedRequests.head
        queuedRequests = queuedRequests.tail
        sender ! nextRequest
      }
      else
      {
        availableProcessors = sender :: availableProcessors
      }
    }
    case _ => { println("ERROR ERROR ERROR") }
  }


}

class QueryProcessor( catalogueService: CatalogueService ) extends Actor {

  override def receive ={

    case pq : ProcessQuery => {

      var requestStatus = ""

      try {
        val q = pq.streamQuery
        val future = catalogueService.shards(pq.streamQuery.envName, q.parentDSName, q.dsName, q.region).invoke(q.bbf)
        val shards = Await.result(future, 10 seconds)

        println(s"Found [${shards.length}]")

        val cols = if (q.projections.isEmpty) {
          scala.collection.mutable.Set()
        } else {
          val c = scala.collection.mutable.Set("x", "y", "time")
          q.projections.foreach(p => c.+=(p))
          c
        }

        requestStatus = if (!shards.isEmpty) {
          val tempReader = new NetCdfReader(shards.head.shardName, cols.toSet)
          val columns = tempReader.getVariables().map(x => WriterColumn.Column(x.getShortName, 0, x.getDataType))
          tempReader.close()
          println(s"Now have ${shards.length}")
          val writer = new NetCdfWriter(pq.cacheName, columns, Map[String, DataType]())
          try {
            shards.foreach(x => {
              val reader = new NetCdfReader(x.shardName, cols.toSet)
              try {
                val data = reader.getVariablesAndData(Query(q.bbf, q.projections, q.filters))
                println(s"Writing ${data._2.length} rows.")
                if (data._2.length != 0) {
                  writer.writeWithFilter(data._1, data._2)
                }
              }
              catch {
                case e: Exception => println(s"Error reading shard ${x.shardName}")
              }
              finally {
                reader.close()
              }
            })
          }
          finally {
            writer.close()
          }
          "Success"
        }
        else {
          "Error: Empty resultset."
        }
      }
      catch {
        case e : Exception => { requestStatus = s"Error: Failed to query results ${e.printStackTrace()}" }

      }
      sender ! Completed(QueryStatus(true, pq.cacheName, requestStatus))
    }
  }
}
