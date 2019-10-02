package com.earthwave.pointstream.impl

import akka.actor.{Actor, ActorPath, ActorRef, ActorSystem, Props, Terminated}
import akka.remote.DisassociatedEvent
import com.earthwave.catalogue.api.{CatalogueService, Shard}
import com.earthwave.pointstream.api.{Query, QueryStatus, StreamQuery}
import com.earthwave.pointstream.impl.Messages.InitiatingConnection
import com.earthwave.pointstream.impl.QueryManagerMessages.{CompleteRequest, Completed, InitiateQueryRequest, IsCompleted, ProcessQuery, ShardToProcess}
import com.earthwave.pointstream.impl.WriterColumn.Column
import ucar.ma2.DataType
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

object QueryManagerMessages
{
  case class ProcessQuery( cacheName : String, streamQuery : StreamQuery, shards : List[Shard] )
  case class IsCompleted( cacheName : String  )
  case class Completed( s : QueryStatus)

  case class InitiateQueryRequest( cacheName : String, streamQuery : StreamQuery)
  case class ShardToProcess( shard : Shard )
  case class CompleteRequest()
}

class QueryManager( catalogueService : CatalogueService,system : ActorSystem) extends Actor {

  private val log = LoggerFactory.getLogger(QueryManager.super.getClass)

  val queryProcessors = List.range[Int](1,3,1).map( x => system.actorOf(Props(new QueryProcessor(x)),s"QueryProcessor_$x"))

  queryProcessors.foreach( qp => context.watch(qp) )

  var availableProcessors = queryProcessors
  queryProcessors.foreach(a => a ! InitiatingConnection() )

  var queuedRequests = List[ProcessQuery]()
  var processingRequests = List[(ProcessQuery,ActorPath)]()
  var completedRequests = List[QueryStatus]()
  var retries = Map[String,Int]()

  def processQuery( q : ProcessQuery, processor : ActorRef ) ={
    processor ! InitiateQueryRequest( q.cacheName, q.streamQuery )
    q.shards.foreach( s => processor ! ShardToProcess(s) )
    processor ! CompleteRequest()
  }


  override def receive={
    case Messages.WorkerConnected() =>{
      if( retries.contains( sender.path.name))
      {
        retries.-=(sender.path.name)
      }
    }
    case q : ProcessQuery => {
      if( availableProcessors.isEmpty )
      {
        queuedRequests = q :: queuedRequests
      }
      else
      {
        val processor = availableProcessors.head
        availableProcessors = availableProcessors.tail
        processingRequests = (q, processor.path) :: processingRequests

        processQuery( q, processor)
      }
    }
    case c : IsCompleted => {
      log.info("Received is completed request")
      log.info( s"Workers available:[${availableProcessors.length}] Processing Requests: [${processingRequests.length}]" )
      if (availableProcessors.isEmpty && processingRequests.isEmpty)
      {
        //Bad news all the workers have stopped and are not available.
        sender ! QueryStatus(true, "", s"Error","No available workers")
      }
      else if( !queuedRequests.filter(p => c.cacheName == p.cacheName).isEmpty )
      {
        log.info(s"${c.cacheName} is queued.")

        if( !availableProcessors.isEmpty )
        {
          val processor = availableProcessors.head
          val request = queuedRequests.filter(p => c.cacheName == p.cacheName).head
          queuedRequests = queuedRequests.filterNot(p => c.cacheName == p.cacheName)
          availableProcessors = availableProcessors.tail
          processingRequests = (request, processor.path) :: processingRequests

          processQuery( request, processor)
        }

        sender ! QueryStatus(false, c.cacheName, s"Queued", "Ok.")
      }
      else if( !processingRequests.filter(p => c.cacheName == p._1.cacheName).isEmpty )
      {
        log.info(s"${c.cacheName} is processing.")
        sender ! QueryStatus(false, c.cacheName , "Processing", "Ok.")
      }
      else if( !completedRequests.filter(p => c.cacheName == p.cacheName).isEmpty )
      {
        log.info(s"${c.cacheName} is completed")
        val queryStatus = completedRequests.filter(p => c.cacheName == p.cacheName).head
        sender ! queryStatus
      }

    }
    case c : Completed => {
      log.info(s"Completed ${c.s.cacheName}")
      completedRequests = c.s :: completedRequests
      processingRequests = processingRequests.filterNot(r => r._1.cacheName == c.s.cacheName )

      if( !queuedRequests.isEmpty )
      {
        val nextRequest = queuedRequests.head
        queuedRequests = queuedRequests.tail
        processingRequests = (nextRequest,sender.path) :: processingRequests
        processQuery(nextRequest,sender)
      }
      else
      {
        availableProcessors = sender :: availableProcessors
      }
    }
    case Terminated(qp) =>{
      log.error( s"Detected terminated remote actor ${qp.path}" )

      availableProcessors = availableProcessors.filterNot(f => f.path == qp.path)
      val requests = processingRequests.filter( f => f._2 == qp.path )

      if( !requests.isEmpty )
      {
        val request = requests.head._1
        log.info(s"Request ${request.cacheName} is going to be queued because the process was terminated.")
        queuedRequests = request :: queuedRequests
      }

      processingRequests = processingRequests.filterNot(f => f._2 == qp.path)

      implicit val executionContext = ExecutionContext.global

      Future {
        Thread.sleep(60 * 1000)
        val retryCount = retries.getOrElse(qp.path.name, 0)
        if (retryCount <= Messages.maxRetryCount) {

          val instance = qp.path.name.split("_")(1)
          val actor = system.actorOf(Props(new QueryProcessor(instance.toInt)), qp.path.name)
          log.info(s"Creating actor [${actor.path.name}] Instance=[${instance.toInt}].")
          context.watch(actor)

          actor ! Messages.InitiatingConnection()

          availableProcessors = actor :: availableProcessors

          retries.+=((actor.path.name, retryCount + 1))
        }
      }
    }
    case _ => { log.error("Unsupported message type received.") }
  }


}

class QueryProcessor( instance : Int ) extends Actor {

  private val log = LoggerFactory.getLogger( QueryProcessor.super.getClass )

  private var shards = List[Shard]()
  private var queryHeader : Option[InitiateQueryRequest]= None

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, DisassociatedEvent.getClass)
  }


  override def receive ={
    case Messages.InitiatingConnection() =>
    {
      log.info(s"Worker [$instance] Received Initiating connection.")
      sender ! Messages.WorkerConnected()
    }
    case qr : InitiateQueryRequest =>{
      shards = List[Shard]()
      log.info(s"Received initiate request for ${qr.cacheName}.")
      queryHeader = Some(qr)
    }
    case ShardToProcess(s) =>{
      log.info(s"Received shard name ${s.shardName}")
      shards = s :: shards
    }
    case CompleteRequest() =>{
      log.info(s"Received complete request.")
      processQuery( ProcessQuery(queryHeader.get.cacheName, queryHeader.get.streamQuery, shards ) )
    }
    case d : DisassociatedEvent=>
    {
      log.warn(s"SwathGridCellPublisher disassociated event received.")
      context.system.terminate()
    }

    case _ => { log.error(s"QueryProcessor Unsupported message type received. ${sender.path.name} ") }
  }

  def processQuery( pq : ProcessQuery) {
    var requestStatus = ""

    try {
      val q = pq.streamQuery
      log.info(s"QueryProcessor [$instance]. Received ${q.envName}, ${q.region}, ${q.parentDSName}, ${q.dsName} ")

      val shards = pq.shards
      log.info(s"Found [${shards.length}]")

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
        log.info(s"Now have ${shards.length}")
        val writer = new NetCdfWriter(pq.cacheName, columns, List[Column](), List[Column](), Map[String, DataType]())
        try {
          shards.foreach(x => {
            val reader = new NetCdfReader(x.shardName, cols.toSet)
            try {
              val data = reader.getVariablesAndData(Query(q.bbf, q.projections, q.filters))
              log.info(s"Writing ${data._2.length} rows.")
              if (data._2.length != 0) {
                writer.writeWithFilter(data._1, data._2)
              }
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
      case e: Exception => {
        val msg = s"Error: Failed to query results. ${e.getMessage}"
        log.error(msg)
        requestStatus = msg
      }
    }
    if (requestStatus.contentEquals("Success")) {
      log.info(s"Request complete with success")
      sender ! Completed(QueryStatus(true, pq.cacheName, "Success", requestStatus))
    }
    else {
      log.info(s"Request complete in failed state.")
      sender ! Completed(QueryStatus(true, "Error: No File", "Error", requestStatus))
    }
  }

  override def postStop(): Unit = {
    log.info("Stop called.")
    context.system.terminate()
  }
}
