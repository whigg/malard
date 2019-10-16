package com.earthwave.pointstream.impl

import akka.actor.{Actor, ActorPath, ActorSystem, Props, Terminated}
import com.earthwave.catalogue.api.CatalogueService
import com.earthwave.pointstream.api.SwathProcessorStatus
import com.earthwave.pointstream.impl.SwathGridCellPublisher.WorkerSwathToGridCellRequest
import com.earthwave.pointstream.impl.SwathProcessorMessages.IsCompleted

import scala.concurrent.{ExecutionContext, Future}

object SwathProcessorMessages
{
  case class IsCompleted( swathFile : String, hash : Long)
  case class Completed( swathProcessorStatus : SwathProcessorStatus )
}

class SwathProcessor( catalogueService : CatalogueService,system : ActorSystem) extends Actor {

  import org.slf4j.LoggerFactory
  private val log = LoggerFactory.getLogger(SwathProcessor.super.getClass)

  val swathProcessors = List.range[Int](1,2,1).map( x => system.actorOf(Props(new SwathGridCellPublisher(x)),s"SwathProcessor_$x"))

  //watch the remote actors
  swathProcessors.foreach( sp =>  context.watch(sp) )

  var availableProcessors = swathProcessors

  swathProcessors.foreach(a => a ! Messages.InitiatingConnection() )

  var queuedRequests = List[WorkerSwathToGridCellRequest]()
  var processingRequests = List[(WorkerSwathToGridCellRequest, ActorPath)]()
  var completedRequests = List[SwathProcessorStatus]()
  var terminatedRequests = List[WorkerSwathToGridCellRequest]()

  var retries = Map[String,Int]()

  override def receive={
    case Messages.WorkerConnected() => {
      if( retries.contains( sender.path.name))
      {
        retries.-=(sender.path.name)
      }
    }
    case q : WorkerSwathToGridCellRequest => {

      if( availableProcessors.isEmpty )
      {
        queuedRequests = q :: queuedRequests
      }
      else
      {
        val processor = availableProcessors.head
        availableProcessors = availableProcessors.tail
        processingRequests = (q,processor.path) :: processingRequests
        processor ! q
      }
      log.info(s"SwathProcessor Queued Requests [${queuedRequests.length}] Available Processors [${availableProcessors.length}] ProcessingRequests [${processingRequests.length}] Completed Requests [${completedRequests.length}]  ")
    }
    case c : IsCompleted => {
      log.info( s"Workers available:[${availableProcessors.length}] Processing Requests: [${processingRequests.length}]" )
      if (availableProcessors.isEmpty && processingRequests.isEmpty)
      {
        //Bad news all the workers have stopped and are not available.
        sender ! SwathProcessorStatus(true, c.swathFile, s"Error","No available workers", None, c.hash)
      }
      else if( !queuedRequests.filter(p => c.hash == p.userRequest.hash).isEmpty )
      {
        log.info(s"${c.swathFile} is queued.")
        if( !availableProcessors.isEmpty )
        {
          val processor = availableProcessors.head
          val request = queuedRequests.filter(p => c.hash == p.userRequest.hash).head
          queuedRequests = queuedRequests.filterNot(p => c.hash == p.userRequest.hash)
          availableProcessors = availableProcessors.tail
          processingRequests = (request, processor.path) :: processingRequests

          processor ! request
        }
        sender ! SwathProcessorStatus(false, c.swathFile, s"Queued","Ok.", None, c.hash)
      }
      else if( !processingRequests.filter(p => c.hash == p._1.userRequest.hash).isEmpty )
      {
        log.info(s"${c.swathFile} is processing.")
        sender ! SwathProcessorStatus(false, c.swathFile, "Processing.","Ok.", None, c.hash)
      }
      else if( !completedRequests.filter(p => c.hash == p.hash).isEmpty )
      {
        log.info(s"${c.swathFile} is completed")
        val queryStatus = completedRequests.filter(p => c.hash == p.hash).head
        sender ! queryStatus
      }
      else
      {
        log.info(s"Error status of ${c.swathFile} cannot be determined.")
      }

    }
    case c : SwathProcessorMessages.Completed => {
      log.info(s"Swath Processor Completed ${c.swathProcessorStatus.inputFileName}")
      completedRequests = c.swathProcessorStatus :: completedRequests
      processingRequests = processingRequests.filterNot(r => r._1.userRequest.inputFileName == c.swathProcessorStatus.inputFileName )

      if( !queuedRequests.isEmpty )
      {
        val nextRequest = queuedRequests.head
        queuedRequests = queuedRequests.tail
        processingRequests = (nextRequest, sender.path ) :: processingRequests
        sender ! nextRequest
      }
      else
      {
        availableProcessors = sender :: availableProcessors
      }

      log.info(s"SwathProcessor Queued Requests [${queuedRequests.length}] Available Processors [${availableProcessors.length}] ProcessingRequests [${processingRequests.length}] Completed Requests [${completedRequests.length}]  ")
    }
    case Terminated(sp) => {
      log.error(s"Remote Actor [${sp.path.name} has been terminated]")
      availableProcessors = availableProcessors.filterNot(f => f.path == sp.path)
      log.info(s"Now [${availableProcessors.length}] available processors ")

      val requests = processingRequests.filter( f => f._2 == sp.path )
      if( !requests.isEmpty )
      {
        val request = requests.head._1
        log.info(s"Request ${request.userRequest.inputFileName} is going to be queued because the process was terminated.")
        queuedRequests = request :: queuedRequests
      }

      processingRequests = processingRequests.filterNot(f => f._2 == sp.path)

      implicit val executionContext = ExecutionContext.global

      Future{
        val retryCount = retries.getOrElse(sp.path.name,0)
        if( retryCount <= Messages.maxRetryCount ) {
          Thread.sleep(60 * 1000)

          log.info("Finished waiting for restart.")
          val instance = sp.path.name.split("_")(1)
          log.info(s"Creating actor [${sp.path.name}] Instance [$instance]. RetryCount [$retryCount]")
          val actor = system.actorOf(Props(new SwathGridCellPublisher(instance.toInt)), sp.path.name)

          actor ! Messages.InitiatingConnection()
          context.watch(actor)

          availableProcessors = actor :: availableProcessors
          log.info(s"Available processors [${availableProcessors.length}].")

          retries.+=((sp.path.name, retryCount + 1))
        }
      }
    }
    case _ => { log.error("Unexpected message type received.") }
  }
}
