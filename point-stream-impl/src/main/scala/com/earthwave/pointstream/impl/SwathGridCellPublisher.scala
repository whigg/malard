package com.earthwave.pointstream.impl

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import akka.actor.{Actor, Terminated}
import akka.remote.DisassociatedEvent
import com.earthwave.catalogue.api.{GridCellFile, SwathDetail}
import com.earthwave.environment.api.Environment
import com.earthwave.pointstream.api.{GridCellPoints, SwathProcessorStatus, SwathToGridCellsRequest}
import com.earthwave.pointstream.impl.SwathGridCellPublisher.WorkerSwathToGridCellRequest
import com.earthwave.projection.api.Projection
import ucar.ma2.DataType

import scala.collection.mutable.ListBuffer
import scala.util.Try


object SwathGridCellPublisher
{
  case class WorkerSwathToGridCellRequest(    fileId : Int
                                            , userRequest : SwathToGridCellsRequest
                                            , environment : Environment
                                            , projections : List[Projection])


}

class SwathGridCellPublisher( val idx : Int ) extends Actor {

  val cache = new CoordinateTransformCache()

  import org.slf4j.LoggerFactory
  private val log = LoggerFactory.getLogger(SwathGridCellPublisher.getClass)

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, DisassociatedEvent.getClass)
  }

  override def receive = {
    case _ : Messages.InitiatingConnection =>
    {
      log.info(s"Worker [$idx] Received Initiating connection.")
      sender ! Messages.WorkerConnected()
    }
    case r :  WorkerSwathToGridCellRequest=>{

      val date = LocalDateTime.ofEpochSecond(r.userRequest.dataTime,0,ZoneOffset.UTC)
      val dateStr = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
      val gridCellSize = r.userRequest.gridCellSize



      log.info(s"About to open file ${r.userRequest.inputFilePath}/${r.userRequest.inputFileName}")
      val tryReader = Try(new NetCdfReader( s"${r.userRequest.inputFilePath}/${r.userRequest.inputFileName}", Set[String]() ))

      val continue = tryReader match {
        case scala.util.Success(a) => (true, Some(a) )
        case scala.util.Failure(e) => {
                                        val msg = s"Failed to open file  ${r.userRequest.inputFilePath}/${r.userRequest.inputFileName} with error ${e.getMessage}"
                                        sender ! SwathProcessorMessages.Completed(SwathProcessorStatus(true, r.userRequest.inputFileName, "Error", msg, None, r.userRequest.hash))
                                        log.error( s"$msg")
                                        (false, None)
                                      }
        }

      if( continue._1 ) {
        val reader = continue._2.get
        try {
          val lat = reader.getVariableByName("lat").read()
          val lon = reader.getVariableByName("lon").read()

          val columns = reader.getVariables().map(x => WriterColumn.Column(x.getShortName, 0, x.getDataType))

          val xyeOption = CoordinateTransform.transformArray(lat, lon, r.projections, cache)

          val projectedColumns = xyeOption.getOrElse(throw new Exception(s"Coordinate transform failed for file: ${r.userRequest.inputFileName}")).getVariableDefinitions()

          val xye = xyeOption.get
          val numberOfPoints = xye.x.length

          val ts = date.toEpochSecond(ZoneOffset.UTC)
          val t = new Array[Long](numberOfPoints)
          val f = new Array[Int](numberOfPoints)
          val rowId = new Array[Int](numberOfPoints)

          val minimumFilters = r.userRequest.columnFilters.map(c => (reader.getVariableByName(c.column).read(), Operators.getOperator(c.op, c.threshold)))

          val intermediatePath = s"${r.environment.swathIntermediatePath}/${r.userRequest.parentDataSet}/${r.userRequest.dataSet}/${r.userRequest.region}/y${date.getYear}/m${date.getMonthValue}/"
          FileHelper.createDir(intermediatePath)

          //Walk through the mapped coordinates
          //Bucketing the grid cells
          var mask = Map[GridCellPoints, ListBuffer[Int]]()
          for (i <- 0 until numberOfPoints) {
            //Now do the bucketing
            val cellX = Math.floor(xye.x(i) / gridCellSize).toLong * gridCellSize
            val cellY = Math.floor(xye.y(i) / gridCellSize).toLong * gridCellSize
            val fileName = s"$intermediatePath${idx}_${dateStr}_${xye.proj(i)}_${cellX}_${cellY}_${r.userRequest.inputFileName}"
            val gridCell = GridCellPoints(xye.proj(i), cellX, cellY, LocalDateTime.of(date.getYear, date.getMonth, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toEpochSecond, gridCellSize, List(fileName))

            t(i) = ts
            f(i) = r.fileId
            rowId(i) = i

            val indices = mask.getOrElse(gridCell, new ListBuffer[Int]())
            //Only add to the mask if not in the filter
            val exclude: Boolean = minimumFilters.foldLeft(false)((x, y) => {
              if (y._2.op(y._1.getDouble(i)) || x == true) {
                true
              }
              else {
                false
              }
            })
            if (!exclude) {
              indices.append(i)
            }
            mask.+=((gridCell, indices))
          }

          mask = mask.filter(x => x._2.length > 0)

          val addData = List[AnyRef](t, f, rowId)

          val addColumns = List(WriterColumn.Column("time", 0, DataType.LONG), WriterColumn.Column("swathFileId", 0, DataType.INT), WriterColumn.Column("rowId", 0, DataType.INT))

          //Now for each grid cell build the data arrays
          //The array reads are expensive, so we do them once up front
          //Then apply the mask for each gridcell that is very quick, even though
          //the array is iterated over many times.
          val varsAndData = reader.getVariablesAndData(r.userRequest.includeColumns)

          var results = Map[GridCellPoints, (String, Int)]()
          for ((k, v) <- mask) {
            val dfw = new NetCdfWriter(k.fileName.head, columns, projectedColumns, addColumns, Map[String, DataType]())

            dfw.write(varsAndData, xye, addData, v.toArray)

            results.+=((k, (k.fileName.head, dfw.rowCount)))

            dfw.close()
          }

          val filteredPointCount = results.map(w => w._2._2).sum
          //Now write into the Swath Acquisition Table
          //This enables us to invert the data mapping and get the points back in the original swath format.
          val swathDetail = SwathDetail(r.userRequest.dataSet
            , r.userRequest.region
            , "catalogue"
            , r.userRequest.inputFileName
            , r.fileId
            , date.getYear
            , date.getMonthValue
            , numberOfPoints
            , filteredPointCount
            , LocalDateTime.now()
            , results.map(kv => GridCellFile(kv._1.minT, kv._1.projection, kv._1.minX, kv._1.minY, kv._2._2, kv._2._1)).toList)

          sender ! SwathProcessorMessages.Completed(SwathProcessorStatus(true, r.userRequest.inputFileName, "Success", "Written successfully", Some(swathDetail), r.userRequest.hash))
        }
        catch {
          case e: Exception => {

            e.printStackTrace()
            sender ! SwathProcessorMessages.Completed( SwathProcessorStatus(true, r.userRequest.inputFileName, "Error", e.getMessage, None, r.userRequest.hash))
          }
        }
        finally {
          reader.close()
        }
      }
    }
    case d : DisassociatedEvent=>
    {
      log.warn(s"SwathGridCellPublisher disassociated event received.")
      context.system.terminate()
    }
    case _ => {
      log.error("Unexpected message received.")
    }
  }

  override def postStop(): Unit = {
    log.info("Stop called.")
    context.system.terminate()
  }



}
