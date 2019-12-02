package com.earthwave.pointstream.impl

import java.time.{LocalDateTime, ZoneOffset}

import com.earthwave.pointstream.impl.WriterColumn.Column
import ucar.ma2.DataType
import ucar.nc2.Variable

import scala.io.Source

object TestHarness {

  def main(args : Array[String]):Unit ={

    //dummy catalogue path
    val path = "C:\\Earthwave\\v2\\mtngla\\catalogue.csv"
    val outPath = "C:\\Earthwave\\v2\\mtngla\\test.nc"

    val files = Source.fromFile(path).getLines().map( str => {val tokens = str.split(",")
                                                                        tokens(1)  }).drop(1).toList

    val startTime = LocalDateTime.now()

    val tmpreader = new NetCdfReader(files.head, Set[String]())
    val writer = new NetCdfWriter(outPath, tmpreader.getVariables().map( x => WriterColumn.Column(x.getShortName, 0, x.getDataType)),List[Column](),List[Column](),Map[String, DataType]())
    tmpreader.close()

    val readers = files.map( f => new NetCdfReader(f, Set[String]()))

    readers.map( r => {
                        val data = r.getVariables()
                        writer.writeBuffered(data)
                      }   )

    writer.writeBuffered( List[Variable](), true )

    readers.foreach(r => r.close())

    writer.close()

    val elapsedTime = LocalDateTime.now().atOffset(ZoneOffset.UTC).toEpochSecond - startTime.atOffset(ZoneOffset.UTC).toEpochSecond

    println(s"Took [$elapsedTime]s.")

  }

}
