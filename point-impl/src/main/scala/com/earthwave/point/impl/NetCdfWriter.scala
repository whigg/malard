package com.earthwave.point.impl

import com.earthwave.point.impl.WriterColumn.Column
import ucar.ma2.DataType
import ucar.nc2.{NetcdfFileWriter, Variable}
import ucar.nc2.write.{Nc4Chunking, Nc4ChunkingStrategy}

object WriterColumn {

  case class Column(name: String, decimalPlaces: Int, dataType: DataType)

}

class NetCdfWriter( filename : String, val srcColumns : List[Column], deflateLevel :Int = 1 ) {

  val fileName = filename
  private var rowcount: Int = 0

  private val chunker: Nc4Chunking = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard, deflateLevel, true)
  private val writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4, filename, chunker)
  writer.addUnlimitedDimension("row")

  writer.create()

  writer.setRedefineMode(true)

  private val srcVariables = srcColumns.map(x => writer.addVariable(null, x.name, x.dataType, "row"))

  writer.setRedefineMode(false)


  def writeWithFilter(variables : List[(Variable,ucar.ma2.Array)], mask : Array[Int]) = {

    val origin = Array.ofDim[Int](1)
    origin(0) = if( rowcount == 0 ){ 0 }else{(rowcount)-1}

    //put the packing code in here
    val variablePairs = srcVariables.zip(variables)

    variablePairs.foreach(v =>
          {
            val arr = ArrayHelper.applyMask(v._2._2, mask)
              try {
                writer.write(v._1, origin, arr)
              }
              catch{
                case e : Exception => println( s"Error occurred writing ${v._1.getShortName}")

              }
          })

    rowcount = rowcount + mask.length
}
  def write(variables : List[(Variable, ucar.ma2.Array)]) = {
    val origin = Array.ofDim[Int](1)
    origin(0) = if( rowcount == 0 ){ 0 }else{(rowcount)-1}

    val variablePairs = srcVariables.zip(variables)

    variablePairs.foreach(v =>
    {
      writer.write(v._1, origin, v._2._2)
    })

    rowcount = rowcount + variables.head._2.getSize.toInt
  }

  def rowCount : Int  = {
    rowcount
  }

  def close() ={

    writer.close()
  }
}
