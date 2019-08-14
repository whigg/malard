package com.earthwave.pointstream.impl

import com.earthwave.pointstream.impl.WriterColumn.Column
import ucar.ma2.DataType
import ucar.nc2.{NetcdfFileWriter, Variable}
import ucar.nc2.write.{Nc4Chunking, Nc4ChunkingStrategy}

object WriterColumn {

  case class Column(name: String, decimalPlaces: Int, dataType: DataType)

}

class NetCdfWriter( filename : String, val srcColumns : List[Column], projectionColumns : List[Column], additionalColumns : List[Column], val schema : Map[String,DataType], deflateLevel :Int = 1 ) {

  val fileName = filename
  private var rowcount: Int = 0

  private val chunker: Nc4Chunking = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard, deflateLevel, true)
  private val writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4, filename, chunker)
  writer.addUnlimitedDimension("row")

  writer.create()

  writer.setRedefineMode(true)

  private val srcVariables = srcColumns.map(x => {
                                                    if( schema.contains(x.name)  )
                                                    {
                                                      val schemaType = schema(x.name)
                                                      println(s"Variable with name ${x.name} and $schemaType and source ${x.dataType}")
                                                      writer.addVariable(null, x.name, schemaType, "row")
                                                    }
                                                    else {
                                                      if(x.name == "dataset")
                                                      {
                                                        println(s"${x.dataType}")
                                                      }
                                                      writer.addVariable(null, x.name, x.dataType, "row")
                                                    }
                                                  })

  private val prjVariables = projectionColumns.map(x => (x.name, writer.addVariable(null, x.name, x.dataType, "row"))).toMap
  private val addVariables = additionalColumns.map( x => writer.addVariable(null, x.name, x.dataType,"row") )


  writer.setRedefineMode(false)


  def writeWithFilter(variables : List[(Variable,ucar.ma2.Array)], mask : Array[Int]) = {

    val origin = Array.ofDim[Int](1)
    origin(0) = rowcount

    //put the packing code in here
    val variablePairs = srcVariables.zip(variables)

    variablePairs.foreach(v =>
          {
            val arr = ArrayHelper.applyMask(v._1.getDataType, v._2._2, mask)
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
    origin(0) = rowcount

    val variablePairs = srcVariables.zip(variables)

    variablePairs.foreach(v =>
    {
      try {
        if (!v._1.getDataType.toString.contentEquals(v._2._1.getDataType.toString)) {
          writer.write(v._1, origin, ArrayHelper.convertArray(v._2._2, v._1.getDataType))
        }
        else {
          writer.write(v._1, origin, v._2._2)
        }
      }
      catch {
        case e : Exception => throw new Exception( s"Error writing Name=[${v._1}] SourceType=[${v._2._1.getDataType}] ToType=[${v._1.getDataType}]" )
        }
    })

    rowcount = rowcount + variables.head._2.getSize.toInt
  }

  def write(variables : List[(Variable,ucar.ma2.Array)], coords : TransformedCoordinates, addCols : List[AnyRef], mask : Array[Int]) = {

    val origin = Array.ofDim[Int](1)
    origin(0) = rowcount

    if(mask.length > 0) {
      //put the packing code in here
      val variablePairs = srcVariables.zip(variables)
      variablePairs.foreach(v => writer.write(v._1, origin, ArrayHelper.applyMask(v._2._1.getDataType , v._2._2, mask)))

      writer.write(prjVariables.get(coords.getX()).get, origin, ArrayHelper.applyMask(DataType.DOUBLE, ucar.ma2.Array.factory(coords.x), mask))
      writer.write(prjVariables.get(coords.getY()).get, origin, ArrayHelper.applyMask(DataType.DOUBLE, ucar.ma2.Array.factory(coords.y), mask))

      val addPairs = addVariables.zip(addCols)
      addPairs.foreach(x => writeArray(x._1, origin, x._2, mask))

      rowcount = rowcount + mask.length
    }
  }


  def rowCount : Int  = {
    rowcount
  }

  def close() ={
    println("Flush called")
    writer.flush()
    println("Flush finished")
    writer.close()
  }

  private def writeArray( v : Variable, origin : Array[Int], a : AnyRef , mask : Array[Int]) ={

    if( v.getDataType == DataType.SHORT) {
      writer.write(v, origin, ArrayHelper.applyMask(DataType.SHORT, ucar.ma2.Array.factory(a.asInstanceOf[Array[Short]]), mask))
    }
    if( v.getDataType == DataType.INT) {
      writer.write(v, origin, ArrayHelper.applyMask(DataType.INT, ucar.ma2.Array.factory(a.asInstanceOf[Array[Int]]), mask))
    }
    else if( v.getDataType == DataType.LONG)
    {
      writer.write(v, origin, ArrayHelper.applyMask(DataType.LONG, ucar.ma2.Array.factory(a.asInstanceOf[Array[Long]]), mask))
    }
    else
    {
      throw new Exception(s"Unsupported data type: ${v.getDataType.toString}")
    }
  }

}
