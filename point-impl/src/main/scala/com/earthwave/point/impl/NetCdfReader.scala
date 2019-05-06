package com.earthwave.core

import scala.collection.JavaConverters._
import ucar.nc2.{NetcdfFile, Variable}

class NetCdfReader(val fileName : String) {

  private val file =  NetcdfFile.open(fileName)
  private val variables = file.getVariables.asScala.toList

  def getVariableByName(  name : String ) : Variable ={

    // put the code to the unpacking in here

    val filter = variables.filter( x => x.getShortName.contentEquals(name) )

    filter.head
  }

  def getVariables( ) : List[Variable] = {

    // put the code to the unpacking in here

    variables
  }

  def getVariablesAndData() : List[(Variable, ucar.ma2.Array)] ={

    variables.map(v => (v,v.read()))
  }

  def getVariablesAndData( withColumns : Set[String]) : List[(Variable, ucar.ma2.Array)] ={

    val vars = variables.filter( v => withColumns.contains(v.getShortName) )
    vars.map(v => (v,v.read()))
  }

  def close() ={
    file.close()
  }

}
