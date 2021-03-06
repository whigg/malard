package com.earthwave.pointstream.impl

import com.earthwave.pointstream.api.Query
import org.gdal.ogr.Layer

import scala.collection.JavaConverters._
import ucar.nc2.{NetcdfFile, Variable}

class NetCdfReader(val fileName : String, projection : Set[String]) {

  private val file =  NetcdfFile.open(fileName)
  private val variables = if(projection.isEmpty)
                          {
                            file.getVariables.asScala.toList
                          }else
                          {
                            file.getVariables.asScala.toList.filter(v => projection.contains(v.getShortName))
                          }

  def getVariableByName(  name : String ) : Variable ={

    // put the code to the unpacking in here

    val filter = variables.filter( x => x.getShortName.contentEquals(name) )

    filter.headOption.getOrElse(throw new Exception( s"Column [$name] is not in the data file." ))
  }

  def getVariableExists( name : String ) : Boolean ={

    val filter = variables.filter( x => x.getShortName.contentEquals(name) )

    if( filter.isEmpty ){ return false }else{ return true }
  }

  def getVariables( ) : List[Variable] = {

    // put the code to do the unpacking in here

    variables
  }

  def getVariablesAndData(q : Query, maskFilter : Mask) : (List[(Variable, ucar.ma2.Array)],Array[Int]) ={

    val tempVariables = variables.map(v => (v,v.read()))

    val x = tempVariables.filter( x => x._1.getShortName.contentEquals(q.bbf.xCol.toLowerCase()) ).head._2
    val y = tempVariables.filter( y => y._1.getShortName.contentEquals(q.bbf.yCol.toLowerCase()) ).head._2
    val t = tempVariables.filter( t => t._1.getShortName.contentEquals( "time")).head._2

    val filters = q.filters.map( f => { (Operators.getOperator(f.op,f.threshold), tempVariables.filter( v => v._1.getShortName.contentEquals( f.column )).headOption.getOrElse( throw new Exception( s"Column ${f.column} is not in the data"  ))._2)  } )

    val mask = ArrayHelper.buildMask(x, y, t, q.bbf, filters, maskFilter )

    (tempVariables, mask)
  }

  def getVariablesAndData( withColumns : Set[String]) : List[(Variable, ucar.ma2.Array)] ={

    val vars = if(!withColumns.isEmpty){ variables.filter( v => withColumns.contains(v.getShortName) )}else{variables}
    vars.map(v => (v,v.read()))
  }

  def close() ={
    file.close()
  }

}
