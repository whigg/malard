package com.earthwave.validation.impl

import java.io.File

import com.earthwave.point.impl.NetCdfReader
import com.earthwave.validation.api.{ValidationError, ValidationErrors, ValidationRequest, ValidationService}
import com.lightbend.lagom.scaladsl.api.ServiceCall

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

/**
  * Implementation of the CatalogueService.
  */
class ValidationServiceImpl() extends ValidationService {

  private def getListOfFiles(dir: String, startsWith : String, endsWith : String) : List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.filter(_.getName.endsWith(endsWith)).filter(_.getName.startsWith(startsWith))
    }
    else{ List[File]() }
  }

  override  def validate() : ServiceCall[ValidationRequest, ValidationErrors] = { req =>

    val files = getListOfFiles( req.dir, req.startsWith, req.endsWith )

    def checkColumns( file : File ) : List[String] ={
      println(s"Checking file: ${file.getName}")
      val reader = new NetCdfReader( file.getPath, Set[String]() )

      var missingColumns = new ListBuffer[String]()

      req.expectedColumns.foreach(c => if(!reader.getVariableExists(c)){ missingColumns.append(c) })

      reader.close()

      missingColumns.toList
    }

    val filesCheck = files.map( f => (f.getName, checkColumns(f)) ).filterNot( f => f._2.isEmpty ).map( f => ValidationError(f._1,f._2) )

    Future.successful( ValidationErrors( filesCheck )  )
  }

}

