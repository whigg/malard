package com.earthwave.validationstream.impl

import java.io.File

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.earthwave.point.impl.NetCdfReader
import com.lightbend.lagom.scaladsl.api.ServiceCall
import com.earthwave.validationstream.api.{ValidationRequest, ValidationStatus, ValidationStreamService}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

/**
  * Implementation of the ValidationStreamService.
  */
class ValidationStreamServiceImpl extends ValidationStreamService {

  implicit val ec = ExecutionContext.global

  override def validateNetCdfs = ServiceCall[ValidationRequest, Source[ValidationStatus, NotUsed]] { request =>

    Future.successful(Source.apply(getFileList(request)).mapAsync(8)( f => checkFile(f, request.expectedColumns) ) )

  }


  private def getFileList( request : ValidationRequest) : List[File] =
  {
    val d = new File(request.dir)
    val files = if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.filter(_.getName.endsWith(request.endsWith)).filter(_.getName.startsWith(request.startsWith))
    }
    else {
      List[File]()
    }
    files
  }

  private def checkFile(file : File, columns : List[String]) : Future[ValidationStatus] = {
    Future {
        println(s"Checking file: ${file.getName}")
        val reader = new NetCdfReader( file.getPath, Set[String]() )

        val missingColumns = new ListBuffer[String]()

        columns.foreach(c => if(!reader.getVariableExists(c)){ missingColumns.append(c) })

        reader.close()

        val status = if( missingColumns.isEmpty ){ ValidationStatus( file.getAbsolutePath, "Success", missingColumns.toList ) }else{ValidationStatus( file.getAbsolutePath, "Fail", missingColumns.toList )  }

        status
      }
    }
}
