package com.earthwave.environment.impl


import java.io.File

import akka.NotUsed
import com.earthwave.environment.api.{Environment, EnvironmentService}
import com.lightbend.lagom.scaladsl.api.ServiceCall

import scala.concurrent.Future

/**
  * Implementation of the CatalogueService.
  */
class EnvironmentServiceImpl() extends EnvironmentService {

  var env : Option[Environment] = None

  override def setEnvironment(): ServiceCall[Environment, String] = { x =>

    env = Some(x)

    def createDir( path : String) : Unit=
    {
      val dir = new File(path)
      if(!dir.exists()){
        dir.mkdirs()
      }
    }

    createDir(x.outputCdfPath)

    Future.successful( s"Environment Set [${x.name}]" )
  }

  override def getEnvironment(): ServiceCall[NotUsed, Environment] = { _ =>

    Future.successful( env.get )
  }

}
