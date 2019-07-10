package com.earthwave.pointstream.impl

import com.earthwave.catalogue.api.CatalogueService
import com.earthwave.environment.api.EnvironmentService
import com.lightbend.lagom.scaladsl.api.ServiceLocator.NoServiceLocator
import com.lightbend.lagom.scaladsl.server._
import com.lightbend.lagom.scaladsl.devmode.LagomDevModeComponents
import play.api.libs.ws.ahc.AhcWSComponents
import com.earthwave.pointstream.api.PointStreamService
import com.softwaremill.macwire._

class PointStreamLoader extends LagomApplicationLoader {

  override def load(context: LagomApplicationContext): LagomApplication =
    new PointStreamApplication(context) {
      override def serviceLocator: NoServiceLocator.type = NoServiceLocator
    }

  override def loadDevMode(context: LagomApplicationContext): LagomApplication =
    new PointStreamApplication(context) with LagomDevModeComponents

  override def describeService = Some(readDescriptor[PointStreamService])
}

abstract class PointStreamApplication(context: LagomApplicationContext)
  extends LagomApplication(context)
    with AhcWSComponents {

  // Bind the service that this server provides
  override lazy val lagomServer: LagomServer = serverFor[PointStreamService](wire[PointStreamServiceImpl])

  // Bind the Catalogue and EnvironmentServices.
  lazy val catalogueService = serviceClient.implement[CatalogueService]
  lazy val environmentService = serviceClient.implement[EnvironmentService]
}
