package com.earthwave.validationstream.impl

import com.lightbend.lagom.scaladsl.api.ServiceLocator.NoServiceLocator
import com.lightbend.lagom.scaladsl.server._
import com.lightbend.lagom.scaladsl.devmode.LagomDevModeComponents
import play.api.libs.ws.ahc.AhcWSComponents
import com.earthwave.validationstream.api.ValidationStreamService
import com.softwaremill.macwire._

class ValidationStreamLoader extends LagomApplicationLoader {

  override def load(context: LagomApplicationContext): LagomApplication =
    new ValidationStreamApplication(context) {
      override def serviceLocator: NoServiceLocator.type = NoServiceLocator
    }

  override def loadDevMode(context: LagomApplicationContext): LagomApplication =
    new ValidationStreamApplication(context) with LagomDevModeComponents

  override def describeService = Some(readDescriptor[ValidationStreamService])
}

abstract class ValidationStreamApplication(context: LagomApplicationContext)
  extends LagomApplication(context)
    with AhcWSComponents {

  // Bind the service that this server provides
  override lazy val lagomServer: LagomServer = serverFor[ValidationStreamService](wire[ValidationStreamServiceImpl])
}
