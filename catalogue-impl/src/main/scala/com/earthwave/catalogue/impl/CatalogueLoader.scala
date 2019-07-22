package com.earthwave.catalogue.impl

import com.lightbend.lagom.scaladsl.api.ServiceLocator
import com.lightbend.lagom.scaladsl.api.ServiceLocator.NoServiceLocator
import com.lightbend.lagom.scaladsl.persistence.cassandra.CassandraPersistenceComponents
import com.lightbend.lagom.scaladsl.server._
import com.lightbend.lagom.scaladsl.devmode.LagomDevModeComponents
import play.api.libs.ws.ahc.AhcWSComponents
import com.earthwave.catalogue.api.CatalogueService
import com.earthwave.environment.api.EnvironmentService
import com.lightbend.lagom.scaladsl.broker.kafka.LagomKafkaComponents
import com.lightbend.lagom.scaladsl.playjson.JsonSerializerRegistry
import com.softwaremill.macwire._

class CatalogueLoader extends LagomApplicationLoader {

  override def load(context: LagomApplicationContext): LagomApplication =
    new CatalogueApplication(context) {
      override def serviceLocator: ServiceLocator = NoServiceLocator
    }

  override def loadDevMode(context: LagomApplicationContext): LagomApplication =
    new CatalogueApplication(context) with LagomDevModeComponents

  override def describeService = Some(readDescriptor[CatalogueService])
}

abstract class CatalogueApplication(context: LagomApplicationContext)
  extends LagomApplication(context)
    with CassandraPersistenceComponents
    with LagomKafkaComponents
    with AhcWSComponents {

  // Bind the service that this server provides
  override lazy val lagomServer: LagomServer = serverFor[CatalogueService](wire[CatalogueServiceImpl])

  lazy val environmentService = serviceClient.implement[EnvironmentService]
  // Register the JSON serializer registry
  override lazy val jsonSerializerRegistry: JsonSerializerRegistry = CatalogueSerializerRegistry


}
