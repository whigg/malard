package com.earthwave.projection.impl

import com.earthwave.environment.api.EnvironmentService
import com.earthwave.projection.api.ProjectionService
import com.lightbend.lagom.scaladsl.api.ServiceLocator
import com.lightbend.lagom.scaladsl.api.ServiceLocator.NoServiceLocator
import com.lightbend.lagom.scaladsl.persistence.cassandra.CassandraPersistenceComponents
import com.lightbend.lagom.scaladsl.server._
import com.lightbend.lagom.scaladsl.devmode.LagomDevModeComponents
import play.api.libs.ws.ahc.AhcWSComponents
import com.lightbend.lagom.scaladsl.broker.kafka.LagomKafkaComponents
import com.lightbend.lagom.scaladsl.playjson.JsonSerializerRegistry
import com.softwaremill.macwire._

class ProjectionLoader extends LagomApplicationLoader {

  override def load(context: LagomApplicationContext): LagomApplication =
    new GridCellStatsApplication(context) {
      override def serviceLocator: ServiceLocator = NoServiceLocator
    }

  override def loadDevMode(context: LagomApplicationContext): LagomApplication =
    new GridCellStatsApplication(context) with LagomDevModeComponents

  override def describeService = Some(readDescriptor[ProjectionService])
}

abstract class GridCellStatsApplication(context: LagomApplicationContext)
  extends LagomApplication(context)
    with CassandraPersistenceComponents
    with LagomKafkaComponents
    with AhcWSComponents {

  // Bind the service that this server provides
  override lazy val lagomServer: LagomServer = serverFor[ProjectionService](wire[ProjectionServiceImpl])

  // Register the JSON serializer registry
  override lazy val jsonSerializerRegistry: JsonSerializerRegistry = ProjectionSerializerRegistry

  lazy val environmentService = serviceClient.implement[EnvironmentService]

}
