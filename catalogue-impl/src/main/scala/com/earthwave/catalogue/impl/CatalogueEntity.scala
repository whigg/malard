package com.earthwave.catalogue.impl

import com.earthwave.catalogue.api._
import com.lightbend.lagom.scaladsl.playjson.{JsonSerializer, JsonSerializerRegistry}

import scala.collection.immutable.Seq


/**
  * Akka serialization, used by both persistence and remoting, needs to have
  * serializers registered for every type serialized or deserialized. While it's
  * possible to use any serializer you want for Akka messages, out of the box
  * Lagom provides support for JSON, via this registry abstraction.
  *
  * The serializers are registered here, and then provided to Lagom in the
  * application loader.
  */
object CatalogueSerializerRegistry extends JsonSerializerRegistry {
  override def serializers: Seq[JsonSerializer[_]] = Seq(
    JsonSerializer[DataSet],
    JsonSerializer[Catalogue],
    JsonSerializer[CatalogueElement],
    JsonSerializer[BoundingBox],
    JsonSerializer[Shard],
  )
}
