package com.earthwave.pointstream.impl

import com.earthwave.projection.api.Projection
import org.gdal.osr.CoordinateTransformation
import org.gdal.osr.SpatialReference

class CoordinateTransformCache() {
  private val wgs84 = new SpatialReference()
  wgs84.ImportFromEPSG(4326)
  private var spatialReferenceCache = List[SpatialReference]()

  def createCoordinateTransform( proj4 : String ) : CoordinateTransformation =
  {
    val spatialReference = new SpatialReference()
    spatialReference.ImportFromProj4(proj4)

    spatialReferenceCache = spatialReference :: spatialReferenceCache

    new CoordinateTransformation(wgs84, spatialReference)
  }

  private val cache = scala.collection.mutable.Map[String, CoordinateTransformation]()

  def get(projection : Projection) : CoordinateTransformation={
    return cache.getOrElse(projection.shortName, {
                                                    println(s"Creating coordinate transform ${projection.shortName}")
                                                    val coordinateTransform = createCoordinateTransform(projection.proj4)
                                                    cache.+=((projection.shortName,coordinateTransform))
                                                    coordinateTransform})
  }


  def close(): Unit = {
    cache.values.foreach(v => v.delete())
    spatialReferenceCache.foreach(v => v.delete())
  }
}
