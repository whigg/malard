package com.earthwave.mask.api

import akka.NotUsed
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

/**
  * The Mask service interface.
  * <p>
  * This describes everything that Lagom needs to know about how to serve and
  * consume the CatalogueService.
  */
trait MaskService extends Service {

  def publishMask( parentDataSet : String, `type` : String, region : String ) : ServiceCall[MaskFile, String]

  def getMasks( parentDataSet : String ) : ServiceCall[NotUsed,List[Mask]]

  def getGridCellMasks( parentDataSet : String, `type` : String, region : String ): ServiceCall[NotUsed,List[GridCellMask]]

  def getGridCellMask( parentDataSet : String, `type` : String, region : String ): ServiceCall[GridCell,GridCellMask]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("mask")
      .withCalls(
        pathCall("/mask/gridmasks/:parentdataset", getMasks _ ),
        pathCall("/mask/gridcells/:parentdataset/:type/:region", getGridCellMasks _ ),
        pathCall("/mask/gridcellmask/:parentdataset/:type/:region", getGridCellMask _),
        pathCall("/mask/publishmask/:parentdataset/:type/:region", publishMask _)
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

