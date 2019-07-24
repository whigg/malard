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

  def publishMask( envName : String, parentDataSet : String, dataSet : String, `type` : String, region : String ) : ServiceCall[MaskFile, String]

  def getMasks( envName : String, parentDataSet : String ) : ServiceCall[NotUsed,List[Mask]]

  def getGridCellMasks( envName : String, parentDataSet : String, dataSet : String, `type` : String, region : String ): ServiceCall[NotUsed,List[GridCellMask]]

  def getGridCellMask( envName : String, parentDataSet : String, dataSet : String, `type` : String, region : String ): ServiceCall[GridCell,GridCellMask]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("mask")
      .withCalls(
        pathCall("/mask/gridmasks/:envName/:parentdataset", getMasks _ ),
        pathCall("/mask/gridcells/:envName/:parentdataset/:dataset/:type/:region", getGridCellMasks _ ),
        pathCall("/mask/gridcellmask/:envName/:parentdataset/:dataset/:type/:region", getGridCellMask _),
        pathCall("/mask/publishmask/:envName/:parentdataset/:dataset/:type/:region", publishMask _)
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

