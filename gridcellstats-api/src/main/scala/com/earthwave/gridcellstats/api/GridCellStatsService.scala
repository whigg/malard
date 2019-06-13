package com.earthwave.gridcellstats.api


import akka.NotUsed
import com.earthwave.mask.api.GridCell
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

/**
  * The Mask service interface.
  * <p>
  * This describes everything that Lagom needs to know about how to serve and
  * consume the CatalogueService.
  */
trait GridCellStatsService extends Service {

  def publishGridCellStats( parentDataSet : String, runName : String ) : ServiceCall[GridCellStatistics, String]

  def getAvailableStatistics( parentDataSet : String ) : ServiceCall[NotUsed, List[String] ]

  def getGridCellStatistics( parentDataSet : String, runName : String ) : ServiceCall[GridCell,Map[String,Double]]

  def getRunStatistics( parentDataSet : String, runName : String ) : ServiceCall[NotUsed,List[GridCellStatistics]]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("gridcellstats")
      .withCalls(
        pathCall("/gridcellstats/publishgridcellstats/:parentdataset/:runName", publishGridCellStats _ ),
        pathCall("/gridcellstats/getavailablestatistics/:parentdataset", getAvailableStatistics _ ),
        pathCall("/gridcellstats/getgridcellstatistics/:parentdataset/:runName", getGridCellStatistics _),
        pathCall("/gridcellstats/getrunstatistics/:parentdataset/:runName", getRunStatistics _)
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

