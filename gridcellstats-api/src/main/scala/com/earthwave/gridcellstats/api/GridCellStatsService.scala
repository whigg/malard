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

  def publishGridCellStats(envName : String,  parentDataSet : String, runName : String  ) : ServiceCall[GridCellStatistics, String]

  def getAvailableStatistics( envName : String, parentDataSet : String ) : ServiceCall[NotUsed, List[String] ]

  def getGridCellStatistics( envName : String, parentDataSet : String, runName : String) : ServiceCall[GridCell,Map[String,Double]]

  def getRunStatistics( envName : String, parentDataSet : String, runName : String ) : ServiceCall[NotUsed,List[GridCellStatistics]]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("gridcellstats")
      .withCalls(
        pathCall("/gridcellstats/publishgridcellstats/:envName/:parentdataset/:runName", publishGridCellStats _ ),
        pathCall("/gridcellstats/getavailablestatistics/:envName/:parentdataset", getAvailableStatistics _ ),
        pathCall("/gridcellstats/getgridcellstatistics/:envName/:parentdataset/:runName", getGridCellStatistics _),
        pathCall("/gridcellstats/getrunstatistics/:envName/:parentdataset/:runName", getRunStatistics _)
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

