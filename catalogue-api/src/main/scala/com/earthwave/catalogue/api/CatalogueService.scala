package com.earthwave.catalogue.api

import akka.NotUsed
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

/**
  * The Catalogue service interface.
  * <p>
  * This describes everything that Lagom needs to know about how to serve and
  * consume the CatalogueService.
  */
trait CatalogueService extends Service {


  /**
    * List available parent data sets
    *
    * curl -X GET http://localhost:9000/api/parentdatasets
    */
  def parentDataSets() : ServiceCall[NotUsed, List[DataSet] ]
  /**
    * List available data sets
    *
    * curl -X GET http://localhost:9000/api/datasets/dataSetName
    */
  def dataSets(dataSetName : String) : ServiceCall[NotUsed, List[DataSetRegion] ]

  /**
    * Overall bounding box of the data
    *
    * curl -X GET http://localhost:9000/api/boundingbox/parent/:one/dataset/:two
    */
  def boundingBox( parentDsName : String, dsName : String, region : String ) : ServiceCall[NotUsed, BoundingBox ]

  /**
    * Overall bounding box of the data
    *
    * curl -X GET http://localhost:9000/api/boundingbox/parent/:one/dataset/:two
    */
  def boundingBoxQuery( parentDsName : String, dsName : String, region : String ) : ServiceCall[BoundingBoxFilter, List[BoundingBox] ]

  /**
    * Get shards  for the bounding box that is passed
    *
    * curl -X GET http://localhost:9000/api/shards/parent/:one/dataset/:two
    */

  def shards( parentDsName : String, dsName : String, region : String ) : ServiceCall[BoundingBoxFilter, List[Shard] ]

  /**
    * Get GridCells from input swathId.
    *
    * curl -X GET http://localhost:9000/api/swathdetails/:parent/:dataset
    */
  def getSwathDetails( parentDsName : String, dsName : String, region : String ) : ServiceCall[NotUsed,List[SwathDetail]]

  /**
    * Get GridCells from input swathId.
    *
    * curl -X GET http://localhost:9000/api/swathdetailsfromid/:parent/:dataset/:id
    */
  def getSwathDetailsFromId( parentDsName : String, dsName : String, region : String, id : Long ) : ServiceCall[NotUsed,SwathDetail]
  /**
    * Get GridCells from input swathId.
    *
    * curl -X GET http://localhost:9000/api/swathdetailsfromname/:parent/:dataset/:name
    */
  def getSwathDetailsFromName( parentDsName : String, dsName : String, region : String,name : String ) : ServiceCall[NotUsed,SwathDetail]

  def publishCatalogueElement( parentDsName : String, dsName : String) : ServiceCall[CatalogueElement, String]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("catalogue")
      .withCalls(
        pathCall("/api/parentdatasets", parentDataSets()),
        pathCall("/api/datasets/:parentName", dataSets _),
        pathCall("/api/boundingbox/:parent/:dsname/:region", boundingBox _ ),
        pathCall("/api/boundingbox/:parent/:dsname/:region", boundingBoxQuery _ ),
        pathCall("/api/shards/:parent/:dsname/:region", shards _ ),
        pathCall("/api/swathdetailsfromid/:parent/:dsname/:region/:id", getSwathDetailsFromId _ ),
        pathCall("/api/swathdetailsfromname/:parent/:dsname/:region/:name", getSwathDetailsFromName _ ),
        pathCall( "/api/swathdetails/:parent/:dsname/:region", getSwathDetails _),
        pathCall( "/api/publishcatalogueelement/:parent/:dsname", publishCatalogueElement _)
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

