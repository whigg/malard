package com.earthwave.catalogue.api

import akka.NotUsed
import com.lightbend.lagom.scaladsl.api.Service.pathCall
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

/**
  * The Catalogue service interface.
  * <p>
  * This describes everything that Lagom needs to know about how to serve and
  * consume the CatalogueService.
  */
trait CatalogueService extends Service {

  /**
    * Example: curl -H "Content-Type: application/json" -X POST -d {\"dbName\":\"mtngla\",\"year\":2010}
    * http://localhost:9000/api/gridcells
    */
  def filterCatalogue(): ServiceCall[CatalogueFilter, Catalogue]

  /**
    * List available parent data sets
    *
    * curl -X GET http://localhost:9000/api/parentdatasets
    */
  def parentDataSets() : ServiceCall[NotUsed, DataSets ]
  /**
    * List available data sets
    *
    * curl -X GET http://localhost:9000/api/datasets/dataSetName
    */
  def dataSets(dataSetName : String) : ServiceCall[NotUsed, DataSets ]

  /**
    * Overall bounding box of the data
    *
    * curl -X GET http://localhost:9000/api/boundingbox/parent/:one/dataset/:two
    */
  def boundingBox( parentDsName : String, dsName : String ) : ServiceCall[NotUsed, BoundingBox ]

  /**
    * Overall bounding box of the data
    *
    * curl -X GET http://localhost:9000/api/boundingbox/parent/:one/dataset/:two
    */
  def boundingBoxQuery( parentDsName : String, dsName : String ) : ServiceCall[BoundingBoxFilter, BoundingBoxes ]

  /**
    * Get shards  for the bounding box that is passed
    *
    * curl -X GET http://localhost:9000/api/shards/parent/:one/dataset/:two
    */

  def shards( parentDsName : String, dsName : String ) : ServiceCall[BoundingBoxFilter, Shards ]

  /**
    * Get GridCells from input swathId.
    *
    * curl -X GET http://localhost:9000/api/swathdetails/:parent/:dataset
    */
  def getSwathDetails( parentDsName : String, dsName : String ) : ServiceCall[NotUsed,List[SwathDetail]]

  /**
    * Get GridCells from input swathId.
    *
    * curl -X GET http://localhost:9000/api/swathdetailsfromid/:parent/:dataset/:id
    */
  def getSwathDetailsFromId( parentDsName : String, dsName : String, id : Long ) : ServiceCall[NotUsed,SwathDetail]
  /**
    * Get GridCells from input swathId.
    *
    * curl -X GET http://localhost:9000/api/swathdetailsfromname/:parent/:dataset/:name
    */
  def getSwathDetailsFromName( parentDsName : String, dsName : String, id : String ) : ServiceCall[NotUsed,SwathDetail]

  override final def descriptor: Descriptor = {
    import Service._
    // @formatter:off
    named("catalogue")
      .withCalls(
        pathCall("/api/gridcells", filterCatalogue()),
        pathCall("/api/parentdatasets", parentDataSets()),
        pathCall("/api/datasets/:parentName", dataSets _),
        pathCall("/api/boundingbox/:parent/:dsname", boundingBox _ ),
        pathCall("/api/boundingbox/:parent/:dsname", boundingBoxQuery _ ),
        pathCall("/api/shards/:parent/:dsname", shards _ ),
        pathCall("/api/swathdetailsfromid/:parent/:dsname/:id", getSwathDetailsFromId _ ),
        pathCall("/api/swathdetailsfromname/:parent/:dsname/:name", getSwathDetailsFromName _ ),
        pathCall( "/api/swathdetails/:parent/:dsname", getSwathDetails _)
      )
      .withAutoAcl(true)
    // @formatter:on
  }
}

