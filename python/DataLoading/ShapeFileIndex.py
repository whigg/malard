#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 11:02:33 2019

@author: jon
"""

import osgeo.ogr as ogr
import osgeo.osr as osr

import os
import PointProcessor as pp

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

class ShapeFileIndex:
    def __init__(self, base_path, product_type, proj4, region, pub_date ):
        # set up the shapefile driver
        driver = ogr.GetDriverByName("ESRI Shapefile")
        
        path = "{}/{}/{}/CS_OFFL_{}_index_{}_{}.shp".format(base_path, pub_date.strftime("%Y"), pub_date.strftime("%m"), product_type, region, pub_date.strftime("%Y_%m") )
        ensure_dir(path)
        
        # create the data source
        data_source = driver.CreateDataSource(path)
        
        # create the spatial reference, WGS84
        srs = osr.SpatialReference()
        srs.ImportFromProj4(proj4)
        
        # create the layer
        layer = data_source.CreateLayer("landice", srs, ogr.wkbPolygon)
        
        
        layer.CreateField(ogr.FieldDefn("pub_dat", ogr.OFTString))
        layer.CreateField(ogr.FieldDefn("min_x", ogr.OFTInteger))
        layer.CreateField(ogr.FieldDefn("max_x", ogr.OFTInteger))
        layer.CreateField(ogr.FieldDefn("min_y", ogr.OFTInteger))
        layer.CreateField(ogr.FieldDefn("max_y", ogr.OFTInteger))
        layer.CreateField(ogr.FieldDefn("units", ogr.OFTString))
        field_pdgs_path = ogr.FieldDefn("pdgs_path", ogr.OFTString)
        field_pdgs_path.SetWidth(128)
        layer.CreateField(field_pdgs_path)
        rws = ogr.FieldDefn("website", ogr.OFTString)
        rws.SetWidth(24)
        layer.CreateField(rws)
        cc =  ogr.FieldDefn("info", ogr.OFTString )
        cc.SetWidth(24)
        layer.CreateField(cc)
        
        self.layer = layer
        self.data_source = data_source
        self.pub_date = pub_date
    # Process the text file and add the attributes and features to the shapefile
    def addGridCell(self, gc, pdgs_path ):  # create the feature
      feature = ogr.Feature(self.layer.GetLayerDefn())
      # Set the attributes using the values from the delimited text file
      
      feature.SetField("website","http://www.cryotempo.org")
      feature.SetField("info", "info@earthwave.co.uk")
      feature.SetField("min_x", gc.minX )
      feature.SetField("max_x", gc.maxX )
      feature.SetField("pub_dat", self.pub_date.isoformat())  
      feature.SetField("min_y", gc.minY)
      feature.SetField("max_y", gc.maxY)
      feature.SetField("units", 'metres')  
      feature.SetField("pdgs_path", pdgs_path )
    
      # create the WKT for the feature using Python string formatting
      wkt = "POLYGON (( {} {}, {} {}, {} {}, {} {}, {} {}))".format( gc.minX,gc.minY,gc.minX,gc.maxY,gc.maxX,gc.maxY,gc.maxX,gc.minY,gc.minX,gc.minY )
    
      # Create the point from the Well Known Txt
      point = ogr.CreateGeometryFromWkt(wkt)
    
      # Set the feature geometry using the point
      feature.SetGeometry(point)
      # Create the feature in the layer (shapefile)
      self.layer.CreateFeature(feature)
      # Dereference the feature
      feature = None

    # Save and close the data source
    def close(self):
        self.layer = None
        self.data_source = None