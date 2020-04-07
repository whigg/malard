package com.earthwave.pointstream.impl

import java.time.{LocalDateTime, ZoneOffset}

import com.earthwave.pointstream.impl.WriterColumn.Column
import ucar.ma2.DataType
import ucar.nc2.Variable

import scala.io.Source

import org.gdal.ogr.ogr

import org.gdal.ogr.{Driver, Layer, Geometry, ogrConstants, DataSource,FieldDefn,Feature}



object TestHarness {

  ogr.RegisterAll()

  def main(args : Array[String]):Unit ={
    val driver = ogr.GetDriverByName("ESRI Shapefile")
    val inmemDriver = ogr.GetDriverByName("MEMORY")

    val file = "/data/eagle/project-cryo-tempo/data/masks/greenland/NoPeripheryMergeReProj/GRE_Basins_IMBIE2_v1.3_NoPeriphReproj.shp"

    try {
      val source = driver.Open(file)
      val layer = source.GetLayer(0)
    }
    catch{
      case e => Exception{

        e.printStackTrace()
        print("Error..." )
      }

    }


  }

}
