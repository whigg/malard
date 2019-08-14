package com.earthwave.pointstream.impl

import java.io.File

object FileHelper {

  def getListOfFiles(dir: String, startsWith : String, endsWith : String) : List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.filter(_.getName.endsWith(endsWith)).filter(_.getName.startsWith(startsWith))
    }
    else{ List[File]() }
  }
  def parseField(f: String): java.lang.Double = f match {

    case x: String if x.isEmpty => null;
    case y: String => y.toDouble;
  }

  def parseLine( line : String ) : Vector[java.lang.Double] = {
    line.split(",").map( x => parseField(x) ).toVector
  }

  def createDir( path : String) : Unit=
  {
    val dir = new File(path)
    if(!dir.exists()){
      dir.mkdirs()
    }
  }
}
