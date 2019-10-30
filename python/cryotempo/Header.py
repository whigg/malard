#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 12:14:12 2019

@author: jon
"""
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ET

from xml.etree.ElementTree import Element
from datetime import datetime 

def addElement( parent, tag, value = None, attr = {} ):
    elem = Element(tag)
    elem.text = value
    if len(attr) > 0:
        for k,v in attr.items():
            elem.set(k,v)
            
    parent.append(elem)
    return elem

def createHeader( attributes, gridded = False  ):    
    root = Element("Earth_Explorer_Header")
    fh = Element("Fixed_Header")
    root.append(fh)

    addElement(fh, "File_Name", attributes['File_Name'] )
    addElement(fh, "File_Description", attributes["File_Description"])
    addElement(fh, "Notes")
    addElement( fh, "Mission", "CryoSat" )
    addElement( fh, "File_Class","General Test File")
    addElement( fh, "File_Type", attributes["File_Type"])
    vp = addElement( fh, "Validity_Period" )
    addElement( vp, "Validity_Start", "UTC={}".format(attributes["Validity_Start"]) )
    addElement( vp, "Validity_Stop", "UTC={}".format(attributes["Validity_Stop"]) )
    addElement( fh, "File_Version", "0001" )
    src = addElement( fh, "Source"  )
    addElement(src,"System", "PDS" )
    addElement(src,"Creator", "TBD" )
    addElement(src,"Creator_Version", "0.1" )
    addElement(src,"Creation_Date", "UTC={}".format(datetime.now().isoformat()) )
    
    vh = addElement(root,"Variable_Header")
    mph = addElement(vh, "MPH")

    addElement( mph, "Product", attributes['File_Name'] )
    addElement( mph, "Proc_Stage_Code", "TEST")
    addElement( mph, "Ref_Doc", "http://www.cryotempo.org")
    addElement( mph, "Proc_Time", "UTC={}".format(datetime.now().isoformat()))
    addElement( mph, "Software_Version", "{}/{}".format( attributes['Creator'], attributes['Creator_Version'] ))
    addElement( mph, "Tot_size" , str(attributes["Tot_size"]), {"unit" : "bytes"} ) #Length 21 string
         
    sph = addElement( vh, "SPH"  )
    
    addElement(sph, "SPH_Descriptor", "{} SPECIFIC HEADER".format(attributes["File_Type"]) )
    ti = addElement(sph, "Time_Information")
    addElement( ti, "Start_Record_Time", "UTC={}".format(attributes["Validity_Start"]))
    addElement( ti, "Stop_Record_Time", "UTC={}".format(attributes["Validity_Stop"]))
    
    pl = addElement(sph, "Product_Location")
    addElement(pl, "Min_X", str(attributes["Min_X"]), { "unit" : "metres", "proj4" : attributes["Projection"]  } )
    addElement(pl, "Max_X", str(attributes["Max_X"]), { "unit" : "metres", "proj4" : attributes["Projection"] } )
    addElement(pl, "Min_Y", str(attributes["Min_Y"]), { "unit" : "metres", "proj4" : attributes["Projection"] } )
    addElement(pl, "Max_Y", str(attributes["Max_Y"]), { "unit" : "metres" , "proj4" : attributes["Projection"] } )
    
    #Gridded product has an interpolation window
    if gridded == True:
        addElement(sph, "Grid_Pixel_Width", str(attributes["Grid_Pixel_Width"]) , {"units": "metres"}  )
        addElement(sph, "Grid_Pixel_Height" , str(attributes["Grid_Pixel_Height"]) , {"units": "metres"}  )
        iw = addElement(sph, "Interpolation_Window")
        addElement(iw, "Window_Start", "UTC={}".format(attributes["Validity_Start"]))
        addElement(iw, "Window_End", "UTC={}".format(attributes["Validity_Stop"]))
        
    
    rough_string = ET.tostring(root, encoding="utf-8")
    reparsed = minidom.parseString(rough_string)
    pretty = reparsed.toprettyxml(indent="    ")
    return pretty

attributes = {"File_Description": "L3 Point thematic product containing swath data generated from CryoSat2 SARIN data.", "File_Type": "THEM_POINT", "File_Name" : "testing", "Validity_Start" : datetime.now(), "Validity_Stop" : datetime.now(), "Creator" : "Earthwave", "Creator_Version" : 0.1, "Tot_size" : 1000,"Projection": "proj4 code..", "Min_X":0, "Max_X":100000,"Min_Y":-200000,"Max_Y":-100000 } 

root = createHeader( attributes )

with open( "{}.HDR".format(attributes["File_Name"]), "wt", encoding="utf8"  ) as f:
    f.write(root)



     