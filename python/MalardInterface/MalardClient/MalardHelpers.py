import pandas as pd
import netCDF4
import json

def getDataFrameFromNetCDF(filePath):
    nc = netCDF4.Dataset(filePath)
    data = {}

    for v in nc.variables:
        d = nc.variables[v]
        srs = pd.Series(d[:])
        data[v] = srs

    df = pd.DataFrame(data)
    nc.close()
    return df

def getSwathDetailsAsDataFrame(parentDsName, dsName, region, query):
    swaths = query.getSwathDetails(parentDsName, dsName, region)
    
    jsonObj = json.loads(swaths)

    data = []

    for rec in jsonObj:
        gridCells = pd.DataFrame(rec['gridCells'])
        gridCells['swathName'] = rec['swathName']
        gridCells['swathId'] = rec['swathId']
        gridCells['swathPointCount'] = rec['swathPointCount']
        gridCells['filteredSwathPointCount'] = rec['filteredSwathPointCount']
        data.append(gridCells)

    return pd.concat(data, ignore_index=True)
