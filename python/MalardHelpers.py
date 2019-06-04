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

def getSwathDetailsAsDataFrame(parentDsName, dsName, query):
    swaths = query.getSwathDetails(parentDsName, dsName)
    
    jsonObj = json.loads(swaths)

    data = []

    for rec in jsonObj:
        gridCells = pd.DataFrame(rec['gridCells'])
        gridCells['swathName'] = rec['swathName']
        gridCells['pointCount'] = rec['pointCount']
        data.append(gridCells)

    return pd.concat(data, ignore_index=True)
