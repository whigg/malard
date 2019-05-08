import pandas
import netCDF4

def getDataFrameFromNetCDF(filePath):
    nc = netCDF4.Dataset(filePath)
    data = []

    for v in nc.variables:
        d = nc.variables[v]
        srs = pandas.Series(d[:], name=v)
        data.append(srs)

    return pandas.DataFrame(data)