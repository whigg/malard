from MalardClient.MalardClient import MalardClient
from MalardClient.DataSet import DataSet
from MalardClient.BoundingBox import BoundingBox
from datetime import datetime
from dateutil.relativedelta import relativedelta

import math
import pandas as pd
import ND as nd

def distance( x1s, y1s, x2s, y2s ):
    
    return [math.sqrt( (x1 -x2 )**2 + (y1-y2)**2 ) for x1,y1,x2,y2 in zip( x1s, y1s, x2s, y2s )]
        
client = MalardClient()

ds = DataSet("cryotempo","poca","greenland" )

dsSwath = DataSet("cryotempo","GRIS_BaselineC_Q2","greenland" )

ds_oib = DataSet("cryotempo","oib","greenland" )

filters = [{'column':'coh','op':'gte','threshold':0.5},{'column':'power','op':'gte','threshold':1000.0}]

projections = ['swathFileId','x','y','time','WGS84_Ellipsoid_Height(m)']

projections_swath = ['swathFileId','x','y','time','coh','power','elev','demDiff','demDiffMad'] 
    

years = [2011,2012,2013,2014,2015,2016]

dfs = []
start_t = datetime.now()

total_match = 0 

for y in years:
    minT=datetime(y,3,1,0,0,0)
    maxT=datetime(y,6,30,23,59,59)
    
    
    #minX=-200000
    #maxX=-100000
    #minY=-2400000
    #maxY=-2300000
    
    #bb = BoundingBox( minX, maxX, minY, maxY, minT, maxT )
    bb = client.boundingBox(ds_oib)
    
    bb = BoundingBox( bb.minX, bb.maxX, bb.minY, bb.maxY, minT, maxT )
    
    gcs = client.gridCells( ds_oib, bb )
    nr_gcs = len(gcs)
    
    print("Nr of grid cells to process: {}".format(nr_gcs))
        
    for i, gc in enumerate(gcs):
        
        bb = BoundingBox( gc.minX, gc.maxX, gc.minY, gc.maxY, minT, maxT )
        
        resSwath = client.executeQuery( dsSwath, bb, filters=filters, projections = projections_swath )
    
        o_minT=minT - relativedelta(days=10)
        o_maxT=maxT + relativedelta(days=10)
        
        bb = BoundingBox( gc.minX, gc.maxX, gc.minY, gc.maxY, o_minT, o_maxT )
        
        resOib =  client.executeQuery( ds_oib, gc, projections=projections )
        
        if resSwath.status == "Success" and resOib.status == "Success": 
        
            df_swath = resSwath.to_df
            df_oib = resOib.to_df
        
            print("Nr OIB points [{}] Nr Swath Points [{}]".format(len(df_oib),len(df_swath)))
        
            numBucketsPerRow = 1
            oib_indices = nd.compute_nearest_neighbour( df_swath, df_oib, bb, 50.05, 864000.0 )
            
            df_swath['oib_index'] = oib_indices
            oib_index = range(0,len(df_oib))
            df_oib['oib_index'] = oib_index
            swath_index = range(0,len(df_swath))
            df_swath['swath_index'] = swath_index 
            
            alljoined = pd.merge( df_swath, df_oib, how='inner', on='oib_index', suffixes=("_swath","_oib") )
            dists = distance( alljoined.x_swath, alljoined.y_swath, alljoined.x_oib, alljoined.y_oib )
            alljoined['distance'] = dists
            
            dfs.append( alljoined  )
            total_match = total_match + len(alljoined)
            print( "GC {} Number of points under 50m {}. Total Matched {}".format(  i, len(alljoined), total_match ))  
            
        else:
            print("Skipping {} Message {}".format(i,resSwath.message))
            
        client.releaseCacheHandle(resSwath.resultFileName)
        client.releaseCacheHandle(resOib.resultFileName)
    
end_t =datetime.now()

#saving the output file
df = pd.concat( dfs, ignore_index=True )

fname = '/home/jon/data/spatialjoin.h5'
storeOutput = pd.HDFStore(fname,mode='w',complevel=9, complib='blosc')

storeOutput['SwathOib50m'] = df

storeOutput.close() 


print( "Total processing time {} Total Match {}".format((end_t - start_t).total_seconds(), total_match) )