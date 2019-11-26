from MalardClient.MalardClient import MalardClient
from MalardClient.DataSet import DataSet
from MalardClient.BoundingBox import BoundingBox
from datetime import datetime
from dateutil.relativedelta import relativedelta

import bucket as b

import numpy as np
import cupy as cp

client = MalardClient()

ds = DataSet("cryotempo","poca","greenland" )

dsSwath = DataSet("cryotempo","GRIS_BaselineC_Q2","greenland" )

ds_oib = DataSet("cryotempo","oib","greenland" )

bb = client.boundingBox(ds)

minX=-200000
maxX=-100000
minY=-2400000
maxY=-2300000
minT=datetime(2011,3,1,0,0,0)
maxT=datetime(2011,3,31,23,59,59)

bb = BoundingBox( minX, maxX, minY, maxY, minT, maxT )

resPoca = client.executeQuery( ds, bb )

resSwath = client.executeQuery( dsSwath, bb )

minT=minT - relativedelta(days=10)
maxT=maxT + relativedelta(days=10)

bb = BoundingBox( minX, maxX, minY, maxY, minT, maxT )

resOib =  client.executeQuery( ds_oib, bb )

df_swath = resSwath.to_df

df_oib = resOib.to_df

print(df_swath['time'].max())
print(df_oib['time'].max())

print("Nr OIB points [{}] Nr Swath Points [{}]".format(len(df_oib),len(df_swath)))

filterKernel = cp.ElementwiseKernel(
        'raw float64 x1, float64 x2, raw float64 y1, float64 y2, raw int64 t1, int64 t2',
        'float64 z',
        '''
            if( abs(t2-t1[0]) <= 864000 && abs(x2-x1[0]) <= 50 && abs(y2 -y1[0]) <=50)
            {
                z = sqrt((x1[0]-x2)*(x1[0]-x2) + (y1[0]-y2)*(y1[0]-y2));        
            }
            else
            {
                z = 999.0;
            }
        ''',
        'filter',
        )


def compute_nearest_neighbour( df_swath, df_oib, bb, numBucketsPerRow ):
    
    start_t = datetime.now()
    print("Starting Calculation {}".format(start_t))
       
    xArray = cp.empty(1)
    yArray = cp.empty(1)
    tArray = cp.empty(1,dtype=np.int64)    
    
    x_oib = cp.array(df_oib.x)
    y_oib = cp.array(df_oib.y)
    t_oib = cp.array(df_oib.time,dtype=np.int64)
  
    swath_index = []
    oib_index = []

    for i, (x,y,t) in enumerate(zip( df_swath.x, df_swath.y, df_swath.time )):    
        
        xArray.fill(x)
        yArray.fill(y)
        tArray.fill(t)
        
        filterSrs = filterKernel( xArray, x_oib, yArray, y_oib, tArray, t_oib  )
        
        closest = cp.argmin(filterSrs)
        
        if closest > 0: 
            swath_index.append(i)
            oib_index.append( closest )

    total_t = datetime.now()
    print("Took {}s".format((total_t - start_t).total_seconds()))

                
    return (swath_index, oib_index) 



numBucketsPerRow = 1
#swath, oib = compute_nearest_neighbour( df_swath, df_oib, bb, numBucketsPerRow )

print(len(swath))

client.releaseCacheHandle(resPoca.resultFileName)
client.releaseCacheHandle(resSwath.resultFileName)
client.releaseCacheHandle(resOib.resultFileName)

