import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 100)
pd.set_option('display.expand_frame_repr',False)
from datetime import datetime

import cupy as cp

year = 2012
area = 'Jak'

numBins=6
#Load Uncertainty
bins = pd.read_hdf('/data/puma/scratch/cryotempo/uncertainty/processeddata/uncertainty_{}bins_binDefs.h5'.format(numBins))
std = np.load('/data/puma/scratch/cryotempo/uncertainty/processeddata/uncertainty_{}bins_std.npy'.format(numBins))
uStd = np.load('/data/puma/scratch/cryotempo/uncertainty/processeddata/uncertainty_{}bins_uStd.npy'.format(numBins))
count = np.load('/data/puma/scratch/cryotempo/uncertainty/processeddata/uncertainty_{}bins_count.npy'.format(numBins))
mean = np.load('/data/puma/scratch/cryotempo/uncertainty/processeddata/uncertainty_{}bins_mean.npy'.format(numBins))

def binKernel( num_bins ):
    binK = cp.ElementwiseKernel(
            'float64 x, raw float64 bins',
            'float64 z',
            '''
                min_index = -9;
                for( j=0; j < num_bins - 1; ++j )
                {
                    if( bins[j] < x )
                    {
                        min_index = j;    
                    }
                }
                if( bins[num_bins -1] < x)
                {
                    min_index = -9;        
                }
                z = min_index;
            ''',
            'distance_arg',
            loop_prep='''
                         int j = 0;
                         int min_index = -9;
                         int num_bins = {};
                         '''.format(num_bins)
            )
    return binK

def getValue(row,array,binIndList):
    #Only works for 6 variables at present
    x=[]
    for i in binIndList:
        index = int(row[i])
        if index == -9:
            return np.nan
        x.append(index)
    
    return array[x[0],x[1],x[2],x[3],x[4],x[5]]
    

def applyUncertainty( df ):
    start_t = datetime.now()
    
    #Allocate the bin numbers to each row
    binCols = bins.columns
    binIndCols = []
    binCount = len(bins) # I do this and pass bincount into the lambda function to reduce count calls inside the lambda function
    
    print(bins)
    
    kernel = binKernel( binCount )
    
    #df = df[0:100000]
    
    for var in binCols:
        newColName = 'bin_{}'.format(var)
        last_t = datetime.now()
        
        df[newColName] = cp.asnumpy(kernel(cp.array(df[var],dtype=np.float64), cp.array(bins[var],dtype=np.float64) ))
        print("adding index {}: {}s".format(var,(datetime.now() - last_t).total_seconds()) )
        binIndCols.append(newColName)
    
    
    last_t = datetime.now()
    #Add the variables
    lookup_array = np.array(df[binIndCols], dtype=np.int32)
    lookup = [ -9 if np.min(r) == -9 else tuple(r) for r in lookup_array]
    
    print("Lookup {}s".format((datetime.now() - last_t).total_seconds()) )
    
    last_t = datetime.now()
    
    df['Q_uStd'] =  [ np.nan if i == -9 else uStd[i] for i in lookup]
    
    print(df["Q_uStd"].sum())
    
    print("uStd {}s".format((datetime.now() - last_t).total_seconds()) )
    
    last_t = datetime.now()
    
    df['Q_std'] = [ np.nan if i == -9 else std[i] for i in lookup]
    
    print("std {}s".format((datetime.now() - last_t).total_seconds()) )
    
    last_t = datetime.now()
    #df['Q_count'] = df.apply(lambda x: getValue(x,count,binIndCols),axis=1)
    df['Q_count'] = [ np.nan if i == -9 else count[i] for i in lookup]
    
    print("count {}s".format((datetime.now() - last_t).total_seconds()) )
    
    last_t = datetime.now()
    #df['Q_mean'] = df.apply(lambda x: getValue(x,mean,binIndCols),axis=1)
    df['Q_mean'] = [ np.nan if i == -9 else mean[i] for i in lookup]
    
    print("mean {}s".format((datetime.now() - last_t).total_seconds()) )
    end_t = datetime.now()
    
    print("Join time: {}s".format((end_t - start_t).total_seconds()) )
    
    return df

def main():
    #Load data
    loadName = 'Joined-{}-Mar-May-{}'.format(area,year)
    df = pd.read_hdf('/data/puma1/scratch/cryotempo/uncertainty/processeddata/{}_withDem.h5'.format(loadName))    
    
    df = applyUncertainty( df )
    
    df.to_hdf('/data/puma1/scratch/cryotempo/uncertainty/processeddata/{}_with_uncert_{}bins.h5'.format(loadName,numBins),'data')

if __name__ == "__main__":
    main()

