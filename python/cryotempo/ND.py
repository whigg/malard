#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 09:30:25 2019

@author: jonathan
"""

import pandas as pd
import cupy as cp
import numpy as np
from datetime import datetime


def brute_force():
    for i,b in enumerate(batches):
        x_s.fill(1)
        y_s.fill(1)
        x2 = cp.multiply( oib_x, x_s )
        y2 = cp.multiply( oib_y, y_s )
        
        dist = x2 ** 2 + y2 ** 2
        mindist = cp.min( dist,axis=0 )
        arr = cp.asarray(mindist)
        

rowSize = 220
colSize = 21000

minDistKernel = cp.ElementwiseKernel(
        'float64 x, float64 y, raw float64 oib_x, raw float64 oib_y',
        'float64 z',
        '''
            for( j=0; j < num_oib; ++j )
            {
                tmp = sqrt((x - oib_x[j])*(x - oib_x[j]) + (y - oib_y[j])*(y - oib_y[j]));
                if( tmp < min_dist )
                {
                    min_dist = tmp;
                    min_index = j;
                }
            }
            z = min_index;
        ''',
        'bucket',
        loop_prep='''
                     int j = 0;
                     int min_index = -1;
                     float min_dist = 99999999;
                     float tmp = 99999999;
                     int num_oib = {};
                    '''.format(colSize)
        )




numBatches = 1000


batches = range(0, numBatches)


x_s = cp.ones( (rowSize,1) )
y_s = cp.ones( (rowSize,1) )

oib_x = cp.ones( (1, colSize) )
oib_y = cp.ones( (1, colSize) )


num_oib = cp.empty( (1, rowSize) )
num_oib.fill(rowSize)

oib_x.fill(2)
oib_y.fill(3) 

start_t = datetime.now()

#dists = minDistKernel( x_s, y_s, oib_x, oib_y  )

dists = brute_force( )

print(dists.shape)

res =cp.asnumpy(dists)

elapsed_t = ( datetime.now() - start_t ).total_seconds()



print(elapsed_t)


