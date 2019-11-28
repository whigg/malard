#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 09:30:25 2019

@author: jonathan
"""

import numpy as np
import cupy as cp

from datetime import datetime

def createKernel( distance, timeinsecs, colSize ):
    minDistKernel = cp.ElementwiseKernel(
            'float64 x, float64 y, int64 t, raw float64 oib_x, raw float64 oib_y, raw int64 oib_t',
            'float64 z',
            '''
                for( j=0; j < num_oib; ++j )
                {
                    tmp = sqrt((x - oib_x[j])*(x - oib_x[j]) + (y - oib_y[j])*(y - oib_y[j]));
                    if( tmp < min_dist && tmp <= distance && abs( oib_t[j] - t) <= time )
                    {
                        min_dist = tmp;
                        min_index = j;
                    }
                }
                z = min_index;
            ''',
            'distance_arg',
            loop_prep='''
                         int j = 0;
                         int min_index = -1;
                         float min_dist = 99999999;
                         float tmp = 99999999;
                         int num_oib = {};
                         float distance = {};
                         int time ={};'''.format(colSize,distance, timeinsecs)
            )
    return minDistKernel

def compute_nearest_neighbour( df_swath, df_oib, bb, distance, timewindowinsecs ):
    
    start_t = datetime.now()
    print("Starting Calculation {}".format(start_t))
       
    xArray = cp.array(df_swath.x)
    yArray = cp.array(df_swath.y)
    tArray = cp.array(df_swath.time,dtype=np.int64)    
    
    x_oib = cp.array(df_oib.x)
    y_oib = cp.array(df_oib.y)
    t_oib = cp.array(df_oib.time,dtype=np.int64)
  
    min_dist_kernel = createKernel( distance, timewindowinsecs, len(x_oib)  )
    
    filterSrs = min_dist_kernel( xArray, yArray, tArray, x_oib, y_oib, t_oib  )
       
    oib_indices = cp.asnumpy(filterSrs)
    total_t = datetime.now()
    print("Took {}s".format((total_t - start_t).total_seconds()))

    return oib_indices
