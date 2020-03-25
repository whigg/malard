import MalardClient.MalardClient as c

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import sys

#sys.path.append('/data/puma/scratch/cryotempo/analysis')

#import waveform_adjustment as wa

from datetime import datetime

jakobshavn = "/data/puma1/scratch/cryotempo/notebook/shpfiles/jakobshaven_NPS.shp"
petermann = "/data/puma1/scratch/cryotempo/notebook/shpfiles/petermann_NPS.shp"

greenland = "/data/puma1/scratch/cryotempo/masks/icesheets.shp"
#Filters
minCoh = 0.5
minPowerdB = -160
maxDemDiff = 100
#maxDemDiffMad = 20
minPower = 100

#years=[2011,2012,2013,2014,2015,2016]

def applyDemDiffMad( df, demDiffMadNew ):
    tempDf = pd.DataFrame(df)
    
    print("Start rows {}".format(len(df)))
    
    print("Initial nr of unique wf nrs {}".format( len(df["wfUnique"].unique()) ))
    
    #tempDf = df[df['coh'] >= 0.8]
    #tempDf = tempDf[tempDf['powerScaled'] >= 10000]
    
    print("After coherence and power filters {}".format(len(tempDf)))
    print("After Coh and Pwr filters nr of unique wf nrs {}".format( len(tempDf["wfUnique"].unique()) ))
    
    #create new demDiffMad
    tempDf['demDiffMadNew'] = tempDf['demDiff'].groupby(tempDf['wfUnique']).transform('mad')
    print("Max DemDiff Mad [{}]".format(tempDf['demDiffMadNew'].max()))
           
    newDf = pd.DataFrame(tempDf['wfUnique']) 
    newDf['wfUnique'] = tempDf['wfUnique']
    newDf['demDiffMadNew'] = tempDf['demDiffMadNew']
    
    newDf = newDf.groupby(by="wfUnique").min().reset_index()
    
    res_df = pd.merge(df, newDf, how="inner", on="wfUnique", suffixes=('','_wf'))
    
    print("After demDiffMerge number of wf_numbers {}".format( len(res_df["wfUnique"].unique()) ))

    print("Max DemDiff Mad [{}]".format(res_df['demDiffMadNew'].max()))
            
    print("Number of rows before filter {}".format(len(df)))
    res_df = res_df[res_df['demDiffMadNew'] <= demDiffMadNew ]
    print("Number of rows after filter {}".format(len(res_df)))
    res_df = res_df.reset_index()
    
    print("Max demDiffMadNew after filter has been applied: {}".format(res_df["demDiffMadNew"].max()))
    
    return res_df

def loadAndJoinToPoca(  dsSwath, dsPoca, filters, columns_swath, malardEnv, extentFilter = None, minT = None, maxT = None, bb = None, bb_offset=None, newDemDiffMad = None, adjustWaveform=True):

    client = c.MalardClient(environmentName=malardEnv)
    filter_mask = []#[{"column":"inRegionMask","op":"eq","threshold":1}]
         
    swathResult = None
    if bb is None:
        swathResult = client.executeQueryPolygon(dsSwath, minT, maxT, extentFilter,filters=filters, projections=columns_swath)
    else:
        swathResult = client.executeQuery(dsSwath, bb_offset, filters = filters, projections=columns_swath )
    
    columns_poca= ['wf_number','x','y','time','elev']
    
    if bb is None:
        pocaResult = client.executeQueryPolygon(dsPoca, minT, maxT, extentFilter, projections=columns_poca)
    else:
        pocaResult = client.executeQuery(dsPoca, bb_offset, filters=filter_mask, projections=columns_poca )
    
    joined = pd.DataFrame()
    status = swathResult.status == "Success" and pocaResult.status == "Success" and swathResult.message.startswith("Error") == False and pocaResult.message.startswith("Error") == False
    
    print("Swath Status {} message {}".format( swathResult.status, swathResult.message ) )
    print("Poca Status {} message {}".format( pocaResult.status, pocaResult.message ) )
    
    if status:     
        swath = swathResult.to_df
        poca = pocaResult.to_df
        if len(swath) == 0 or len(poca) == 0:
            print( "Swath length=[{}] Poca Length=[{}]".format(len(swath), len(poca)) )        
            return ( joined, status )
            
        print("Swath length is: {}".format(len(swath))) 
        
        print( "Poca status {}".format(pocaResult.message))
        print("Poca length is: {}".format(len(poca)))
        
        client.releaseCacheHandle(pocaResult.resultFileName)
        client.releaseCacheHandle(swathResult.resultFileName)
        
        swath['wfUnique']=swath['time']*100000+swath['wf_number']
        
        #if dsSwath.dataSet.endswith("_d"):
        #    
        #    powerdB = swath["powerdB"]
        #    powerdBEsa = 10*np.log10(1e9*np.power(10,powerdB/10))
            
        #    swath["powerdB"] = powerdBEsa
            
        #    print("Nr rows before power filter {}".format(len(swath)))
        #    swath = swath[swath["powerdB"] > -160.0 ]
        #    print("Nr rows after power filter {}".format(len(swath)))
        
        if newDemDiffMad is not None: 
            swath = applyDemDiffMad( swath, newDemDiffMad )
           
        #Join Data
        joined = pd.merge(swath,poca,left_on=['time','wf_number'],right_on=['time','wf_number'],how='inner', suffixes=('','_poca'))
        #joined.drop(['startTime'],axis=1,inplace=True)
        
        #Get Unique list of WF numbers
        joined['distToPoca'] = np.sqrt( (joined['x'] - joined['x_poca'] )**2 + (joined['y'] - joined['y_poca'])**2 )
    
    
        
        if adjustWaveform and len(joined) > 0:
            print( "Applying waveform adjustment." )
            #joined = wa.adjust_whole_df( joined )
            raise ValueError("AdjustWaveform No Longer supported.")
        else:
            print("Skipping waveform adjustment.")
    
        ratio = 0 if len(swath) == 0 else len(joined)/len(swath)
        print("Joined length is: {}, {:.2%} of original".format(len(joined),ratio))
        print("Unique WFs: {}".format(len(joined['wfUnique'].unique()))) 
        
        del swath
        del poca
    else:
        print( "Swath Message {} Poca Message: {}.".format(swathResult.message, pocaResult.message ) )
        
    return ( joined, status )
    
def main( dsSwath, dsPoca, extentFilter, areaLabel, filters ):
    
    for year in years:    
        print("start {}".format(year))
        
        minT = datetime(year,3,1,0,0,0)
        #maxT = datetime(2011,5,31,23,59,59)
        maxT = datetime(year,5,31,23,59,59)
        
        joined, status = loadAndJoinToPoca(  dsSwath, dsPoca, filters, extentFilter, minT, maxT)
        
        #Save 
        saveName = 'Joined-{}-Mar-May-{}'.format(areaLabel,year)
        joined.to_hdf('/data/puma1/scratch/cryotempo/uncertainty/processeddata/{}.h5'.format(saveName),'data') 


if __name__ == "__main__":
    #Area define:
    years=[2011]    
    shpFile = jakobshavn
    extentFilter = c.MaskFilter( p_shapeFile=shpFile )
    areaLabel = 'jak'  # or 'jak', 'str'
    #Load Swath
    dsSwath = c.DataSet("cryotempo","swath_c","greenland")
    #Load Poca
    dsPoca = c.DataSet("cryotempo","poca_g","greenland")
    
    filters = [{"column":"power","op":"gte","threshold":minPower},{"column":"powerdB","op":"gte","threshold":minPowerdB},{"column":"coh","op":"gte","threshold":minCoh},{"column":"demDiffMad","op":"lt","threshold":maxDemDiffMad},{"column":"demDiff","op":"lte","threshold":maxDemDiff},{"column":"demDiff","op":"gte","threshold":-maxDemDiff}]   #demDiff<100, demDiff>-100, 
    #filters = [{"column":"inRegionMask","op":"eq","threshold":1}.{"column":"powerScaled","op":"gte","threshold":minPower},{"column":"powerdB","op":"gte","threshold":minPowerdB},{"column":"coh","op":"gte","threshold":minCoh},{"column":"demDiffMad","op":"lt","threshold":maxDemDiffMad},{"column":"demDiff","op":"lte","threshold":maxDemDiff},{"column":"demDiff","op":"gte","threshold":-maxDemDiff}]   #demDiff<100, demDiff>-100, 
   
       
    main( dsSwath, dsPoca, extentFilter, areaLabel, filters)
    
#Temp Petermann post cutdown
#abc = joined[(joined['lat']>79.7) & (joined['lat']<81.3) & (joined['lon']>296.5) & (joined['lon']<302.8)]    
#abc.to_hdf('/data/puma1/scratch/cryotempo/uncertainty/processeddata/{}.h5'.format(saveName),'data')    