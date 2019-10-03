import pandas as pd
import numpy as np
import os

data_dir = '~/Dropbox/teaching/data/'

# ------------------  CRSP  ----------------
# Read CRSP monthly data
crspm = pd.read_csv(os.path.join(data_dir,'crsp_raw_data.txt'),
    sep='\t',low_memory=False)
crspm.columns = crspm.columns.str.lower()

# Check data types
print(crspm.dtypes)

# Convert data type
crspm['ret'] = pd.to_numeric(crspm['ret'],errors='coerce')

# Check duplicates
crspm = crspm.drop_duplicates(['permno','date'])

# US domestic common shares
print(crspm['shrcd'].value_counts())

crspm = crspm[crspm['shrcd'].isin([10,11])]

# Stock exchanges
print(crspm['exchcd'].value_counts())

crspm = crspm[crspm['exchcd'].isin([1,2,3])]

# Count number of obsrvations
print(len(crspm))

# Negative stock prices
print(len(crspm[crspm['prc']<=0]))

crspm['price'] = crspm['prc'].abs()
crspm['price'] = np.where(crspm['price']==0,np.nan,crspm['price'])

# Non-positive market value
crspm['me'] = (crspm['price']*crspm['shrout']) / 1000
crspm['me'] = np.where(crspm['me']==0,np.nan,crspm['me'])

# Adjusted price and shares
crspm['price_adj'] = crspm['price'] / crspm['cfacpr']
crspm['shrout_adj'] = crspm['shrout'] * crspm['cfacshr']

# Save data in txt
crspm.to_csv(os.path.join(data_dir,'crsp_monthly.txt'),sep='\t',index=False)

# ------------------  Compustat  ----------------
# Import Compustat
funda = pd.read_csv(os.path.join(data_dir,'funda_raw_data.txt'),sep='\t')

# Keep unique GVKEY-DATADATE observations if you extract data from
# WRDS server without the conditions below
# funda = funda.query("consol=='C' and indfmt=='INDL' and datafmt=='STD'
#    and popsrc=='D' and curcd=='USD'")

# Multiple fiscal year ends in same calendar year
funda['year'] = (funda['datadate']/10000).astype(int)
funda['n'] = funda.groupby(['gvkey','year'])['year'].transform('count')
funda = funda[funda['n']==1]

# Save Compustat in txt */
funda.to_csv(os.path.join(data_dir,'funda.txt'),sep='\t',index=False)

# ----------------------  Know your data  ----------------------*/
crspm = pd.read_csv(os.path.join(data_dir,'crsp_monthly.txt'),
  sep='\t',low_memory=False)

# Holding period returns
crspm['logret'] = np.log(crspm['ret']+1)
crspm = crspm.sort_values(['permno','date']).reset_index(drop=True)
crspm['hpr'] = crspm.groupby('permno')['logret'] \
  .rolling(window=6,min_periods=6).sum().reset_index(drop=True)
crspm['hpr'] = np.exp(crspm['hpr']) - 1

# Summarize full sample
print(crspm['ret'].describe())

print(crspm['ret'].describe()[['mean','50%','std']])

print(crspm['ret'].describe(percentiles=[0.1,0.9]))

# Summarize by group
crspm['year'] = (crspm['date']/10000).astype(int)

print(crspm[crspm['year']>=1991].groupby('year')['ret'].describe())

print(crspm[crspm['year']>=1991].groupby('year') \
      [['ret','price']].agg(['mean','median','std']))


