import pandas as pd
import numpy as np
import os

data_dir = '/users/ml/dropbox/teaching/data/'

# Read data
uk = pd.read_csv(os.path.join(data_dir,'uk.txt'),sep='\t',low_memory=False)

# Keep common shares
uk = uk[uk['tpci']=='0']

# Keep fic = 'GBR'
uk = uk[uk['fic']=='GBR']

# Check duplicates
uk = uk.drop_duplicates(['gvkey','iid','datadate'])

# Adjusted price
uk['p_adj'] = (uk['prccd']/uk['ajexdi']) * uk['trfd']

# Security level ID
uk['stkcd'] = uk['gvkey'].astype(str) + uk['iid']

# Generate date index
date_index = uk.drop_duplicates('datadate')[['datadate']].copy()
date_index = date_index.sort_values('datadate').reset_index(drop=True)
date_index['date_idx'] = date_index.index + 1

# Calculate daily return
uk1 = uk.merge(date_index,how='inner',on='datadate')
uk1 = uk1.sort_values(['stkcd','datadate']).reset_index(drop=True)
uk1['ldate_idx'] = uk1.groupby('stkcd')['date_idx'].shift(1)
uk1['lp_adj'] = uk1.groupby('stkcd')['p_adj'].shift(1)
uk1['date_diff'] = uk1['date_idx'] - uk1['ldate_idx']
uk1['ret'] = uk1['p_adj'] / uk1['lp_adj'] - 1

uk1['date_diff'].value_counts()

uk1['ret'] = np.where(uk1['date_diff']==1,uk1['ret'],np.nan)

# ------------  Calculate monthly return  -------------------
uk_month = uk1.query('monthend==1')[['stkcd','datadate','p_adj']].copy()
uk_month['yyyymm'] = (uk_month['datadate']/100).astype(int)
uk_month['year'] = (uk_month['yyyymm']/100).astype(int)
uk_month['month'] = uk_month['yyyymm'] % 100

# Generate month index
uk_month['month_idx'] = (uk_month['year']-2017)*12 + uk_month['month'] - 9

uk_month = uk_month.sort_values(['stkcd','yyyymm']).reset_index(drop=True)
uk_month['lmonth_idx'] = uk_month.groupby('stkcd')['month_idx'].shift(1)
uk_month['lp_adj'] = uk_month.groupby('stkcd')['p_adj'].shift(1)
uk_month['month_diff'] = uk_month['month_idx'] - uk_month['lmonth_idx']
uk_month['ret'] = uk_month['p_adj'] / uk_month['lp_adj'] - 1

uk_month['month_diff'].value_counts()

uk_month['ret'] = np.where(uk_month['month_diff']==1,uk_month['ret'],np.nan)

