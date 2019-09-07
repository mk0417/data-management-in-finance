import pandas as pd
import numpy as np
import os

data_dir = 'your_data_path'

# Read IBES data
ibes = pd.read_csv(os.path.join(data_dir,'ibes_1976_1990_summ_both.txt'),
    sep='\t',low_memory=False)
ibes.columns = ibes.columns.str.lower()

# Check if 1-year EPS forecasts: unique or value_counts
print(ibes['measure'].unique())
print(ibes['fpi'].unique())

ibes['measure'].value_counts()

ibes['fpi'].value_counts()

# Keep US only: 1=US and 0=international
ibes['usfirm'].value_counts()
ibes_us = ibes[ibes['usfirm']==1].copy()

# Keep firms with at least 60-month of numest
ibes_us['num_month'] = ibes.groupby('ticker')['numest'].transform('count')
ibes_us = ibes_us.query('num_month>=60')

# Firm-month observations
len(ibes_us)

# Number of unique firms
len(ibes_us['ticker'].unique())

# Summary statistics
ibes_us[['numest','meanest','stdev']].describe()

ibes_us['year'] = (ibes_us['statpers']/10000).astype(int)
ibes_us.groupby('year')[['numest','meanest','stdev']] \
    .agg(['mean','median','std'])

# Percentiles
pctls = ibes_us.groupby('year')['numest'] \
    .quantile([i/10 for i in range(1,10)]).unstack().reset_index()

# Correlation
ibes_us[['numest','meanest','stdev']].corr()


