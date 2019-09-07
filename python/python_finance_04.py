import pandas as pd
import numpy as np
import os

data_dir = 'your_data_path'

# Read firm assets from Datastream
asset = pd.read_excel(os.path.join(data_dir,'datastream_data.xlsx'),
    sheet_name='asset_1')

# Transpose data
asset1 = pd.melt(asset,id_vars='Code',value_vars=asset.columns[1:],
    value_name='asset')
asset1['isin'] = asset1['variable'].str[:12]
asset1 = asset1[['isin','Code','asset']]
asset1 = asset1.rename(columns={'Code':'year'})

# Read fiscal year end
fyear = pd.read_excel(os.path.join(data_dir,'datastream_data.xlsx'),
    sheet_name='fyend')

# Transpose data
fyear1 = pd.melt(fyear,id_vars='Code',value_vars=fyear.columns[1:],
    value_name='fyear')
fyear1['isin'] = fyear1['variable'].str[:12]
fyear1[['fmonth','_tmp']] = fyear1['fyear'].str.split('/',n=1,expand=True)
fyear1 = fyear1.rename(columns={'Code':'year'})
fyear1 = fyear1[['isin','year','fyear','fmonth']]

# Merge total assets and fiscal year end
asset2 = asset1.merge(fyear1,how='inner',on=['isin','year'])
asset2 = asset2.sort_values(['isin','year']).reset_index(drop=True)

