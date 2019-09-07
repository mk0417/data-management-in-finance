import pandas as pd
import numpy as np
import os

data_dir = 'your_data_path'

asset = pd.read_excel(os.path.join(data_dir,'bloomberg_data.xlsx'),
    sheet_name='Sheet3')

asset1 = pd.melt(asset,id_vars='date',value_vars=asset.columns[1:],
    value_name='asset')

asset1['isin'] = asset1['variable'].str[:12]
asset1['asset'] = pd.to_numeric(asset1['asset'],errors='coerce')
asset1 = asset1[['isin','date','asset']] \
    .sort_values(['isin','date']).reset_index(drop=True)

