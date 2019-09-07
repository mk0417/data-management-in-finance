import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import os

data_dir = 'your_data_path'

exec = pd.read_csv(os.path.join(data_dir,'execucomp.txt'),
    sep='\t',low_memory=False,encoding='ISO-8859-1')
exec.columns = exec.columns.str.lower()
# Rename year
exec = exec.rename(columns={'year':'fyear'})
# Check duplicates
exec = exec.drop_duplicates(['gvkey','fyear','execid'])

# Count number of executives
pct_female = exec.groupby(['gvkey','fyear','gender'])['execid'] \
    .count().unstack()
pct_female = pct_female.fillna(0)
pct_female['total'] = pct_female['FEMALE'] + pct_female['MALE']

# Count number of female CEO
female_ceo = exec[(exec['ceoann']=='CEO')&(exec['gender']=='FEMALE')] \
    .groupby(['gvkey','fyear'])['execid'].count().to_frame('n_female_ceo')
pct_female = pct_female.join(female_ceo,how='left')
pct_female = pct_female.fillna(0)
pct_female['pct_female'] = pct_female['FEMALE'] / pct_female['total']
pct_female['pct_female_ceo'] = pct_female['n_female_ceo'] / pct_female['total']

# Calculate averge percentage by year
pct_female = pct_female.groupby(level=1) \
    [['pct_female','pct_female_ceo']].mean()

# Plot
pct_female.plot(figsize=(7,3))
plt.tight_layout()
plt.savefig(os.path.join(data_dir,'pct_female_py.png'))
plt.show()

