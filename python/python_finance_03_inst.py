import pandas as pd
import numpy as np
import wrds
from matplotlib import pyplot as plt
import os

# Connect to WRDS
conn = wrds.Connection(wrds_username='lubspl12')

# Read data
inst = conn.raw_sql("""
    select *
    from tfn.s34type1
    where rdate>='01/01/2005'
""")

inst1 = inst.query('rdate==fdate').copy()

n_inst = inst1.groupby(['typecode','rdate']) \
    ['mgrno'].count().to_frame('n').reset_index()
n_inst.head()

n_inst1 = pd.pivot(n_inst,index='rdate',columns='typecode',values='n')
del n_inst1.index.name
del n_inst1.columns.name
n_inst1.columns = ['Bank','Insurance','Mutual fund','Advisor','Other']
n_inst1['total'] = n_inst1.sum(1)
n_inst1.index = pd.to_datetime(n_inst1.index,format='%Y-%m-%d')

n_inst1.plot(figsize=(8,5))
plt.tight_layout()
plt.savefig('/Users/ml/Desktop/data_management/n_inst_py.png')
plt.show()



