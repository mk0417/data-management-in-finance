import pandas as pd
import numpy as np
import wrds
import os
import warnings

warnings.filterwarnings('ignore',category=FutureWarning)

wd = '/users/ml/dropbox/teaching/data/'
os.chdir(wd)


# ------------------------------------
#    Construct PERMNO-GVKEY link
# -------------------------------------
msenames = pd.read_csv('msenames.txt',sep='\t',
    usecols=['permno','cusip','ncusip','comnam','namedt','nameendt'])
msenames.columns = msenames.columns.str.lower()
msenames = msenames[msenames['ncusip'].notna()]
msenames['permno'] = msenames['permno'].astype(int)
msenames['namedt'] = pd.to_datetime(msenames['namedt'],format='%Y-%m-%d')
msenames['nameendt'] = pd.to_datetime(msenames['nameendt'],format='%Y-%m-%d')
msenames['namedt'] = msenames['namedt'].dt.year*10000 \
    + msenames['namedt'].dt.month*100 + msenames['namedt'].dt.day
msenames['nameendt'] = msenames['nameendt'].dt.year*10000 \
    + msenames['nameendt'].dt.month*100 + msenames['nameendt'].dt.day

security = pd.read_csv('security.txt',sep='\t',usecols=['gvkey','iid','cusip'])
security['cusip'] = security['cusip'].str[:8]
security = security[~security['iid'].str.contains('C')]
security = security.rename(columns={'cusip':'cusip_comp'})

permno_gvkey = msenames.merge(security,
    how='left',left_on='ncusip',right_on='cusip_comp')
permno_gvkey = permno_gvkey.sort_values(['permno','namedt'])
permno_gvkey['gvkey'] = permno_gvkey.groupby('permno')['gvkey'] \
    .fillna(method='bfill')
permno_gvkey = permno_gvkey[permno_gvkey['gvkey'].notna()]
permno_gvkey['begdate'] = permno_gvkey.groupby(['permno','gvkey']) \
    ['namedt'].transform('min')
permno_gvkey['enddate'] = permno_gvkey.groupby(['permno','gvkey']) \
    ['nameendt'].transform('max')
permno_gvkey = permno_gvkey.drop_duplicates(['permno','gvkey'])
permno_gvkey = permno_gvkey[['permno','gvkey','begdate','enddate']]
permno_gvkey = permno_gvkey.sort_values(['permno','gvkey']) \
    .reset_index(drop=True)
len(permno_gvkey)


# -------------------------------
#    Data from WRDS website
# -------------------------------
msf = pd.read_csv('merge_crsp.txt',sep='\t')
msf.columns = msf.columns.str.lower()
msf = msf.query('10<=shrcd<=11 and 1<=exchcd<=3')
msf = msf.drop_duplicates(['permno','date'])
msf['yyyymm'] = (msf['date']/100).astype(int)
msf['me'] = msf['prc'].abs() * msf['shrout'] / 1000
msf = msf[msf['me']>0]
len(msf)

funda = pd.read_csv('merge_compustat.txt',sep='\t')
funda = funda.query("indfmt=='INDL' and consol=='C' and popsrc=='D' \
    and datafmt=='STD' and seq>0")
funda['cusip'] = funda['cusip'].str[:8]
funda = funda.sort_values(['gvkey','fyear','datadate']).reset_index(drop=True)
funda = funda.drop_duplicates(['gvkey','fyear'],keep='last')
funda['yyyymm'] = (funda['datadate']/100).astype(int)
len(funda)

merged = msf[['permno','yyyymm','date','me']] \
    .merge(permno_gvkey[['permno','gvkey','begdate','enddate']],
    how='inner',on='permno')
len(merged)

merged = merged.query('begdate<=date<=enddate')
len(merged)

merged = merged.drop_duplicates(['permno','yyyymm'])
len(merged)

merged= merged.merge(funda[['gvkey','datadate','yyyymm','seq']],
    how='inner',on=['gvkey','yyyymm'])
len(merged)

merged = merged.drop_duplicates(['permno','yyyymm'])
len(merged)

merged = merged.sort_values(['permno','yyyymm']).reset_index(drop=True)


# ------------------------------------------------------
#   Connect to WRDS server without downloading data
# -------------------------------------------------------
conn = wrds.Connection(wrds_username='lubspl12')

permno_gvkey = conn.raw_sql("""
    select cast(a.permno as int),b.gvkey,a.namedt,a.nameendt
    from crsp.msenames a left join
        (select gvkey,cusip
        from comp.security
        where right(iid,1)!='C') b
    on a.ncusip=substr(b.cusip,1,8)
    where a.ncusip!=''
    order by permno,namedt
""",date_cols=['namedt','nameendt'])

permno_gvkey['gvkey'] = permno_gvkey.groupby('permno') \
    ['gvkey'].fillna(method='bfill')
permno_gvkey = permno_gvkey[permno_gvkey['gvkey'].notna()]
permno_gvkey['begdate'] = permno_gvkey.groupby(['permno','gvkey']) \
    ['namedt'].transform('min')
permno_gvkey['enddate'] = permno_gvkey.groupby(['permno','gvkey']) \
    ['nameendt'].transform('max')
permno_gvkey = permno_gvkey.drop_duplicates(['permno','gvkey'])
permno_gvkey = permno_gvkey[['permno','gvkey','begdate','enddate']]
permno_gvkey = permno_gvkey.sort_values(['permno','gvkey']) \
    .reset_index(drop=True)
len(permno_gvkey)

msf = conn.raw_sql("""
    select a.*
    from
    (select cast(a.permno as int),a.date,abs(a.prc)*a.shrout/1000 as me
    from crsp.msf a left join msenames b
    on a.permno=b.permno and a.date>=b.namedt and a.date<=b.nameendt
    where b.shrcd between 10 and 11
        and b.exchcd between 1 and 3
        and a.date between '01/01/1975' and '12/31/2017'
    ) a
    where me>0
""",date_cols=['date'])

len(msf)

msf = msf.drop_duplicates(['permno','date'])
msf['yyyymm'] = msf['date'].dt.year*100 + msf['date'].dt.month

len(msf)

funda = conn.raw_sql("""
    select gvkey,datadate,fyear,seq
    from comp.funda
    where indfmt='INDL' and consol='C' and popsrc='D'
        and datafmt='STD' and seq>0
        and datadate>='01/01/1975' and datadate<='12/31/2017'
""",date_cols=['datadate'])

funda['yyyymm'] = funda['datadate'].dt.year*100 + funda['datadate'].dt.month
funda = funda.sort_values(['gvkey','fyear','datadate'])
funda = funda.drop_duplicates(['gvkey','fyear'],keep='last')

len(funda)

merged1 = msf.merge(permno_gvkey,how='inner',on='permno')
merged1 = merged1.query('begdate<=date<=enddate')
len(merged1)

merged1 = merged1.merge(funda,how='inner',on=['gvkey','yyyymm'])
len(merged1)

merged1 = merged1.drop_duplicates(['permno','yyyymm'])



# ------------------------------------------------------
#   Comparison: generate PERMNO-GVKEY link by CUSIP
#   Example: permno=10051
# -------------------------------------------------------
permno_gvkey_by_cusip = msenames.merge(security,
    how='inner',left_on='cusip',right_on='cusip_comp')
permno_gvkey_by_cusip = permno_gvkey_by_cusip \
    .drop_duplicates(['permno','gvkey'])
permno_gvkey_by_cusip = permno_gvkey_by_cusip[['permno','gvkey','cusip']]
len(permno_gvkey_by_cusip)

permno_gvkey_by_cusip = permno_gvkey_by_cusip.add_suffix('_by_cusip')

comparison = permno_gvkey.merge(permno_gvkey_by_cusip,how='outer',
    left_on=['permno','gvkey'],right_on=['permno_by_cusip','gvkey_by_cusip'])
len(comparison)

miss_by_cusip = comparison[comparison['gvkey_by_cusip'].isna()]

miss_by_cusip.head()

msenames.query('permno==10051')

security.query('gvkey==13445')

security.query("cusip_comp=='41043F20'")

permno_gvkey.query('permno==10051')

funda.query('gvkey==13345')

funda.query('gvkey==16456')

msf.query('permno==10051')



# ------------------------------------------------------
#   Comparison: merge CRSP and Compustat  by CUSIP
#   Do not use PERMNO-GVKEY link table
#   Example: permno=10051
# -------------------------------------------------------

merged2 = msf.merge(funda[['gvkey','cusip','datadate','yyyymm','seq']],
    how='inner',on=['cusip','yyyymm'])
len(merged2)

merged2 = merged2.drop_duplicates(['permno','yyyymm'])
len(merged2)

merged2.query('permno==10051')
