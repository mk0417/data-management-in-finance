import pandas as pd
import numpy as np
import wrds
import os
import warnings

warnings.filterwarnings('ignore',category=FutureWarning)

wd = ''
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
permno_gvkey['gvkey'] = permno_gvkey.groupby('permno') \
    ['gvkey'].fillna(method='bfill')
permno_gvkey = permno_gvkey[permno_gvkey['gvkey'].notna()]
permno_gvkey['begdate'] = permno_gvkey.groupby(['permno','gvkey']) \
    ['namedt'].transform('min')
permno_gvkey['enddate'] = permno_gvkey.groupby(['permno','gvkey']) \
    ['nameendt'].transform('max')
permno_gvkey = permno_gvkey.drop_duplicates(['permno','gvkey'])
permno_gvkey = permno_gvkey[['permno','gvkey','begdate','enddate']]


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

funda = pd.read_csv('merge_compustat.txt',sep='\t')
funda = funda.query("indfmt=='INDL' and consol=='C' and popsrc=='D' \
    and datafmt=='STD' and seq>0")
funda = funda.sort_values(['gvkey','fyear','datadate']).reset_index(drop=True)
funda = funda.drop_duplicates(['gvkey','fyear'],keep='last')
funda['yyyymm'] = (funda['datadate']/100).astype(int)

merged = msf[['permno','yyyymm','date','me']] \
    .merge(permno_gvkey[['permno','gvkey','begdate','enddate']],
    how='inner',on='permno')
merged = merged.query('begdate<=date<=enddate')
merged = merged.drop_duplicates(['permno','yyyymm'])
merged= merged.merge(funda[['gvkey','datadate','yyyymm','seq']],
    how='inner',on=['gvkey','yyyymm'])
merged = merged.drop_duplicates(['permno','yyyymm'])
merged = merged.sort_values(['permno','yyyymm']).reset_index(drop=True)



# ------------------------------------------------------
#   Connect to WRDS server without downloading data
# -------------------------------------------------------
conn = wrds.Connection()

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

msf = msf.drop_duplicates(['permno','date'])
msf['yyyymm'] = msf['date'].dt.year*100 + msf['date'].dt.month

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

merged1 = msf.merge(permno_gvkey,how='inner',on='permno')
merged1 = merged1.query('begdate<=date<=enddate')
merged1 = merged1.merge(funda,how='inner',on=['gvkey','yyyymm'])
merged1 = merged1.drop_duplicates(['permno','yyyymm'])

