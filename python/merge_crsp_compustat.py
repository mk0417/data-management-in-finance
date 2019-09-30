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
    usecols=['permno','cusip','ncusip','comnam'])
msenames.columns = msenames.columns.str.lower()
msenames = msenames[msenames['ncusip'].notna()]
msenames['permno'] = msenames['permno'].astype(int)

security = pd.read_csv('security.txt',sep='\t',usecols=['gvkey','iid','cusip'])
security['cusip'] = security['cusip'].str[:8]
security = security.rename(columns={'cusip':'cusip_comp'})

permno_gvkey = msenames.merge(security,
    how='inner',left_on='ncusip',right_on='cusip_comp')
permno_gvkey = permno_gvkey.drop_duplicates(['permno','gvkey'])
len(permno_gvkey)


# -------------------------------
#    Data from WRDS website
# -------------------------------

msf = pd.read_csv('merge_crsp_2013_2017.txt',sep='\t')
msf.columns = msf.columns.str.lower()
msf = msf.query('10<=shrcd<=11 and 1<=exchcd<=3')
msf = msf.drop_duplicates(['permno','date'])
msf['yyyymm'] = (msf['date']/100).astype(int)
msf['me'] = msf['prc'].abs() * msf['shrout'] / 1000
msf = msf[msf['me']>0]

funda = pd.read_csv('merge_compustat.txt',sep='\t')
funda = funda.query("indfmt=='INDL' and consol=='C' and popsrc=='D' \
    and datafmt=='STD' and seq>0")
funda = funda.sort_values(['gvkey','datadate']).reset_index(drop=True)
funda = funda.drop_duplicates(['gvkey','fyear'],keep='last')
funda['yyyymm'] = (funda['datadate']/100).astype(int)

merged = msf[['permno','yyyymm','date','me']] \
    .merge(permno_gvkey[['permno','gvkey']],how='inner',on='permno') \
    .merge(funda[['gvkey','datadate','yyyymm','seq']],
    how='inner',on=['gvkey','yyyymm'])
merged = merged.sort_values(['permno','yyyymm','me','seq'])
merged = merged.drop_duplicates(['permno','yyyymm'])
merged = merged.sort_values(['permno','yyyymm']).reset_index(drop=True)


# ------------------------------------------------------
#   Connect to WRDS server without downloading data
# -------------------------------------------------------
conn = wrds.Connection()

merged1 = conn.raw_sql("""
    select t3.*, t4.datadate, t4.seq, t4.fyear
    from
    (select t1.*, t2.gvkey
    from
    (select cast(a.permno as int), a.date,
        abs(a.prc)*a.shrout/1000 as me, b.ncusip,
        cast(date_part('year',cast(date as date)) as int) as year,
        cast(date_part('month',cast(date as date)) as int) as month
    from crsp.msf a inner join crsp.msenames b
    on a.permno = b.permno and a.date>=b.namedt and a.date<=b.nameendt
    where b.exchcd between 1 and 3 and b.shrcd between 10 and 11
        and a.date>='01/01/2013' and a.date<='12/31/2017') t1
    inner join
    (select distinct cast(c.permno as int), d.gvkey
    from crsp.msenames c inner join comp.security d
    on c.ncusip=substr(d.cusip,1,8)
    where c.ncusip!='') t2
    on (t1.permno=t2.permno)
    ) t3
    inner join
    (select gvkey, datadate, seq, fyear,
        cast(date_part('year',cast(datadate as date)) as int) as year,
        cast(date_part('month',cast(datadate as date)) as int) as month
    from comp.funda
    where indfmt='INDL' and consol='C' and popsrc='D'
        and datafmt='STD' and curcd='USD' and seq>0
        and datadate>='01/01/1975' and datadate<='12/31/2017') t4
    on (t3.gvkey=t4.gvkey and t3.year=t4.year and t3.month=t4.month)
    where t3.me>0
    """)

merged1['yyyymm'] = merged1['year']*100 + merged1['month']
merged1 = merged1.sort_values(['permno','yyyymm','me','seq'])
merged1 = merged1.drop_duplicates(['permno','yyyymm'])
merged1 = merged1.sort_values(['permno','yyyymm']).reset_index(drop=True)



# ------------------------------------------------------
#   Comparison: generate PERMNO-GVKEY link by CUSIP
# -------------------------------------------------------
permno_gvkey_by_cusip = msenames.merge(security,
    how='inner',left_on='cusip',right_on='cusip_comp')
permno_gvkey_by_cusip = permno_gvkey_by_cusip \
    .drop_duplicates(['permno','gvkey'])
permno_gvkey_by_cusip = permno_gvkey_by_cusip \
    [['permno','gvkey','cusip']]
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

