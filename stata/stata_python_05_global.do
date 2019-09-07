clear

cd "your_working_directory"

/* Read data from Compustat Global */
import delimited uk.txt, clear

/* Keep common shares */
keep if tpci == "0"

/* Keep primary issue */
keep if iid == prirow

/* fic = GBR */
keep if fic == "GBR"

/* Check duplicates */
duplicates drop gvkey iid datadate, force

/* Adjusted price */
gen p_adj = prccd / ajexdi * trfd

/* Security level id */
tostring gvkey, gen(gvkey_str)
gen stkcd = gvkey_str + iid
order stkcd datadate


/* Generate date index */
preserve
duplicates drop datadate, force
gen date_idx = _n
keep datadate date_idx
sort datadate
save uk_date_idx, replace
restore

/* Calculate daily return */
merge m:1 datadate using uk_date_idx
sort stkcd datadate
bysort stkcd: gen ldate_idx = date_idx[_n-1]
bysort stkcd: gen lp_adj = p_adj[_n-1]
gen date_diff = date_idx - ldate_idx
gen ret = p_adj / lp_adj - 1
count if ret!=.

tab date_diff

replace ret = . if date_diff!=1
count if ret!=.

/* Calculate monthly return */
keep if monthend==1
keep stkcd datadate p_adj

gen yyyymm = int(datadate/100)
gen year = int(yyyymm/100)
gen month = mod(yyyymm,100)

tabstat yyyymm, s(min)

/* Generate month index */
gen month_idx = (year-2017)*12 + month - 9

sort stkcd yyyymm
bysort stkcd: gen lmonth_idx = month_idx[_n-1]
bysort stkcd: gen lp_adj = p_adj[_n-1]
gen month_diff = month_idx - lmonth_idx
gen ret = p_adj / lp_adj - 1
count if ret!=.

tab month_diff

replace ret = . if month_diff!=1
count if ret!=.

