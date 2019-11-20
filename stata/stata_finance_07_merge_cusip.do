clear
set more off

cd ""

/*Compustat data*/
import delimited merge_compustat.txt, clear
sort gvkey fyear datadate
by gvkey fyear: gen n = _n
by gvkey fyear: gen n_max = _N
keep if n==n_max
gen yyyymm = int(datadate/100)
gen cusip8 = substr(cusip,1,8)
duplicates drop cusip8 fyear, force
keep gvkey cusip8 datadate yyyymm seq ib at
save _compustat, replace

/*CRSP data*/
import delimited merge_crsp.txt, clear
keep if inlist(shrcd,10,11) & inlist(exchcd,1,2,3)
duplicates drop permno date, force
gen yyyymm = int(date/100)
gen cusip8 = cusip
gen me = abs(prc) * shrout / 1000
keep if me>0 & me!=.
keep cusip8 yyyymm ret me

/*Merge Compustat*/
merge 1:1 cusip8 yyyymm using _compustat
keep if _merge==3
drop _merge
duplicates drop gvkey yyyymm, force
sort gvkey datadate
rename cusip8 cusip
order cusip gvkey datadate yyyymm ret me
save merged, replace

/*Clean temporary files*/
rm _compustat.dta

