clear
set more off

cd "/users/ml/dropbox/teaching/data/"

/*Generate PERMNO-GVKEY link table*/
import delimited security.txt, clear
gen _mergecusip = substr(cusip,1,8)
rename cusip cusip_comp
keep gvkey _mergecusip
save _security, replace

import delimited msenames.txt, clear
keep if ncusip != ""
gen _mergecusip = ncusip
gen newnamedt = date(namedt,"YMD")
gen newnameendt = date(nameendt,"YMD")
gen long _namedt = year(newnamedt)*10000 + month(newnamedt)*100 + day(newnamedt)
gen long _nameendt = year(newnameendt)*10000 + month(newnameendt)*100 + day(newnameendt)
keep permno _mergecusip _namedt _nameendt

merge m:m _mergecusip using _security
keep if _merge==1 | _merge==3
drop _merge

gsort permno -_namedt
by permno: replace gvkey=gvkey[_n-1] if gvkey==.
bysort permno gvkey: egen long begdate=min(_namedt)
bysort permno gvkey: egen long enddate=max(_nameendt)
drop _*

duplicates drop permno gvkey, force
save _permno_gvkey_link, replace

/*Compustat data*/
import delimited merge_compustat.txt, clear
keep if indfmt=="INDL" & consol=="C" & popsrc=="D" & datafmt=="STD" & seq>0 & seq!=.
sort gvkey fyear datadate
by gvkey fyear: gen n = _n
by gvkey fyear: gen n_max = _N
keep if n==n_max
gen yyyymm = int(datadate/100)
keep gvkey datadate fyear yyyymm seq
save _compustat, replace

/*CRSP data*/
import delimited merge_crsp.txt, clear
keep if inlist(shrcd,10,11) & inlist(exchcd,1,2,3)
duplicates drop permno date, force
gen yyyymm = int(date/100)
gen me = abs(prc) * shrout / 1000
keep if me>0 & me!=.
keep permno date yyyymm me

/*Merge CRSP and link table*/
joinby permno using _permno_gvkey_link
keep if date>=begdate & date<=enddate
duplicates drop permno yyyymm, force

/*Merge Compustat*/
merge m:1 gvkey yyyymm using _compustat
keep if _merge==3
drop _merge
duplicates drop permno yyyymm, force
sort permno yyyymm
save _merged, replace

/*Clean temporary files*/
rm _security.dta
rm _permno_gvkey_link.dta
rm _compustat.dta
rm _merged.dta

