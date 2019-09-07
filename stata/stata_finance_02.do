clear

/* Set up working directory */
cd "~/Dropbox/teaching/data/"

/* ----------------------- CRSP -----------------------*/
/* Import CRSP data */
import delimited crsp_raw_data.txt, clear

/* Check data type */
describe

/* Convert data type */
gen r = real(ret)

/* Check duplicates */
duplicates drop permno date, force

/* US domestic common shares */
tabulate shrcd

keep if inlist(shrcd,10,11)

/* Stock exchanges */
tab exchcd

keep if inlist(exchcd,1,2,3)

/* Count number of obsrvations */
count

/* Negative stock prices */
count if prc<=0

gen price = abs(prc)
replace price = . if price==0

/* Non-positive market value */
gen me = (price*shrout) / 1000
replace me = . if me<=0

/* Adjusted price and shares */
gen price_adj = price / cfacpr
gen shrout_adj = shrout * cfacshr

/* Save data in Stata format */
save crsp_monthly, replace

/*-------------------------- Compustat ---------------------------*/
/* Import Compustat */
import delimited funda_raw_data.txt, clear

/* Keep unique GVKEY-DATADATE observations */
keep if consol=="C" & indfmt=="INDL" & datafmt=="STD" & popsrc=="D" & curcd=="USD"

/* Multiple fiscal year ends in same calendar year */
bysort gvkey fyear: egen n = count(fyear)
keep if n==1

/* Save Compustat in Stata format */
save funda, replace

/*------------------------- Know your data -----------------------*/
use crsp_monthly, clear

/* Holding period returns */
sort permno date
forvalues i = 1/11 {
    by permno: gen lr`i' = r[_n-`i']
}

gen hpr = 1 + r
forvalues i = 1/11 {
    replace hpr = hpr * (1+lr`i')
}
replace hpr = hpr - 1

/* Summarize full sample */
summarize r price

sum r price, d

tabstat r price, s(mean median sd)

/* Summarize by group */
gen year = int(date/10000)

bysort year: sum r price if year>=1991

tabstat r price if year>=1991, s(mean sd) by(year)

tabstat r price, s(mean median sd) by(year) nototal

table year, c(mean r sd r)

